#!/usr/bin/env bash
# Bring up the whole k3d metrics stack. Safe to re-run at any point: reuses a
# healthy cluster, repairs stuck Helm releases, and re-applies all manifests.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER=metrics-stack
K3D_CONFIG="$ROOT/scripts/k3d-config.yaml"
TIMEOUT=10m   # generous: first-time image pulls on a slow network

# Pinned so an upstream chart release can't break setup.
KPS_VERSION=91.9.0       # kube-prometheus-stack
KYVERNO_VERSION=3.9.1
ISTIO_VERSION=1.30.5

LOG_DIR="$(mktemp -d)"
START=$SECONDS
cd "$ROOT"

log()  { printf '\n\033[1;34m==>\033[0m %s \033[2m(%ss)\033[0m\n' "$1" "$((SECONDS - START))"; }
ok()   { printf '  \033[32m✓\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*"; }
fail() { printf '\n\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# On any exit, stop our background jobs (so an interrupted run doesn't leave a
# helm install running unattended) and clean up logs on success.
cleanup() {
  local code=$?
  for pid in $(jobs -p); do kill "$pid" 2>/dev/null || true; done
  if [ "$code" -eq 0 ]; then rm -rf "$LOG_DIR"; else echo "Logs kept in $LOG_DIR" >&2; fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# Run a command in the background, logging to $LOG_DIR/<name>.log.
# Usage: spawn <name> <command...> (pid is kept in $LOG_DIR/<name>.pid)
spawn() {
  local name=$1; shift
  ( "$@" ) >"$LOG_DIR/$name.log" 2>&1 &
  echo $! >"$LOG_DIR/$name.pid"
}

# Wait for a background job; on failure show its log and abort.
await() {
  local name=$1
  if ! wait "$(cat "$LOG_DIR/$name.pid")"; then
    echo "---- $name (last 40 lines) ----" >&2
    tail -40 "$LOG_DIR/$name.log" >&2
    fail "$name failed (full log: $LOG_DIR/$name.log)"
  fi
  ok "$name"
}

# Retry flaky network operations (chart/image downloads).
retry() {
  local n
  for n in 1 2 3; do
    "$@" && return 0
    [ "$n" -lt 3 ] && { echo "attempt $n failed, retrying in $((n * 5))s: $*" >&2; sleep $((n * 5)); }
  done
  return 1
}

# A release left in pending-* (e.g. by an interrupted run) blocks every later
# `helm upgrade`, and a failed first install can't be upgraded. Clear both.
helm_unstick() {
  local rel=$1 ns=$2 status
  status=$(helm status "$rel" -n "$ns" 2>/dev/null | awk '/^STATUS:/ {print $2}') || true
  case "$status" in
    pending-install)
      echo "release $rel stuck in $status - uninstalling"
      helm uninstall "$rel" -n "$ns" --no-hooks --wait ;;
    pending-upgrade|pending-rollback)
      echo "release $rel stuck in $status - rolling back"
      helm rollback "$rel" -n "$ns" --wait ;;
    failed)
      if ! helm history "$rel" -n "$ns" 2>/dev/null | grep -qE 'deployed|superseded'; then
        echo "release $rel never deployed successfully - uninstalling"
        helm uninstall "$rel" -n "$ns" --no-hooks --wait
      fi ;;
  esac
}

# Usage: helm_install <release> <namespace> <chart> <repo-url> <version> [extra helm args...]
helm_install() {
  local rel=$1 ns=$2 chart=$3 repo=$4 version=$5; shift 5
  helm_unstick "$rel" "$ns"
  retry helm upgrade --install "$rel" "$chart" --repo "$repo" --version "$version" \
    -n "$ns" --create-namespace --timeout "$TIMEOUT" "$@"
}

# Wait for a workload to exist (some are created asynchronously by operators)
# and then to finish rolling out. Prints diagnostics on failure.
wait_ready() {
  local ns=$1 res=$2 deadline=$((SECONDS + 120))
  until kubectl -n "$ns" get "$res" >/dev/null 2>&1; do
    [ "$SECONDS" -gt "$deadline" ] && { echo "$ns/$res was never created"; return 1; }
    sleep 2
  done
  if ! kubectl -n "$ns" rollout status "$res" --timeout="$TIMEOUT"; then
    kubectl -n "$ns" get pods -o wide
    kubectl -n "$ns" get events --sort-by=.lastTimestamp | tail -15
    return 1
  fi
}

image_id() { docker image inspect -f '{{.Id}}' "$1" 2>/dev/null || true; }

# --- 1. dependencies ----------------------------------------------------------
log "Checking dependencies"

if ! command -v docker >/dev/null; then
  fail "Docker is required: https://docs.docker.com/get-docker/"
fi
if ! docker info >/dev/null 2>&1; then
  if [ "$(uname)" = Darwin ] && open -a Docker 2>/dev/null; then
    echo "  starting Docker Desktop..."
    for _ in $(seq 1 90); do docker info >/dev/null 2>&1 && break; sleep 2; done
  fi
  docker info >/dev/null 2>&1 || fail "Docker is installed but not running - start it and re-run"
fi

install_dep() {
  local name=$1 arch
  command -v "$name" >/dev/null && return
  echo "  installing $name"
  if command -v brew >/dev/null; then
    brew install "$name"
  else
    case $name in
      k3d) curl -fsSL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash ;;
      helm) curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash ;;
      kubectl)
        arch=$(uname -m); [ "$arch" = x86_64 ] && arch=amd64; [ "$arch" = aarch64 ] && arch=arm64
        curl -fsSLo /tmp/kubectl "https://dl.k8s.io/release/$(curl -fsSL https://dl.k8s.io/release/stable.txt)/bin/linux/$arch/kubectl"
        sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl ;;
    esac
  fi
  command -v "$name" >/dev/null || fail "could not install $name - install it manually and re-run"
}
for dep in k3d kubectl helm; do install_dep "$dep"; done
ok "docker, k3d, kubectl, helm"

# --- 2. images (built in the background while the cluster comes up) ----------
log "Building images in the background"
OLD_OPERATOR_ID=$(image_id operator-demo:latest)
OLD_SIMULATOR_ID=$(image_id simulator:latest)
# --load: with a docker-container buildx builder as default, a plain build
# would only land in the build cache and k3d would import a stale image.
spawn build-operator retry docker buildx build --load -t operator-demo:latest operator
spawn build-simulator retry docker buildx build --load -t simulator:latest simulator

# --- 3. cluster ---------------------------------------------------------------
HOST_PORTS=$(grep -oE 'port: *[0-9]+:' "$K3D_CONFIG" | grep -oE '[0-9]+')

cluster_ports_ok() {
  local mapped port
  mapped=$(docker port "k3d-$CLUSTER-serverlb" 2>/dev/null) || return 1
  for port in $HOST_PORTS; do
    grep -q "0.0.0.0:$port\$" <<<"$mapped" || { echo "  existing cluster is missing port $port"; return 1; }
  done
}

check_ports_free() {
  local port busy=""
  command -v lsof >/dev/null || return 0
  for port in $HOST_PORTS; do
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 && busy="$busy $port"
  done
  [ -z "$busy" ] && return 0
  for port in $busy; do lsof -nP -iTCP:"$port" -sTCP:LISTEN | tail -n +2 | awk -v p="$port" '{print "  port " p " is used by " $1 " (pid " $2 ")"}'; done
  fail "free the port(s) above and re-run"
}

if k3d cluster get "$CLUSTER" >/dev/null 2>&1 && ! cluster_ports_ok; then
  log "Recreating cluster '$CLUSTER' (its port mappings don't match $K3D_CONFIG)"
  k3d cluster delete "$CLUSTER"
fi

if k3d cluster get "$CLUSTER" >/dev/null 2>&1; then
  log "Reusing existing k3d cluster '$CLUSTER'"
  REUSED=true
  k3d cluster start "$CLUSTER" >/dev/null 2>&1 || true
else
  log "Creating k3d cluster '$CLUSTER'"
  REUSED=false
  check_ports_free
  create_cluster() {
    # clear any half-created leftovers from a previous failed attempt
    k3d cluster delete "$CLUSTER" >/dev/null 2>&1 || true
    k3d cluster create --config "$K3D_CONFIG" --kubeconfig-update-default=false --wait
  }
  retry create_cluster >"$LOG_DIR/k3d-create.log" 2>&1 ||
    { cat "$LOG_DIR/k3d-create.log" >&2; fail "cluster creation failed"; }
fi
# always (re)write the kubeconfig entry: it goes stale if the cluster was
# recreated elsewhere or the user's kubeconfig was reset
k3d kubeconfig merge "$CLUSTER" --kubeconfig-merge-default --kubeconfig-switch-context >/dev/null
kubectl wait --for=condition=Ready nodes --all --timeout=120s >/dev/null
ok "cluster ready ($(kubectl get nodes --no-headers | wc -l | tr -d ' ') nodes)"

# --- 4. platform components in parallel --------------------------------------
log "Installing platform components"
# Create every namespace first: manifest directories are applied in filename
# order, so e.g. loki/alloy.yaml would otherwise run before loki/loki.yaml
# creates the namespace (fails on a fresh cluster).
kubectl apply -f manifests/namespace.yaml -f manifests/istio/namespace.yaml >/dev/null
for ns in loki kafka; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

# Only the CRDs are needed before applying our manifests (Helm installs them
# synchronously), so kube-prometheus-stack doesn't --wait: its pods come up in
# parallel with everything else and are waited for at the end.
spawn kube-prometheus-stack helm_install kube-prometheus-stack monitoring kube-prometheus-stack \
  https://prometheus-community.github.io/helm-charts "$KPS_VERSION" -f helm/kube-prometheus-values.yaml

# Kyverno must be serving before demo-stack pods are created so the policy is enforced.
spawn kyverno helm_install kyverno kyverno kyverno https://kyverno.github.io/kyverno/ "$KYVERNO_VERSION" --wait

# istiod's injection webhook must be up before demo-stack pods are created, or they get no sidecar.
ISTIO_REPO=https://istio-release.storage.googleapis.com/charts
install_istio() {
  helm_install istio-base istio-system base "$ISTIO_REPO" "$ISTIO_VERSION" --set defaultRevision=default &&
  helm_install istiod istio-system istiod "$ISTIO_REPO" "$ISTIO_VERSION" -f helm/istiod-values.yaml --wait
}
spawn istio install_istio

spawn loki kubectl apply -f manifests/loki/
spawn kafka kubectl apply -f manifests/kafka/kafka.yaml

await loki
await kafka
await kube-prometheus-stack

# --- 5. images into the cluster -----------------------------------------------
await build-operator
await build-simulator
IMAGES_CHANGED=false
if [ "$(image_id operator-demo:latest)" != "$OLD_OPERATOR_ID" ] || [ "$(image_id simulator:latest)" != "$OLD_SIMULATOR_ID" ]; then
  IMAGES_CHANGED=true
fi
if ! $REUSED || $IMAGES_CHANGED; then
  retry k3d image import operator-demo:latest simulator:latest -c "$CLUSTER" >"$LOG_DIR/import.log" 2>&1 ||
    { cat "$LOG_DIR/import.log" >&2; fail "image import failed"; }
  ok "images imported"
else
  ok "images unchanged, import skipped"
fi

# --- 6. workloads, rules and dashboards ---------------------------------------
log "Applying manifests"
# monitoring rules/dashboards don't depend on Istio or Kyverno
kubectl apply -f manifests/monitoring/ -f manifests/istio/servicemonitors.yaml >/dev/null
kubectl apply -f manifests/operator/operator-crd.yaml >/dev/null
kubectl wait --for condition=established --timeout=60s crd/appconfigs.demo.example.com >/dev/null

await kyverno
await istio
kubectl apply -f manifests/kyverno/kyverno-policy.yaml 2>&1 | grep -v -i deprecated || true

# demo-stack pods are created only now, so they get sidecars and Kyverno checks
kubectl apply \
  -f manifests/operator/ \
  -f manifests/deployment.yaml -f manifests/statefulset-storage.yaml \
  -f manifests/log-generator/ \
  -f manifests/istio/traffic-generator.yaml \
  -f manifests/simulator/ >/dev/null
ok "manifests applied"

if $REUSED && $IMAGES_CHANGED; then
  # same :latest tag, so restart to pick up the freshly imported images
  kubectl -n demo-stack rollout restart deployment/self-healing-operator deployment/simulator >/dev/null
  ok "restarted operator + simulator for new images"
fi

# --- 7. wait for everything in parallel ---------------------------------------
log "Waiting for workloads to become ready"
WORKLOADS="
demo-stack deployment/sample-web
demo-stack deployment/self-healing-operator
demo-stack deployment/log-generator
demo-stack deployment/log-sender
demo-stack deployment/traffic-generator
demo-stack deployment/simulator
demo-stack statefulset/metrics-storage
kafka statefulset/kafka
loki deployment/loki
loki deployment/alloy
monitoring deployment/kube-prometheus-stack-operator
monitoring deployment/kube-prometheus-stack-grafana
monitoring deployment/kube-prometheus-stack-kube-state-metrics
monitoring statefulset/prometheus-kube-prometheus-stack-prometheus
"
while read -r ns res; do
  [ -n "$ns" ] && spawn "$ns-${res##*/}" wait_ready "$ns" "$res"
done <<<"$WORKLOADS"
while read -r ns res; do
  [ -n "$ns" ] && await "$ns-${res##*/}"
done <<<"$WORKLOADS"

log "Done in $((SECONDS - START))s"
cat <<'EOF'
  Grafana (admin/admin)  http://localhost:3000
  Prometheus             http://localhost:9090
  Loki API               http://localhost:3100
  Log Sender UI          http://localhost:5005
  Test Simulator         http://localhost:8088
  Kafka bootstrap        localhost:9092
EOF
