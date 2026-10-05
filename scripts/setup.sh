#!/usr/bin/env bash
# Bring up the whole k3d metrics stack. Safe to re-run: reuses an existing
# cluster, upgrades Helm releases in place and re-applies all manifests.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER=metrics-stack
LOG_DIR="$(mktemp -d)"
cd "$ROOT"

log() { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# Wait for a background job and dump its log if it failed.
wait_job() {
  local pid=$1 name=$2
  if ! wait "$pid"; then
    echo "---- $name log ----" >&2
    cat "$LOG_DIR/$name.log" >&2
    fail "$name failed"
  fi
  echo "  ✓ $name"
}

# --- 1. dependencies ----------------------------------------------------------
log "Checking dependencies"
command -v docker >/dev/null || fail "Docker is required: https://docs.docker.com/get-docker/"
docker info >/dev/null 2>&1 || fail "Docker is installed but not running"

install_dep() {
  local name=$1
  command -v "$name" >/dev/null && return
  echo "  installing $name"
  if command -v brew >/dev/null; then
    brew install "$name"
  else
    case $name in
      k3d) curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash ;;
      helm) curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash ;;
      kubectl)
        local arch; arch=$(uname -m); [ "$arch" = x86_64 ] && arch=amd64; [ "$arch" = aarch64 ] && arch=arm64
        curl -fsSLo /tmp/kubectl "https://dl.k8s.io/release/$(curl -fsSL https://dl.k8s.io/release/stable.txt)/bin/linux/$arch/kubectl"
        sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl ;;
    esac
  fi
}
for dep in k3d kubectl helm; do install_dep "$dep"; done

# --- 2. images (built in the background while the cluster comes up) ----------
log "Building images (operator-demo, simulator) in the background"
docker build -t operator-demo:latest operator >"$LOG_DIR/build-operator.log" 2>&1 &
BUILD_OPERATOR=$!
docker build -t simulator:latest simulator >"$LOG_DIR/build-simulator.log" 2>&1 &
BUILD_SIMULATOR=$!

# --- 3. cluster ---------------------------------------------------------------
REUSED=false
if k3d cluster get "$CLUSTER" >/dev/null 2>&1; then
  log "Reusing existing k3d cluster '$CLUSTER'"
  REUSED=true
  k3d cluster start "$CLUSTER" >/dev/null 2>&1 || true
else
  log "Creating k3d cluster '$CLUSTER'"
  k3d cluster create --config scripts/k3d-config.yaml
fi
kubectl config use-context "k3d-$CLUSTER" >/dev/null

# --- 4. platform components in parallel --------------------------------------
log "Installing Loki, Alloy, Kafka, Kyverno, Istio and kube-prometheus-stack"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
helm repo add kyverno https://kyverno.github.io/kyverno/ >/dev/null 2>&1 || true
helm repo add istio https://istio-release.storage.googleapis.com/charts >/dev/null 2>&1 || true
helm repo update >/dev/null

kubectl apply -f manifests/namespace.yaml -f manifests/istio/namespace.yaml >/dev/null

helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace -f helm/kube-prometheus-values.yaml --wait --timeout 10m \
  >"$LOG_DIR/kube-prometheus-stack.log" 2>&1 &
PID_KPS=$!

helm upgrade --install kyverno kyverno/kyverno -n kyverno --create-namespace --wait --timeout 10m \
  >"$LOG_DIR/kyverno.log" 2>&1 &
PID_KYVERNO=$!

( helm upgrade --install istio-base istio/base -n istio-system --set defaultRevision=default --wait &&
  helm upgrade --install istiod istio/istiod -n istio-system -f helm/istiod-values.yaml --wait --timeout 10m
) >"$LOG_DIR/istio.log" 2>&1 &
PID_ISTIO=$!

kubectl apply -f manifests/loki/ >"$LOG_DIR/loki.log" 2>&1 &
PID_LOKI=$!

kubectl apply -f manifests/kafka/kafka.yaml >"$LOG_DIR/kafka.log" 2>&1 &
PID_KAFKA=$!

wait_job $PID_LOKI loki
wait_job $PID_KAFKA kafka
wait_job $PID_KYVERNO kyverno
wait_job $PID_ISTIO istio
wait_job $PID_KPS kube-prometheus-stack

# --- 5. images into the cluster -----------------------------------------------
log "Importing images into the cluster"
wait_job $BUILD_OPERATOR build-operator
wait_job $BUILD_SIMULATOR build-simulator
k3d image import operator-demo:latest simulator:latest -c "$CLUSTER"

# --- 6. workloads, rules and dashboards ---------------------------------------
# demo-stack pods are created only now, after istiod is up, so they get sidecars.
log "Applying manifests"
kubectl apply -f manifests/kyverno/kyverno-policy.yaml
kubectl apply -f manifests/operator/operator-crd.yaml
kubectl wait --for condition=established --timeout=60s crd/appconfigs.demo.example.com
kubectl apply -f manifests/operator/
kubectl apply -f manifests/deployment.yaml -f manifests/statefulset-storage.yaml
kubectl apply -f manifests/log-generator/
kubectl apply -f manifests/istio/servicemonitors.yaml -f manifests/istio/traffic-generator.yaml
kubectl apply -f manifests/monitoring/
kubectl apply -f manifests/simulator/

if $REUSED; then
  # same :latest tag, so restart to pick up the freshly imported images
  kubectl -n demo-stack rollout restart deployment/self-healing-operator deployment/simulator
fi

# --- 7. wait for readiness ------------------------------------------------------
log "Waiting for workloads to become ready"
for d in sample-web self-healing-operator log-generator log-sender traffic-generator simulator; do
  kubectl -n demo-stack rollout status "deployment/$d" --timeout=5m
done
kubectl -n demo-stack rollout status statefulset/metrics-storage --timeout=5m
kubectl -n kafka rollout status statefulset/kafka --timeout=5m
kubectl -n loki rollout status deployment/loki --timeout=5m

rm -rf "$LOG_DIR"
cat <<'EOF'

Stack is up:
  Grafana (admin/admin)  http://localhost:3000
  Prometheus             http://localhost:9090
  Loki API               http://localhost:3100
  Log Sender UI          http://localhost:5005
  Test Simulator         http://localhost:8088
  Kafka bootstrap        localhost:9092
EOF
