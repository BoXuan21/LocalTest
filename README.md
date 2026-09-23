# k3d Metrics Stack

A local Kubernetes demo environment (via [k3d](https://k3d.io/)) for exercising a metrics/logging/alerting pipeline: Loki + Alloy, kube-prometheus-stack (Prometheus + Grafana + Alertmanager), Istio, Kyverno, and a small self-healing Kubernetes operator — all wired up with dashboards and alert rules out of the box.

## Architecture

| Component | Namespace | Purpose |
|---|---|---|
| **Loki + Alloy** | `loki` | Log aggregation (Loki) and log collection/shipping (Alloy), queried via `localhost:3100`. |
| **kube-prometheus-stack** | `monitoring` | Prometheus, Grafana (`admin`/`admin`), and Alertmanager, installed via Helm with custom values in [`helm/kube-prometheus-values.yaml`](helm/kube-prometheus-values.yaml). Alertmanager routing (e.g. ServiceNow) is optionally layered in via a gitignored `helm/kube-prometheus-values.secrets.yaml` — see [`helm/kube-prometheus-values.secrets.yaml.example`](helm/kube-prometheus-values.secrets.yaml.example). |
| **Istio** | `istio-system` | Service mesh (`istio-base` + `istiod`), installed via Helm with custom values in [`helm/istiod-values.yaml`](helm/istiod-values.yaml). Sidecar injection is enabled on `demo-stack`, so mesh telemetry (`istio_request_duration_milliseconds`, `istio_requests_total`, etc.) is scraped via the PodMonitor/ServiceMonitor in [`manifests/istio/servicemonitors.yaml`](manifests/istio/servicemonitors.yaml). |
| **Kyverno** | `kyverno` | Policy engine enforcing cluster policies defined in [`manifests/kyverno/kyverno-policy.yaml`](manifests/kyverno/kyverno-policy.yaml). |
| **Self-healing operator** | `demo-stack` | A [kopf](https://kopf.readthedocs.io/)-based Python operator that reconciles a custom `AppConfig` CRD and deliberately exits every `SELF_DESTRUCT_SECONDS` (default 180s) to demonstrate self-healing/restart behavior. Exposes Prometheus metrics on `:8080` and a `/healthz` probe on `:8081`. |
| **Log generator / Log sender** | `demo-stack` | `log-generator` continuously emits sample logs; `log-sender` is a tiny web UI (`localhost:5005`) for manually sending log lines that show up in Loki/Grafana. |
| **sample-web / metrics-storage** | `demo-stack` | Sample Deployment and StatefulSet used as generic workloads for dashboards and Kyverno policy demos. |
| **traffic-generator** | `demo-stack` | Continuously curls `sample-web` through the mesh so Istio sidecars have real request traffic to report latency/error metrics for. |

Metrics and logs flow into Prometheus/Loki, are visualized via preloaded Grafana dashboards ("Self-Healing Operator", "Log Generator - Logs"), and are backed by recording rules and alert rules for operator health, Istio request latency, and anomaly detection.

## Prerequisites

`scripts/setup.sh` will auto-install missing dependencies where possible, except Docker:

- [Docker](https://docs.docker.com/get-docker/) (required, must be installed manually)
- [k3d](https://k3d.io/) (auto-installed if missing)
- [kubectl](https://kubernetes.io/docs/tasks/tools/) (auto-installed if missing)
- [Helm](https://helm.sh/) (auto-installed if missing)

## Getting started

```bash
cd scripts
./setup.sh
```

This will:
1. Check/install dependencies.
2. Build the operator Docker image (`operator-demo:latest`) in the background.
3. Create (or reuse) a k3d cluster named `metrics-stack` (1 server, 2 agents) per [`scripts/k3d-config.yaml`](scripts/k3d-config.yaml).
4. Install Loki, Alloy, Kyverno, Istio, and kube-prometheus-stack in parallel.
5. Import the operator image into the cluster and apply all remaining manifests (CRD, deployments, dashboards, alert/recording rules, log generator/sender, Istio metrics scraping + traffic generator).
6. Wait for all workloads to become ready.

### Exposed ports

| Service | URL |
|---|---|
| Grafana (`admin`/`admin`) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Loki API | http://localhost:3100 |
| Log Sender UI | http://localhost:5005 |

## Usage

**Watch the operator self-restart** (every `SELF_DESTRUCT_SECONDS`, default 180s):

```bash
kubectl -n demo-stack get pods -l app=self-healing-operator -w
```

**Send a log line manually** via the Log Sender UI at http://localhost:5005, or watch `log-generator` produce logs continuously — both are visible in Grafana's "Log Generator - Logs" dashboard.

**Check Istio sidecar injection and mesh traffic:**

```bash
kubectl -n demo-stack get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[*].name}{"\n"}{end}'
# each pod (sample-web, traffic-generator, ...) should list an "istio-proxy" container alongside its app container

# query in Prometheus/Grafana:
# istio_requests_total
# histogram_quantile(0.99, sum(rate(istio_request_duration_milliseconds_bucket[5m])) by (le))
```

`traffic-generator` continuously curls `sample-web` so the mesh has real requests to measure; `IstioHighRequestLatencyP99`, `IstioHighRequestLatencyP99Critical`, and `IstioNoRequestsObserved` alert rules watch that traffic (see [`manifests/monitoring/istio-alerts.yaml`](manifests/monitoring/istio-alerts.yaml)).

**Apply an `AppConfig` custom resource** to trigger the operator's reconcile loop:

```bash
kubectl apply -f - <<'EOF'
apiVersion: demo.example.com/v1
kind: AppConfig
metadata:
  name: example
  namespace: demo-stack
spec:
  message: hello from the operator
EOF
kubectl -n demo-stack get appconfig example -o yaml
```

## Tearing down

```bash
scripts/teardown.sh
```

Deletes the `metrics-stack` k3d cluster.

## Repo layout

```
helm/                       Helm values for kube-prometheus-stack and Istio (istiod)
manifests/
  loki/                     Loki + Alloy
  istio/                    Istio namespace, envoy/istiod ServiceMonitors, traffic-generator
  kyverno/                  Kyverno ClusterPolicy
  operator/                 Operator CRD, RBAC, Deployment
  monitoring/               ServiceMonitors, Grafana dashboards, recording/alert rules (incl. Istio)
  log-generator/            log-generator app + log-sender web UI
  namespace.yaml            demo-stack namespace (Istio sidecar injection enabled)
  deployment.yaml           sample-web workload
  statefulset-storage.yaml  metrics-storage workload
operator/                   Self-healing operator source (Python, kopf) + Dockerfile
scripts/
  setup.sh                  Bring up the whole stack
  teardown.sh               Delete the k3d cluster
  k3d-config.yaml           k3d cluster/port config
```
