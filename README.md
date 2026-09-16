# k3d Metrics Stack

A local Kubernetes demo environment (via [k3d](https://k3d.io/)) for exercising a metrics/logging/alerting pipeline: Kafka, Loki + Alloy, kube-prometheus-stack (Prometheus + Grafana + Alertmanager), Kyverno, and a small self-healing Kubernetes operator — all wired up with dashboards and alert rules out of the box.

## Architecture

| Component | Namespace | Purpose |
|---|---|---|
| **Kafka** | `kafka` | Single-broker StatefulSet (`kafka-0`) exposed on `localhost:9092` for producing/consuming messages. |
| **Loki + Alloy** | `loki` | Log aggregation (Loki) and log collection/shipping (Alloy), queried via `localhost:3100`. |
| **kube-prometheus-stack** | `monitoring` | Prometheus, Grafana (`admin`/`admin`), and Alertmanager, installed via Helm with custom values in [`helm/kube-prometheus-values.yaml`](helm/kube-prometheus-values.yaml). |
| **Kyverno** | `kyverno` | Policy engine enforcing cluster policies defined in [`manifests/kyverno/kyverno-policy.yaml`](manifests/kyverno/kyverno-policy.yaml). |
| **Self-healing operator** | `demo-stack` | A [kopf](https://kopf.readthedocs.io/)-based Python operator that reconciles a custom `AppConfig` CRD and deliberately exits every `SELF_DESTRUCT_SECONDS` (default 180s) to demonstrate self-healing/restart behavior. Exposes Prometheus metrics on `:8080` and a `/healthz` probe on `:8081`. |
| **Log generator / Log sender** | `demo-stack` | `log-generator` continuously emits sample logs; `log-sender` is a tiny web UI (`localhost:5005`) for manually sending log lines that show up in Loki/Grafana. |
| **sample-web / metrics-storage** | `demo-stack` | Sample Deployment and StatefulSet used as generic workloads for dashboards and Kyverno policy demos. |

Metrics and logs flow into Prometheus/Loki, are visualized via preloaded Grafana dashboards ("Self-Healing Operator", "Kafka", "Log Generator - Logs"), and are backed by recording rules and alert rules for Kafka throughput, operator health, and anomaly detection.

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
4. Install Kafka, Loki, Alloy, Kyverno, and kube-prometheus-stack in parallel.
5. Import the operator image into the cluster and apply all remaining manifests (CRD, deployments, dashboards, alert/recording rules, log generator/sender).
6. Wait for all workloads to become ready.

### Exposed ports

| Service | URL |
|---|---|
| Grafana (`admin`/`admin`) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Loki API | http://localhost:3100 |
| Kafka bootstrap | http://localhost:9092 |
| Log Sender UI | http://localhost:5005 |

## Usage

**Watch the operator self-restart** (every `SELF_DESTRUCT_SECONDS`, default 180s):

```bash
kubectl -n demo-stack get pods -l app=self-healing-operator -w
```

**Send a one-off Kafka message** and verify it in Prometheus/Grafana:

```bash
scripts/send-kafka-message.sh test-topic
# query in Prometheus/Grafana: kafka_server_messagesinpersec_count
```

> Note: `send-kafka-message.sh` is referenced by `setup.sh` (which `chmod +x`'s it) but is not currently present in `scripts/`. Use `kafka-message-loop.sh` below in the meantime, or add the script.

**Send Kafka messages on a loop:**

```bash
scripts/kafka-message-loop.sh test-topic 30   # one message every 30s (default)
# Ctrl+C to stop, or background it with `&` and `kill $!`
```

**Send a log line manually** via the Log Sender UI at http://localhost:5005, or watch `log-generator` produce logs continuously — both are visible in Grafana's "Log Generator - Logs" dashboard.

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
helm/                       Helm values for kube-prometheus-stack
manifests/
  kafka/                    Kafka StatefulSet
  loki/                     Loki + Alloy
  kyverno/                  Kyverno ClusterPolicy
  operator/                 Operator CRD, RBAC, Deployment
  monitoring/               ServiceMonitors, Grafana dashboards, recording/alert rules
  log-generator/            log-generator app + log-sender web UI
  namespace.yaml            demo-stack namespace
  deployment.yaml           sample-web workload
  statefulset-storage.yaml  metrics-storage workload
operator/                   Self-healing operator source (Python, kopf) + Dockerfile
scripts/
  setup.sh                  Bring up the whole stack
  teardown.sh               Delete the k3d cluster
  k3d-config.yaml           k3d cluster/port config
  kafka-message-loop.sh     Send Kafka messages on an interval
```
