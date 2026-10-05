# k3d Metrics Stack

A local Kubernetes demo environment (via [k3d](https://k3d.io/)) for exercising a metrics/logging/alerting pipeline: Kafka, Loki + Alloy, kube-prometheus-stack (Prometheus + Grafana + Alertmanager), Istio, Kyverno, and a small self-healing Kubernetes operator — all wired up with dashboards and alert rules out of the box.

## Architecture

| Component | Namespace | Purpose |
|---|---|---|
| **Kafka** | `kafka` | Single-broker StatefulSet (`kafka-0`) exposed on `localhost:9092` for producing/consuming messages. |
| **Loki + Alloy** | `loki` | Log aggregation (Loki) and log collection/shipping (Alloy), queried via `localhost:3100`. |
| **kube-prometheus-stack** | `monitoring` | Prometheus, Grafana (`admin`/`admin`), and Alertmanager, installed via Helm with custom values in [`helm/kube-prometheus-values.yaml`](helm/kube-prometheus-values.yaml). |
| **Istio** | `istio-system` | Service mesh (`istio-base` + `istiod`), installed via Helm with custom values in [`helm/istiod-values.yaml`](helm/istiod-values.yaml). Sidecar injection is enabled on `demo-stack`, so mesh telemetry (`istio_request_duration_milliseconds`, `istio_requests_total`, etc.) is scraped via the PodMonitor/ServiceMonitor in [`manifests/istio/servicemonitors.yaml`](manifests/istio/servicemonitors.yaml). |
| **Kyverno** | `kyverno` | Policy engine enforcing cluster policies defined in [`manifests/kyverno/kyverno-policy.yaml`](manifests/kyverno/kyverno-policy.yaml). |
| **Self-healing operator** | `demo-stack` | A [kopf](https://kopf.readthedocs.io/)-based Python operator that reconciles a custom `AppConfig` CRD and deliberately exits every `SELF_DESTRUCT_SECONDS` (default 180s) to demonstrate self-healing/restart behavior. Exposes Prometheus metrics on `:8080`, plus a `/healthz` probe and a `/crash` test hook (used by the simulator) on `:8081`. |
| **Log generator / Log sender** | `demo-stack` | `log-generator` continuously emits sample logs; `log-sender` is a tiny web UI (`localhost:5005`) for manually sending log lines that show up in Loki/Grafana. |
| **sample-web / metrics-storage** | `demo-stack` | Sample Deployment and StatefulSet used as generic workloads for dashboards and Kyverno policy demos. |
| **traffic-generator** | `demo-stack` | Continuously curls `sample-web` through the mesh so Istio sidecars have real request traffic to report latency/error metrics for. |
| **Test Simulator** | `demo-stack` | Web UI (`localhost:8088`) for triggering failure scenarios on demand (latency, errors, traffic surges, Kafka floods/outages, crashes, OOM kills, Kyverno denials, log storms) while showing the live state of the alerts each scenario should trip. See [Test Simulator](#test-simulator). |

Metrics and logs flow into Prometheus/Loki, are visualized via preloaded Grafana dashboards ("Self-Healing Operator", "Kafka", "Log Generator - Logs", "Test Simulator"), and are backed by recording rules and alert rules for Kafka throughput, operator health, Istio request latency/errors/rate, container resources, and anomaly detection.

## Prerequisites

`scripts/setup.sh` will auto-install missing dependencies where possible, except Docker:

- [Docker](https://docs.docker.com/get-docker/) (required, must be installed manually)
- [k3d](https://k3d.io/) (auto-installed if missing)
- [kubectl](https://kubernetes.io/docs/tasks/tools/) (auto-installed if missing)
- [Helm](https://helm.sh/) (auto-installed if missing)

## Getting started

```bash
scripts/setup.sh   # works from any directory
```

This will:
1. Check/install dependencies (and start Docker Desktop on macOS if it isn't running).
2. Build the operator (`operator-demo:latest`) and simulator (`simulator:latest`) images in the background.
3. Create (or reuse) a k3d cluster named `metrics-stack` (1 server, 2 agents) per [`scripts/k3d-config.yaml`](scripts/k3d-config.yaml).
4. Install Kafka, Loki, Alloy, Kyverno, Istio, and kube-prometheus-stack in parallel (chart versions are pinned at the top of `setup.sh`).
5. Import the images and apply all remaining manifests once Istio and Kyverno are serving, so demo pods get sidecars and policy checks.
6. Wait for every workload in parallel, printing pod status and events for anything that doesn't come up.

The script is safe to re-run at any time; a re-run on a healthy cluster takes about 20 seconds. It:
- skips the image import and restarts unless the operator/simulator code changed;
- repairs Helm releases left stuck by an interrupted run (`pending-install`/`pending-upgrade`) or a failed first install;
- recreates the cluster if its port mappings no longer match `k3d-config.yaml`;
- refuses to create a cluster when a required host port is taken, and names the process using it;
- retries network operations (chart downloads, image builds, cluster creation);
- on failure, keeps per-step logs and prints where they are.

### Exposed ports

| Service | URL |
|---|---|
| Grafana (`admin`/`admin`) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Loki API | http://localhost:3100 |
| Kafka bootstrap | `localhost:9092` |
| Log Sender UI | http://localhost:5005 |
| Test Simulator | http://localhost:8088 |

## Usage

**Watch the operator self-restart** (every `SELF_DESTRUCT_SECONDS`, default 180s):

```bash
kubectl -n demo-stack get pods -l app=self-healing-operator -w
```

**Send a one-off Kafka message** and verify it in Prometheus/Grafana:

```bash
scripts/send-kafka-message.sh test-topic "hello"
# query in Prometheus/Grafana: kafka_server_brokertopicmetrics_count{name="MessagesInPerSec"}
```

For continuous production, rate spikes and broker outages, use the Kafka card in the [Test Simulator](#test-simulator) (it produces 1 msg/s to `sim-events` by default).

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

## Test Simulator

http://localhost:8088 is a control panel for exercising every alert in the stack on purpose. Each card triggers one family of scenarios and lists the alerts it should trip, with their live state from Prometheus (grey = inactive, amber = pending, red = firing). The sidebar shows all pending/firing alerts plus a log of what you triggered. **Reset everything** puts the whole stack back to baseline.

| Card | What it does | Alerts it targets |
|---|---|---|
| **Mesh traffic** | Drives requests through Istio to `sim-target`, a backend whose base latency, tail latency (% of slow requests), and 5xx rate you control. Presets: *p99 > 1s*, *p99 > 3s*, *Error spike* (25%), *Traffic surge* (80 req/s), *Stop all traffic* (also scales `traffic-generator` to 0). | `IstioHighRequestLatencyP99`, `IstioHighRequestLatencyP99Critical`, `IstioHighErrorRate`, `IstioHighRequestRate`, `IstioNoRequestsObserved` |
| **Kafka** | Produces JSON messages at a chosen rate, bursts 5,000 messages, stops producing, or stops/starts the broker (scales `kafka` to 0/1). | `KafkaMessageRateSpike`, `KafkaNoMessagesReceived`, `KafkaMetricsTargetDown` |
| **Self-healing operator** | Crashes the operator container (real container restart), scales it to 0/1, or patches `AppConfig/demo-config` to trigger a reconcile. | `OperatorRestarted`, `OperatorTargetDown`, `OperatorNotReconciling` |
| **Workload faults** | Toggles throwaway `sim-*` Deployments: CrashLoopBackOff, unschedulable pod (requests 64 CPUs), memory held at ~85% of limit, memory leak until OOM kill, CPU burn at the limit. | `ContainerOOMKilled`, `ContainerMemoryNearLimit`, `ContainerCpuNearLimit`, plus the kube-prometheus defaults `KubePodCrashLooping`, `KubePodNotReady`, `KubeDeploymentReplicasMismatch`, `CPUThrottlingHigh` |
| **Kyverno admission** | Submits a pod as a server-side dry run (nothing is created) without resources, without an `app` label, or fully compliant, and shows Kyverno's verdict. | n/a (shows the admission response) |
| **Logs** | Emits logfmt lines at a chosen rate and error share, or bursts 200 errors/warnings, into Loki. | n/a (see "Log Generator - Logs" dashboard) |

Alerts don't fire instantly: each has a rate window plus a `for:` duration. Rough time to *firing*: error rate / request surge / Kafka spike / CPU / memory / OOM ≈ 1–4 min; Istio latency ≈ 6–10 min; target-down ≈ 2 min; the kube-prometheus defaults ≈ 15+ min; "no messages / no requests" ≈ 20–25 min. The chips show each rule's `for:` while inactive and the elapsed time once pending or firing.

The **Test Simulator** Grafana dashboard (http://localhost:3000/d/test-simulator) graphs `sim-target` request rate, p50/p99 latency, error ratio, Kafka throughput, fault-pod CPU/memory against their limits, and container restarts.

How it works: a single pod ([`simulator/`](simulator/), manifests in [`manifests/simulator/`](manifests/simulator/)) serves the UI/API/metrics on `:8080` (excluded from the sidecar so browser polling isn't counted as mesh traffic) and the `sim-target` backend on `:8000` (in the mesh). It talks to the Kubernetes API with a namespaced `Role` that only allows scaling the operator/traffic-generator/Kafka, managing the `sim-*` Deployments, dry-run pod creation, and patching `AppConfig`s. Scenario state is in memory, so restarting the simulator pod resets the sliders (but not workloads it scaled or deployed; use **Reset everything** for those).

## Tearing down

```bash
scripts/teardown.sh
```

Deletes the `metrics-stack` k3d cluster.

## Repo layout

```
helm/                       Helm values for kube-prometheus-stack and Istio (istiod)
manifests/
  kafka/                    Kafka StatefulSet (KRaft, JMX exporter)
  loki/                     Loki + Alloy
  istio/                    Istio namespace, envoy/istiod ServiceMonitors, traffic-generator
  kyverno/                  Kyverno ClusterPolicy
  operator/                 Operator CRD, RBAC, Deployment
  monitoring/               ServiceMonitors, Grafana dashboards, recording/alert rules (incl. Istio, Kafka, workloads)
  simulator/                Test Simulator RBAC, Deployment, Services
  log-generator/            log-generator app + log-sender web UI
  namespace.yaml            demo-stack namespace (Istio sidecar injection enabled)
  deployment.yaml           sample-web workload
  statefulset-storage.yaml  metrics-storage workload
operator/                   Self-healing operator source (Python, kopf) + Dockerfile
simulator/                  Test Simulator source (Python + static UI) + Dockerfile
scripts/
  setup.sh                  Bring up the whole stack
  teardown.sh               Delete the k3d cluster
  k3d-config.yaml           k3d cluster/port config
  send-kafka-message.sh     Send a single Kafka message via the broker pod
```
