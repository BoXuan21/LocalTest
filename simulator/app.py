"""Test-scenario simulator for the k3d metrics stack.

Runs two HTTP servers in one process:

  :8080  control UI + JSON API + /metrics (excluded from the Istio sidecar)
  :8000  "sim-target", a mesh-visible backend whose latency/error rate is
         controlled from the UI, so Istio alerts can be triggered on demand

plus background loops that generate mesh traffic, produce Kafka messages and
emit log lines, and a thin Kubernetes API client for infra-level faults
(scaling workloads to zero, crash loops, OOM kills, Kyverno admission checks).
"""

import json
import logging
import os
import random
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

logging.getLogger("kafka").setLevel(logging.ERROR)

NAMESPACE = os.environ.get("POD_NAMESPACE", "demo-stack")
TARGET_URL = os.environ.get("TARGET_URL", "http://sim-target.demo-stack.svc.cluster.local:8000/")
PROMETHEUS_URL = os.environ.get(
    "PROMETHEUS_URL", "http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090"
)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka.kafka.svc.cluster.local:9092")
OPERATOR_HEALTH_URL = os.environ.get(
    "OPERATOR_HEALTH_URL", "http://operator-metrics.demo-stack.svc.cluster.local:8081"
)
STATIC_DIR = Path(__file__).parent / "static"

MAX_INFLIGHT = 500
FAULT_LABEL = "simulator.demo/fault"

DEFAULTS = {
    "traffic": {"rps": 5, "latency_ms": 20, "slow_pct": 0, "slow_ms": 0, "error_pct": 0},
    "kafka": {"topic": "sim-events", "rate": 1},
    "logs": {"rate": 0, "error_pct": 10},
}
LIMITS = {
    "traffic": {"rps": (0, 100), "latency_ms": (0, 5000), "slow_pct": (0, 100), "slow_ms": (0, 10000), "error_pct": (0, 100)},
    "kafka": {"rate": (0, 200)},
    "logs": {"rate": (0, 50), "error_pct": (0, 100)},
}

_lock = threading.Lock()
STATE = json.loads(json.dumps(DEFAULTS))

# --- metrics -----------------------------------------------------------------

TRAFFIC_REQUESTS = Counter("sim_traffic_requests_total", "Requests sent to sim-target", ["code"])
TRAFFIC_DURATION = Histogram(
    "sim_traffic_request_duration_seconds", "Client-side latency of requests to sim-target",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
TRAFFIC_DROPPED = Counter("sim_traffic_dropped_total", "Requests skipped because too many were in flight")
TRAFFIC_INFLIGHT = Gauge("sim_traffic_inflight", "Requests to sim-target currently in flight")
KAFKA_PRODUCED = Counter("sim_kafka_messages_produced_total", "Kafka messages acknowledged", ["topic"])
KAFKA_ERRORS = Counter("sim_kafka_produce_errors_total", "Kafka produce failures")
LOG_LINES = Counter("sim_log_lines_total", "Log lines emitted by the simulator", ["level"])
SETTING = Gauge("sim_setting", "Current simulator setting", ["group", "name"])

# --- activity log ------------------------------------------------------------

ACTIVITY = deque(maxlen=60)


def logfmt(level, msg, **fields):
    extra = "".join(f' {k}="{v}"' if isinstance(v, str) else f" {k}={v}" for k, v in fields.items())
    line = f'level={level} msg="{msg}"{extra} ts={int(time.time())}'
    print(line, flush=True)
    LOG_LINES.labels(level).inc()


def record(msg, ok=True, **fields):
    ACTIVITY.appendleft({"ts": time.time(), "msg": msg, "ok": ok})
    logfmt("info" if ok else "warn", msg, source="simulator-control", **fields)


def update_settings(group, values):
    clean = {}
    for key, value in values.items():
        if key == "topic" and group == "kafka":
            topic = "".join(c for c in str(value) if c.isalnum() or c in "-_.")[:100]
            if topic:
                clean[key] = topic
        elif key in LIMITS[group]:
            lo, hi = LIMITS[group][key]
            clean[key] = max(lo, min(hi, float(value)))
    with _lock:
        STATE[group].update(clean)
        snapshot = dict(STATE[group])
    for key, value in snapshot.items():
        if isinstance(value, (int, float)):
            SETTING.labels(group, key).set(value)
    return snapshot


def get_settings(group):
    with _lock:
        return dict(STATE[group])


# --- sim-target backend (:8000) ----------------------------------------------


class Server(ThreadingHTTPServer):
    # the stdlib default backlog of 5 causes SYN drops (and 1-3s retransmit
    # delays) under concurrent load, which would distort the injected latency
    request_queue_size = 1024
    daemon_threads = True



class TargetHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        cfg = get_settings("traffic")
        delay = cfg["latency_ms"] * random.uniform(0.8, 1.2)
        if cfg["slow_pct"] and random.random() * 100 < cfg["slow_pct"]:
            delay += cfg["slow_ms"]
        time.sleep(delay / 1000)
        failed = cfg["error_pct"] and random.random() * 100 < cfg["error_pct"]
        status = 500 if failed else 200
        body = json.dumps({"status": status, "delay_ms": round(delay)}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


# --- traffic generator -------------------------------------------------------

_inflight = 0
_inflight_lock = threading.Lock()
RECENT = deque(maxlen=5000)  # (finished_at, status_code, duration_s)


def _send_one():
    global _inflight
    start = time.monotonic()
    code = "error"
    try:
        with urllib.request.urlopen(TARGET_URL, timeout=30) as resp:
            resp.read()
            code = str(resp.status)
    except urllib.error.HTTPError as exc:
        code = str(exc.code)
    except Exception:
        code = "error"
    finally:
        duration = time.monotonic() - start
        TRAFFIC_REQUESTS.labels(code).inc()
        TRAFFIC_DURATION.observe(duration)
        RECENT.append((time.time(), code, duration))
        with _inflight_lock:
            _inflight -= 1
            TRAFFIC_INFLIGHT.set(_inflight)


def traffic_loop():
    global _inflight
    pool = ThreadPoolExecutor(max_workers=MAX_INFLIGHT)
    next_at = time.monotonic()
    while True:
        rps = get_settings("traffic")["rps"]
        if rps <= 0:
            time.sleep(0.25)
            next_at = time.monotonic()
            continue
        now = time.monotonic()
        if next_at > now:
            time.sleep(next_at - now)
        # never try to "catch up" more than a second's worth after a stall
        next_at = max(next_at + 1 / rps, time.monotonic() - 1)
        with _inflight_lock:
            if _inflight >= MAX_INFLIGHT:
                TRAFFIC_DROPPED.inc()
                continue
            _inflight += 1
            TRAFFIC_INFLIGHT.set(_inflight)
        pool.submit(_send_one)


def traffic_stats(window=10):
    now = time.time()
    samples = [s for s in list(RECENT) if s[0] >= now - window]
    if not samples:
        return {"rps": 0, "p50_ms": None, "p99_ms": None, "error_pct": 0, "inflight": _inflight}
    span = max(1.0, min(window, now - samples[0][0]))
    durations = sorted(s[2] for s in samples)

    def pct(q):
        return round(durations[min(len(durations) - 1, int(q * len(durations)))] * 1000)

    errors = sum(1 for s in samples if not s[1].startswith("2"))
    return {
        "rps": round(len(samples) / span, 1),
        "p50_ms": pct(0.50),
        "p99_ms": pct(0.99),
        "error_pct": round(100 * errors / len(samples), 1),
        "inflight": _inflight,
    }


# --- kafka producer ----------------------------------------------------------

_producer = None
_producer_lock = threading.Lock()
_kafka_status = {"connected": False, "last_error": None, "produced": 0, "errors": 0}


def _get_producer():
    global _producer
    with _producer_lock:
        if _producer is None:
            from kafka import KafkaProducer

            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                linger_ms=20,
                max_block_ms=3000,
                request_timeout_ms=5000,
                retries=0,
                value_serializer=lambda v: json.dumps(v).encode(),
            )
        return _producer


def _drop_producer():
    global _producer
    with _producer_lock:
        if _producer is not None:
            try:
                _producer.close(timeout=1)
            except Exception:
                pass
        _producer = None


def _on_sent(topic):
    def ok(_meta):
        KAFKA_PRODUCED.labels(topic).inc()
        _kafka_status["produced"] += 1
        _kafka_status["connected"] = True

    return ok


def _on_failed(exc):
    KAFKA_ERRORS.inc()
    _kafka_status["errors"] += 1
    _kafka_status["last_error"] = str(exc)[:200]


def produce(topic, count):
    try:
        producer = _get_producer()
        for _ in range(count):
            msg = {"source": "simulator", "ts": time.time(), "id": random.getrandbits(32)}
            producer.send(topic, msg).add_callback(_on_sent(topic)).add_errback(_on_failed)
        producer.flush(timeout=10)
        return True
    except Exception as exc:
        _on_failed(exc)
        _kafka_status["connected"] = False
        _drop_producer()
        return False


def kafka_loop():
    carry = 0.0
    while True:
        cfg = get_settings("kafka")
        if cfg["rate"] <= 0:
            carry = 0.0
            time.sleep(0.5)
            continue
        start = time.monotonic()
        carry += cfg["rate"]
        count, carry = int(carry), carry - int(carry)
        if count and not produce(cfg["topic"], count):
            time.sleep(2)  # broker unreachable - back off before reconnecting
        time.sleep(max(0.0, 1 - (time.monotonic() - start)))


# --- log emitter -------------------------------------------------------------

LOG_MESSAGES = {
    "info": ["request handled", "cache hit", "user logged in"],
    "warn": ["slow response", "retrying upstream call", "cache miss"],
    "error": ["request failed", "database connection refused", "upstream timeout"],
}


def emit_log(level):
    logfmt(level, random.choice(LOG_MESSAGES[level]), source="simulator", seq=random.getrandbits(16))


def logs_loop():
    while True:
        cfg = get_settings("logs")
        if cfg["rate"] <= 0:
            time.sleep(0.5)
            continue
        roll = random.random() * 100
        if roll < cfg["error_pct"]:
            emit_log("error")
        elif roll < cfg["error_pct"] + 10:
            emit_log("warn")
        else:
            emit_log("info")
        time.sleep(1 / cfg["rate"])


# --- kubernetes API ----------------------------------------------------------

SA_DIR = Path("/var/run/secrets/kubernetes.io/serviceaccount")


class KubeError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


class Kube:
    def __init__(self):
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        self.base = f"https://{host}:{port}" if host else None
        self.ctx = ssl.create_default_context(cafile=str(SA_DIR / "ca.crt")) if host else None

    def call(self, method, path, body=None, content_type="application/json"):
        if not self.base:
            raise KubeError(0, "not running inside a cluster")
        token = (SA_DIR / "token").read_text().strip()  # re-read: projected tokens rotate
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=10) as resp:
                return json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as exc:
            try:
                message = json.loads(exc.read()).get("message", exc.reason)
            except Exception:
                message = str(exc.reason)
            raise KubeError(exc.code, message) from None

    def merge_patch(self, path, body):
        return self.call("PATCH", path, body, "application/merge-patch+json")


kube = Kube()

# workloads the UI can scale; key -> (namespace, kind, name)
SCALABLE = {
    "operator": (NAMESPACE, "deployments", "self-healing-operator"),
    "traffic-generator": (NAMESPACE, "deployments", "traffic-generator"),
    "kafka": ("kafka", "statefulsets", "kafka"),
}


def scale(target, replicas):
    ns, kind, name = SCALABLE[target]
    kube.merge_patch(f"/apis/apps/v1/namespaces/{ns}/{kind}/{name}/scale", {"spec": {"replicas": int(replicas)}})


def workload_status(target):
    ns, kind, name = SCALABLE[target]
    try:
        obj = kube.call("GET", f"/apis/apps/v1/namespaces/{ns}/{kind}/{name}")
        return {"replicas": obj["spec"].get("replicas", 0), "ready": obj.get("status", {}).get("readyReplicas", 0)}
    except KubeError as exc:
        return {"error": exc.message}


def _resources(cpu, memory, req_cpu=None, req_memory=None):
    return {
        "requests": {"cpu": req_cpu or cpu, "memory": req_memory or memory},
        "limits": {"cpu": cpu, "memory": memory},
    }


PY_IMAGE = "python:3.12-alpine"
BUSYBOX = "busybox:1.36"

# Each fault is a throwaway Deployment; `container` is merged into a pod spec
# that already satisfies the Kyverno policy (app label + requests/limits).
FAULTS = {
    "crashloop": {
        "title": "CrashLoopBackOff",
        "container": {
            "image": BUSYBOX,
            "command": ["sh", "-c", "echo 'level=error msg=\"simulated fatal error, exiting\"'; sleep 5; exit 1"],
            "resources": _resources("50m", "16Mi"),
        },
    },
    "pending": {
        "title": "Unschedulable pod",
        "container": {
            "image": BUSYBOX,
            "command": ["sleep", "infinity"],
            "resources": _resources("64", "16Mi"),  # no node has 64 CPUs
        },
    },
    "memory-pressure": {
        "title": "Memory near limit",
        "container": {
            "image": PY_IMAGE,
            "command": [
                "python3", "-c",
                "import time\n"
                "hog = [b'x' * (1024 * 1024) for _ in range(80)]\n"
                "print('level=warn msg=\"holding 80MiB of a 96Mi limit\"', flush=True)\n"
                "while True: time.sleep(60)\n",
            ],
            "resources": _resources("100m", "96Mi"),
        },
    },
    "oom": {
        "title": "OOM kill",
        "container": {
            "image": PY_IMAGE,
            "command": [
                "python3", "-c",
                "import time\n"
                "hog = []\n"
                "while True:\n"
                "    hog.append(b'x' * (4 * 1024 * 1024))\n"
                "    print(f'level=warn msg=\"leaking memory\" held_mib={len(hog) * 4}', flush=True)\n"
                "    time.sleep(0.5)\n",
            ],
            "resources": _resources("100m", "64Mi"),
        },
    },
    "cpu-burn": {
        "title": "CPU burn",
        "container": {
            "image": BUSYBOX,
            "command": ["sh", "-c", "while :; do :; done"],
            "resources": _resources("100m", "16Mi"),
        },
    },
}


def _fault_deployment(key):
    name = f"sim-{key}"
    labels = {"app": name, FAULT_LABEL: key}
    container = {"name": "fault", **FAULTS[key]["container"]}
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": NAMESPACE, "labels": labels},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                # keep the fault pods out of the mesh so the sidecar doesn't mask the failure
                "metadata": {"labels": labels, "annotations": {"sidecar.istio.io/inject": "false"}},
                "spec": {"terminationGracePeriodSeconds": 1, "containers": [container]},
            },
        },
    }


def set_fault(key, enabled):
    path = f"/apis/apps/v1/namespaces/{NAMESPACE}/deployments"
    if enabled:
        try:
            kube.call("POST", path, _fault_deployment(key))
        except KubeError as exc:
            if exc.status != 409:  # already exists
                raise
    else:
        try:
            kube.call("DELETE", f"{path}/sim-{key}", {"propagationPolicy": "Background"})
        except KubeError as exc:
            if exc.status != 404:
                raise


def fault_status():
    try:
        items = kube.call(
            "GET", f"/apis/apps/v1/namespaces/{NAMESPACE}/deployments?labelSelector={FAULT_LABEL}"
        ).get("items", [])
    except KubeError as exc:
        return {"error": exc.message}
    active = {d["metadata"]["labels"][FAULT_LABEL]: d.get("status", {}).get("readyReplicas", 0) for d in items}
    return {key: {"active": key in active, "ready": active.get(key, 0)} for key in FAULTS}


KYVERNO_CASES = {
    "no-resources": {"labels": {"app": "kyverno-test"}, "resources": None},
    "no-label": {"labels": {}, "resources": _resources("10m", "16Mi")},
    "compliant": {"labels": {"app": "kyverno-test"}, "resources": _resources("10m", "16Mi")},
}


def kyverno_test(case):
    spec = KYVERNO_CASES[case]
    container = {"name": "test", "image": BUSYBOX, "command": ["sleep", "1"]}
    if spec["resources"]:
        container["resources"] = spec["resources"]
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"generateName": "kyverno-test-", "namespace": NAMESPACE, "labels": spec["labels"]},
        "spec": {"containers": [container]},
    }
    try:
        # dryRun: the request goes through admission (incl. Kyverno) but nothing is persisted
        kube.call("POST", f"/api/v1/namespaces/{NAMESPACE}/pods?dryRun=All", pod)
        return {"allowed": True, "message": "Admitted (dry run - nothing was created)."}
    except KubeError as exc:
        if exc.status in (400, 403, 422) and "denied" in exc.message:
            return {"allowed": False, "message": exc.message}
        raise


def operator_reconcile():
    kube.merge_patch(
        f"/apis/demo.example.com/v1/namespaces/{NAMESPACE}/appconfigs/demo-config",
        {"spec": {"message": f"poked by simulator at {time.strftime('%H:%M:%S')}"}},
    )


def operator_crash():
    with urllib.request.urlopen(OPERATOR_HEALTH_URL + "/crash", timeout=5) as resp:
        resp.read()


# --- prometheus --------------------------------------------------------------


def fetch_alert_rules():
    url = PROMETHEUS_URL + "/api/v1/rules?type=alert"
    with urllib.request.urlopen(url, timeout=5) as resp:
        payload = json.loads(resp.read())
    rules = []
    for group in payload.get("data", {}).get("groups", []):
        for rule in group.get("rules", []):
            alerts = rule.get("alerts", [])
            active_at = min((a["activeAt"] for a in alerts if a.get("activeAt")), default=None)
            rules.append({
                "name": rule["name"],
                "state": rule.get("state", "inactive"),
                "for_s": rule.get("duration", 0),
                "severity": rule.get("labels", {}).get("severity", ""),
                "active_at": active_at,
                "count": len(alerts),
                "summary": rule.get("annotations", {}).get("summary", ""),
                "instances": [
                    {k: v for k, v in a.get("labels", {}).items() if k not in ("alertname", "severity", "prometheus")}
                    for a in alerts[:5]
                ],
            })
    return rules


# --- control API (:8080) -----------------------------------------------------


def reset_all():
    errors = []
    for group in DEFAULTS:
        update_settings(group, DEFAULTS[group])
    for target in SCALABLE:
        try:
            scale(target, 1)
        except KubeError as exc:
            errors.append(f"scale {target}: {exc.message}")
    for key in FAULTS:
        try:
            set_fault(key, False)
        except KubeError as exc:
            errors.append(f"fault {key}: {exc.message}")
    return errors


def handle_action(path, body):
    """Return (status, payload). Raises KubeError for API failures."""
    if path in ("/api/traffic", "/api/kafka", "/api/logs"):
        group = path.rsplit("/", 1)[1]
        settings = update_settings(group, body)
        record(f"{group} settings → " + ", ".join(f"{k}={v:g}" if isinstance(v, float) else f"{k}={v}" for k, v in settings.items()))
        return 200, settings

    if path == "/api/kafka/burst":
        count = max(1, min(10000, int(body.get("count", 1000))))
        topic = get_settings("kafka")["topic"]
        threading.Thread(target=produce, args=(topic, count), daemon=True).start()
        record(f"kafka burst of {count} messages to {topic}")
        return 202, {"queued": count}

    if path == "/api/logs/burst":
        level = body.get("level", "error")
        if level not in LOG_MESSAGES:
            return 400, {"error": "unknown level"}
        count = max(1, min(1000, int(body.get("count", 100))))
        for _ in range(count):
            emit_log(level)
        record(f"emitted {count} {level} log lines")
        return 200, {"emitted": count}

    if path == "/api/scale":
        target, replicas = body.get("target"), int(body.get("replicas", 1))
        if target not in SCALABLE or replicas not in (0, 1):
            return 400, {"error": "bad target/replicas"}
        scale(target, replicas)
        record(f"scaled {target} to {replicas}", ok=replicas > 0)
        return 200, {"ok": True}

    if path == "/api/operator/crash":
        try:
            operator_crash()
        except Exception as exc:
            return 502, {"error": f"operator unreachable: {exc}"}
        record("crashed the operator container", ok=False)
        return 200, {"ok": True}

    if path == "/api/operator/reconcile":
        operator_reconcile()
        record("patched AppConfig demo-config to trigger a reconcile")
        return 200, {"ok": True}

    if path == "/api/fault":
        key, enabled = body.get("fault"), bool(body.get("enabled"))
        if key not in FAULTS:
            return 400, {"error": "unknown fault"}
        set_fault(key, enabled)
        record(f"{'started' if enabled else 'stopped'} fault: {FAULTS[key]['title']}", ok=not enabled)
        return 200, {"ok": True}

    if path == "/api/kyverno":
        case = body.get("case")
        if case not in KYVERNO_CASES:
            return 400, {"error": "unknown case"}
        result = kyverno_test(case)
        record(f"kyverno test '{case}': {'admitted' if result['allowed'] else 'denied'}", ok=result["allowed"])
        return 200, result

    if path == "/api/reset":
        errors = reset_all()
        record("reset everything to baseline", ok=not errors)
        return 200, {"errors": errors}

    return 404, {"error": "not found"}


class ControlHandler(BaseHTTPRequestHandler):
    def _send(self, status, payload, content_type="application/json"):
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path in ("/", "/index.html"):
            self._send(200, (STATIC_DIR / "index.html").read_bytes(), "text/html; charset=utf-8")
        elif path == "/metrics":
            self._send(200, generate_latest(), CONTENT_TYPE_LATEST)
        elif path == "/healthz":
            self._send(200, {"ok": True})
        elif path == "/api/state":
            with _lock:
                settings = json.loads(json.dumps(STATE))
            self._send(200, {
                "settings": settings,
                "traffic_stats": traffic_stats(),
                "kafka": {**_kafka_status, "bootstrap": KAFKA_BOOTSTRAP},
                "workloads": {t: workload_status(t) for t in SCALABLE},
                "faults": fault_status(),
                "activity": list(ACTIVITY),
            })
        elif path == "/api/alerts":
            try:
                self._send(200, {"rules": fetch_alert_rules()})
            except Exception as exc:
                self._send(502, {"error": f"prometheus unreachable: {exc}"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(body, dict):
                raise ValueError("body must be an object")
        except ValueError as exc:
            self._send(400, {"error": f"bad request: {exc}"})
            return
        try:
            status, payload = handle_action(path, body)
        except KubeError as exc:
            record(f"{path} failed: {exc.message}", ok=False)
            status, payload = 502, {"error": exc.message}
        except (TypeError, ValueError) as exc:
            status, payload = 400, {"error": str(exc)}
        self._send(status, payload)

    def log_message(self, fmt, *args):
        return


def main():
    for group in DEFAULTS:
        update_settings(group, DEFAULTS[group])
    for loop in (traffic_loop, kafka_loop, logs_loop):
        threading.Thread(target=loop, daemon=True).start()
    threading.Thread(
        target=Server(("0.0.0.0", 8000), TargetHandler).serve_forever, daemon=True
    ).start()
    record("simulator started")
    Server(("0.0.0.0", 8080), ControlHandler).serve_forever()


if __name__ == "__main__":
    main()
