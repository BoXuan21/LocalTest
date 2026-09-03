import os
import time
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import kopf
from prometheus_client import start_http_server, Counter, Gauge

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("self-healing-operator")

START_TIME = time.time()
SELF_DESTRUCT_SECONDS = int(os.environ.get("SELF_DESTRUCT_SECONDS", "180"))

RECONCILE_TOTAL = Counter(
    "operator_reconcile_total",
    "Number of AppConfig reconcile events handled",
)
UPTIME_SECONDS = Gauge(
    "operator_uptime_seconds",
    "Seconds since this operator process started",
)
RESTART_COUNTDOWN = Gauge(
    "operator_seconds_until_self_restart",
    "Seconds remaining before this operator process deliberately exits",
)

_ready = False


def _update_uptime_loop():
    while True:
        elapsed = time.time() - START_TIME
        UPTIME_SECONDS.set(elapsed)
        RESTART_COUNTDOWN.set(max(0, SELF_DESTRUCT_SECONDS - elapsed))
        time.sleep(2)


def _self_destruct_loop():
    logger.info("Self-destruct armed: exiting after %s seconds", SELF_DESTRUCT_SECONDS)
    time.sleep(SELF_DESTRUCT_SECONDS)
    logger.warning("Self-destruct timer elapsed - exiting so Kubernetes restarts the container")
    os._exit(1)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200 if _ready else 503)
            self.end_headers()
            self.wfile.write(b"ok" if _ready else b"starting")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):  # silence default access logging
        return


def _serve_health():
    HTTPServer(("0.0.0.0", 8081), HealthHandler).serve_forever()


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    global _ready
    settings.posting.level = logging.INFO
    _ready = True
    logger.info("Operator started, watching AppConfig resources")


@kopf.on.create("demo.example.com", "v1", "appconfigs")
@kopf.on.update("demo.example.com", "v1", "appconfigs")
def reconcile_appconfig(spec, name, namespace, patch, **_):
    RECONCILE_TOTAL.inc()
    message = spec.get("message", "hello from the operator")
    logger.info("Reconciling AppConfig %s/%s -> %s", namespace, name, message)
    patch.status["lastReconciledMessage"] = message
    patch.status["lastReconciledAt"] = time.time()


def main():
    start_http_server(8080)  
    threading.Thread(target=_serve_health, daemon=True).start()
    threading.Thread(target=_update_uptime_loop, daemon=True).start()
    threading.Thread(target=_self_destruct_loop, daemon=True).start()
    kopf.run(standalone=True)


if __name__ == "__main__":
    main()
