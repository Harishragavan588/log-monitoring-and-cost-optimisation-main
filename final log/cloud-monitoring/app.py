import time
import sys
import logging

import queue
import threading
import datetime
import os
import json
import re
import concurrent.futures

from flask import Flask, request, jsonify, render_template
from zoneinfo import ZoneInfo

try:
    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30), name="IST")

# ─── Google Cloud SDK ────────────────────────────────────────────────────────
try:
    from google.cloud import logging as gcp_logging
    from google.cloud import monitoring_v3
    from google.cloud import firestore as gcp_firestore
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

GCP_PROJECT  = "shop-proj-thiru-9988"
GCP_LOG_NAME = "flask-monitoring-app"   # Our app's log stream in Cloud Logging
GCP_CACHE_TTL = 1                       # Fast 1-second refresh for real-time dashboard updates

app = Flask(__name__)

# ─── Standard logging setup ──────────────────────────────────────────────────
logging.basicConfig(
    stream=sys.stdout, level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─── In-memory write buffer: used only for async GCP transmission ─────────────
# NOT used as a log source for the dashboard — all reads come from GCP.
log_store = {
    "total_logs":        0,
    "errors":            0,
    "warnings":          0,
    "info":              0,
    "logins_count":      0,
    "successful_logins": 0,
    "failed_logins":     0,
    "recent_logs":       []
}


# ─── GCP clients ─────────────────────────────────────────────────────────────
_gcp_log_client       = None
_gcp_logger           = None        # app's named logger in Cloud Logging
_gcp_metrics_client   = None
_gcp_firestore_client = None
_init_lock = threading.Lock()

def _init_gcp():
    global _gcp_log_client, _gcp_logger, _gcp_metrics_client, _gcp_firestore_client
    if _gcp_firestore_client is not None and _gcp_log_client is not None:
        return
    with _init_lock:
        if _gcp_firestore_client is not None and _gcp_log_client is not None:
            return
        if not GCP_AVAILABLE:
            return
        try:
            # Standard ADC — works automatically in Google Cloud Run
            _gcp_log_client       = gcp_logging.Client(project=GCP_PROJECT)
            _gcp_logger           = _gcp_log_client.logger(GCP_LOG_NAME)
            _gcp_metrics_client   = monitoring_v3.MetricServiceClient()
            _gcp_firestore_client = gcp_firestore.Client(project=GCP_PROJECT)
            logger.info(f"GCP clients ready — project={GCP_PROJECT}, log={GCP_LOG_NAME}, firestore=ready")
            return
        except Exception as e:
            logger.info(f"Standard ADC initialization: {e}. Checking local gcloud credentials...")

        try:
            import subprocess
            import google.oauth2.credentials
            token = subprocess.check_output("gcloud auth print-access-token", shell=True).decode().strip()
            creds = google.oauth2.credentials.Credentials(token)
            _gcp_log_client       = gcp_logging.Client(project=GCP_PROJECT, credentials=creds)
            _gcp_logger           = _gcp_log_client.logger(GCP_LOG_NAME)
            _gcp_metrics_client   = monitoring_v3.MetricServiceClient(credentials=creds)
            _gcp_firestore_client = gcp_firestore.Client(project=GCP_PROJECT, credentials=creds)
            logger.info(f"GCP clients ready (via local gcloud credentials) — project={GCP_PROJECT}, firestore=ready")
        except Exception as e:
            logger.warning(f"GCP init failed: {e}")

# ─── Async GCP Write Queue ────────────────────────────────────────────────────
# Logs are enqueued here and written to Cloud Logging by a background thread.
# This keeps Flask request handlers fast — no blocking GCP network calls.
_write_queue = queue.Queue(maxsize=200)
_log_delivery_failures = 0
_log_delivery_failures_lock = threading.Lock()

def _gcp_writer_loop():
    """Background thread: drain write queue to Google Cloud Logging with bounded retry."""
    global _log_delivery_failures
    _init_gcp()
    while True:
        try:
            item = _write_queue.get(timeout=0.1)
        except queue.Empty:
            continue

        if not _gcp_logger:
            with _log_delivery_failures_lock:
                _log_delivery_failures += 1
            sys.stderr.write(f"[LOG_FAILURE] GCP logger not initialized. Total failed logs: {_log_delivery_failures}\n")
            sys.stderr.flush()
            continue

        success = False
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                _gcp_logger.log_struct(
                    item["payload"],
                    severity=item["severity"],
                    labels={"app": "flask-monitoring", "service": item["payload"].get("service", "app")}
                )
                success = True
                break
            except Exception as ex:
                if attempt < max_retries:
                    time.sleep(0.05 * (2 ** (attempt - 1)))
                else:
                    with _log_delivery_failures_lock:
                        _log_delivery_failures += 1
                    sys.stderr.write(f"[LOG_FAILURE] GCP log write failed after {max_retries} attempts: {ex}. Total failed logs: {_log_delivery_failures}\n")
                    sys.stderr.flush()

def _enqueue_gcp_log(payload: dict, severity: str):
    """
    Put log entry onto the asynchronous write queue.
    On queue saturation, first attempt synchronous delivery to Google Cloud Logging.
    If delivery fails, explicitly record the failure locally to application stderr
    and increment a failure counter. Never silently remove an existing queued log to make space for a new one.
    """
    global _log_delivery_failures
    try:
        _write_queue.put_nowait({"payload": payload, "severity": severity})
    except queue.Full:
        delivered = False
        if _gcp_logger:
            try:
                _gcp_logger.log_struct(
                    payload,
                    severity=severity,
                    labels={"app": "flask-monitoring", "service": payload.get("service", "app")}
                )
                delivered = True
            except Exception as ex:
                sys.stderr.write(f"[LOG_FAILURE] Synchronous GCP log write failed on queue saturation: {ex}\n")
                sys.stderr.flush()
        if not delivered:
            with _log_delivery_failures_lock:
                _log_delivery_failures += 1
            sys.stderr.write(f"[LOG_FAILURE] Logging queue saturated and direct write failed. Total failed logs: {_log_delivery_failures}\n")
            sys.stderr.flush()


# ─── GCP Read Cache ───────────────────────────────────────────────────────────
_cache = {
    "logs":           None,
    "metrics":        None,
    "ack_alerts_map": {},
    "logs_ts":        0,
    "metrics_ts":     0,
    "ack_alerts_ts":  0,
}
_cache_lock = threading.RLock()

def _severity_to_level(sev):
    s = str(sev).upper()
    if any(x in s for x in ("ERROR", "CRITICAL", "ALERT", "EMERGENCY")):
        return "ERROR"
    if "WARN" in s:
        return "WARNING"
    return "INFO"

def _level_to_gcp_severity(level):
    return {"ERROR": "ERROR", "WARNING": "WARNING", "INFO": "INFO"}.get(level, "DEFAULT")


def _safe_doc_id(raw_id: str) -> str:
    """
    Sanitize raw string for use as a Firestore document path key.
    The canonical alert_id is preserved in the document payload.
    """
    if not raw_id:
        return ""
    safe = re.sub(r'[/\\#?]+', '_', str(raw_id).strip())
    return safe[:1500]


def format_ist_timestamp(ts) -> str:
    """
    Convert any timestamp/datetime (UTC, string, float, or Firestore timestamp)
    to standardized Indian Standard Time (Asia/Kolkata) string format:
    e.g. '06-Sep-2026 08:30:15 PM IST'
    """
    if ts is None:
        return None
    try:
        if isinstance(ts, (int, float)):
            dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
            return dt.astimezone(IST).strftime("%d-%b-%Y %I:%M:%S %p IST")
        if isinstance(ts, str):
            dt = datetime.datetime.fromisoformat(ts.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(IST).strftime("%d-%b-%Y %I:%M:%S %p IST")
        if hasattr(ts, 'astimezone'):
            if getattr(ts, 'tzinfo', None) is None:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
            return ts.astimezone(IST).strftime("%d-%b-%Y %I:%M:%S %p IST")
        return str(ts)
    except Exception as ex:
        logger.warning(f"Error formatting IST timestamp: {ex}")
        return str(ts)


def _get_acknowledged_alerts_map() -> dict:
    """
    Retrieve map of acknowledged alerts from Firestore collection 'acknowledged_alerts'.
    Returns { canonical_alert_id: { 'status': 'ACKNOWLEDGED', 'acknowledged_at': '...' } }
    Cached in memory for 2.0 seconds to keep dashboard polling snappy.
    """
    global _gcp_firestore_client
    now = time.time()
    with _cache_lock:
        if "ack_alerts_map" in _cache and (now - _cache.get("ack_alerts_ts", 0)) < 2.0:
            return dict(_cache["ack_alerts_map"])

    if not _gcp_firestore_client:
        _init_gcp()
    if not _gcp_firestore_client:
        return {}

    try:
        docs = _gcp_firestore_client.collection("acknowledged_alerts").stream()
        ack_map = {}
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        for doc in docs:
            data = doc.to_dict() or {}
            # Application-layer TTL check: proactively filter out expired acknowledgements (> 7 days)
            exp = data.get("expires_at")
            if exp:
                try:
                    if hasattr(exp, 'astimezone'):
                        if exp.astimezone(datetime.timezone.utc) <= now_utc:
                            continue
                    elif isinstance(exp, str):
                        dt = datetime.datetime.fromisoformat(exp.replace('Z', '+00:00'))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=datetime.timezone.utc)
                        if dt <= now_utc:
                            continue
                except Exception:
                    pass

            canonical_id = data.get("alert_id") or doc.id
            ack_time = format_ist_timestamp(data.get("acknowledged_at"))
            # The returned acknowledgement map is keyed strictly by the original canonical alert_id
            ack_map[str(canonical_id)] = {
                "status": data.get("status", "ACKNOWLEDGED"),
                "acknowledged_at": ack_time
            }

        with _cache_lock:
            _cache["ack_alerts_map"] = ack_map
            _cache["ack_alerts_ts"]  = time.time()
        return ack_map
    except Exception as e:
        logger.warning(f"Firestore ack_alerts fetch error: {e}")
        with _cache_lock:
            return dict(_cache.get("ack_alerts_map", {}))


def _fetch_gcp_logs():
    """Read last 7 days of logs from Google Cloud Logging (rolling 7-day window)."""
    if not _gcp_log_client:
        return None
    try:
        # Rolling 7-day cutoff — logs older than 7 days are excluded from every query
        cutoff     = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
        cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        log_name   = f"projects/{GCP_PROJECT}/logs/{GCP_LOG_NAME}"

        filter_str = (
            f'logName="{log_name}" '
            f'timestamp >= "{cutoff_str}"'
        )

        entries = list(_gcp_log_client.list_entries(
            resource_names=[f"projects/{GCP_PROJECT}"],
            filter_=filter_str,
            order_by=gcp_logging.DESCENDING,
            page_size=200
        ))

        counts  = {"errors": 0, "warnings": 0, "info": 0,
                   "successful_logins": 0, "failed_logins": 0, "new_users": 0}
        recent  = []
        ack_map = _get_acknowledged_alerts_map()

        for idx, e in enumerate(entries):
            level = _severity_to_level(e.severity)
            counts[{"ERROR": "errors", "WARNING": "warnings"}.get(level, "info")] += 1

            payload = e.payload if isinstance(e.payload, dict) else {}
            msg     = payload.get("message", str(e.payload))[:200]
            service = payload.get("service", "app")
            etype   = payload.get("event_type", "")

            msg_lower = msg.lower()
            if etype == "registration" or "registration" in msg_lower or "account created" in msg_lower or "new user" in msg_lower:
                counts["new_users"] += 1
                counts["successful_logins"] += 1
            elif etype == "login_success":
                counts["successful_logins"] += 1
            elif etype == "login_failed":
                counts["failed_logins"]     += 1

            # Timestamp handling: capture UTC ISO string for exact alert lifecycle tracking
            raw_dt = getattr(e, 'timestamp', None)
            if raw_dt:
                if hasattr(raw_dt, 'astimezone'):
                    raw_utc = raw_dt.astimezone(datetime.timezone.utc)
                else:
                    raw_utc = raw_dt
                raw_iso = raw_utc.isoformat()
            else:
                raw_utc = datetime.datetime.now(datetime.timezone.utc)
                raw_iso = raw_utc.isoformat()

            # Convert timestamp to standardized Indian Standard Time (IST)
            ts = format_ist_timestamp(raw_utc)

            # Canonical alert_id from Google Cloud log (e.insert_id or deterministic fallback)
            alert_id = getattr(e, 'insert_id', None)
            if not alert_id:
                raw_time = e.timestamp.isoformat() if getattr(e, 'timestamp', None) else str(idx)
                alert_id = f"{raw_time}_{service}_{level}_{abs(hash(msg))}"

            ack_info = ack_map.get(str(alert_id))
            is_acked = ack_info is not None

            recent.append({
                "id": alert_id,
                "alert_id": alert_id,
                "time": ts,
                "timestamp_utc": raw_iso,
                "alert_timestamp": raw_iso,
                "level": level,
                "service": service,
                "message": msg,
                "acknowledged": is_acked,
                "acknowledged_at": ack_info.get("acknowledged_at") if ack_info else None
            })

        # Build Alert History (all ERROR events from rolling 7-day GCP window)
        alert_history = []
        for l in recent:
            if l["level"] == "ERROR":
                status = "ACKNOWLEDGED" if l["acknowledged"] else "ACTIVE"
                alert_history.append({
                    "alert_id":        l["alert_id"],
                    "id":              l["alert_id"],
                    "time":            l["time"],
                    "timestamp_utc":   l.get("timestamp_utc"),
                    "alert_timestamp": l.get("alert_timestamp"),
                    "service":         l["service"],
                    "message":         l["message"],
                    "severity":        "CRITICAL",
                    "status":          status,
                    "acknowledged_at": l.get("acknowledged_at")
                })

        # Calculate active (unacknowledged) ERROR logs
        active_error_logs = [
            l for l in alert_history
            if l["status"] == "ACTIVE"
        ]
        active_alerts_count = len(active_error_logs)

        result = {
            "total_logs":          len(entries),
            "errors":              counts["errors"],
            "active_alerts_count": active_alerts_count,
            "active_error_logs":   active_error_logs[:25],
            "alert_history":       alert_history,
            "warnings":            counts["warnings"],
            "info":                counts["info"],
            "logins_count":        counts["successful_logins"],
            "successful_logins":   counts["successful_logins"],
            "failed_logins":       counts["failed_logins"],
            "new_users":           counts["new_users"],
            "recent_logs":         recent[:25],
            "all_logs":            recent
        }
        logger.info(
            f"GCP read: {result['total_logs']} logs | "
            f"Active Alerts={result['active_alerts_count']} (Total E={result['errors']}, Hist={len(alert_history)}) W={result['warnings']} I={result['info']} | "
            f"Login OK={result['successful_logins']} FAIL={result['failed_logins']} NEW={result['new_users']}"
        )
        return result

    except Exception as e:
        logger.error(f"GCP log read error: {e}")
        return None


GCP_SERVICE_NAME = "cloud-monitoring"   # Our Cloud Run service name

def _fetch_gcp_metrics():
    """
    Read Cloud Run CPU / memory / container count in PARALLEL from Google Cloud Monitoring.
    cpu/utilizations and memory/utilizations are DISTRIBUTION metrics.
    Instance count is a GAUGE metric.
    Concurrent queries via ThreadPoolExecutor eliminate serial network latency.
    """
    if not _gcp_metrics_client:
        return None
    try:
        now = int(time.time())
        # 1-hour lookback captures active metric points reliably across revisions
        interval = monitoring_v3.TimeInterval({
            "end_time":   {"seconds": now},
            "start_time": {"seconds": now - 3600}
        })
        project_name = f"projects/{GCP_PROJECT}"

        def _fetch_dist_metric(metric_type):
            filter_expr = (
                f'metric.type="{metric_type}" AND '
                f'resource.labels.service_name="{GCP_SERVICE_NAME}"'
            )
            latest_val = None
            latest_t = -1
            try:
                for ts in _gcp_metrics_client.list_time_series(request={
                    "name":     project_name,
                    "filter":   filter_expr,
                    "interval": interval,
                    "view":     monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
                }):
                    for pt in (ts.points or []):
                        t_sec = pt.interval.end_time.timestamp() if hasattr(pt.interval.end_time, 'timestamp') else (
                            pt.interval.end_time.seconds if hasattr(pt.interval.end_time, 'seconds') else 0
                        )
                        dv = getattr(pt.value, "distribution_value", None)
                        val = None
                        if dv is not None and getattr(dv, "mean", None) is not None:
                            val = dv.mean
                        else:
                            dbl = getattr(pt.value, "double_value", None)
                            if dbl is not None:
                                val = dbl

                        if val is not None and t_sec > latest_t:
                            latest_t = t_sec
                            latest_val = val
            except Exception as ex:
                logger.warning(f"GCP metric query error for {metric_type}: {ex}")

            return round(latest_val * 100, 1) if latest_val is not None else None

        def _fetch_instances():
            filter_inst = (
                'metric.type="run.googleapis.com/container/instance_count" AND '
                f'resource.labels.service_name="{GCP_SERVICE_NAME}"'
            )
            latest_active = None
            latest_active_t = -1
            latest_idle = None
            latest_idle_t = -1
            try:
                for ts in _gcp_metrics_client.list_time_series(request={
                    "name":     project_name,
                    "filter":   filter_inst,
                    "interval": interval,
                    "view":     monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
                }):
                    labels = dict(ts.metric.labels or {})
                    state  = labels.get("state", "active")
                    for pt in (ts.points or []):
                        t_sec = pt.interval.end_time.timestamp() if hasattr(pt.interval.end_time, 'timestamp') else (
                            pt.interval.end_time.seconds if hasattr(pt.interval.end_time, 'seconds') else 0
                        )
                        val = None
                        if getattr(pt.value, "int64_value", None) is not None:
                            val = int(pt.value.int64_value)
                        elif getattr(pt.value, "double_value", None) is not None:
                            val = int(pt.value.double_value)

                        if val is not None:
                            if state == "active":
                                if t_sec > latest_active_t:
                                    latest_active_t = t_sec
                                    latest_active = val
                            elif state == "idle":
                                if t_sec > latest_idle_t:
                                    latest_idle_t = t_sec
                                    latest_idle = val
            except Exception as ex:
                logger.warning(f"GCP instance query error: {ex}")

            return {"active": latest_active, "idle": latest_idle}

        # Execute metric queries in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            fut_cpu  = executor.submit(_fetch_dist_metric, "run.googleapis.com/container/cpu/utilizations")
            fut_mem  = executor.submit(_fetch_dist_metric, "run.googleapis.com/container/memory/utilizations")
            fut_inst = executor.submit(_fetch_instances)

            cpu_val  = fut_cpu.result()
            mem_val  = fut_mem.result()
            inst_val = fut_inst.result()

        result = {}
        if cpu_val is not None:
            result["cpu_pct"] = cpu_val
        if mem_val is not None:
            result["mem_pct"] = mem_val
        if inst_val:
            if inst_val.get("active") is not None:
                result["active_instances"] = inst_val["active"]
                result["instance_count"]   = inst_val["active"]
            if inst_val.get("idle") is not None:
                result["idle_instances"]   = inst_val["idle"]

        logger.info(f"GCP Monitoring '{GCP_SERVICE_NAME}' (parallel): {result}")
        return result if result else None

    except Exception as e:
        logger.error(f"GCP metrics read error: {e}")
        return None


def _get_cached_logs():
    """Return GCP logs, refreshing cache if TTL expired (thread-safe)."""
    now = time.time()
    with _cache_lock:
        if _cache["logs"] is not None and (now - _cache["logs_ts"]) <= 1.5:
            return _cache["logs"]

    fresh = _fetch_gcp_logs()
    if fresh is not None:
        with _cache_lock:
            _cache["logs"]    = fresh
            _cache["logs_ts"] = time.time()
    with _cache_lock:
        return _cache["logs"]


def _get_cached_metrics():
    """Return GCP metrics, refreshing cache if TTL expired (thread-safe)."""
    now = time.time()
    with _cache_lock:
        if _cache["metrics"] is not None and (now - _cache["metrics_ts"]) <= 2.5:
            return _cache["metrics"]

    fresh = _fetch_gcp_metrics()
    if fresh is not None:
        with _cache_lock:
            _cache["metrics"]    = fresh
            _cache["metrics_ts"] = time.time()
    with _cache_lock:
        return _cache["metrics"]


# ─── Core log recorder ───────────────────────────────────────────────────────

def record_log(level, service, message, extra: dict = None):
    """
    Write one log entry to:
      1. Local in-memory store (immediate, for fallback / local display)
      2. Google Cloud Logging (async via write queue)
    """
    # Local store
    log_store["total_logs"] += 1
    if level == "ERROR":
        log_store["errors"]   += 1
    elif level == "WARNING":
        log_store["warnings"] += 1
    else:
        log_store["info"]     += 1

    entry = {
        "time": format_ist_timestamp(datetime.datetime.now(datetime.timezone.utc)),
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "level": level,
        "service": service,
        "message": message
    }
    log_store["recent_logs"].insert(0, entry)
    if len(log_store["recent_logs"]) > 200:   # keep up to 200 past logs
        log_store["recent_logs"].pop()

    # GCP Cloud Logging (async)
    payload = {"message": message, "service": service, "level": level}
    if extra:
        payload.update(extra)
    _enqueue_gcp_log(payload, _level_to_gcp_severity(level))


# ─── Lazy GCP init on first request ──────────────────────────────────────────
@app.before_request
def ensure_gcp():
    _init_gcp()


# ─── Customer Application Routes ─────────────────────────────────────────────

@app.route("/")
def login_page():
    record_log("INFO", "Frontend", "User accessed login page")
    return render_template("login.html")


@app.route("/shop")
def shop_page():
    record_log("INFO", "Frontend", "User accessed e-commerce shop page")
    return render_template("index.html")


@app.route("/api/track-login", methods=["POST"])
def track_login():
    """Called by customer UI on successful login or account creation."""
    data       = request.get_json() or {}
    login_type = data.get("type", "Email/Password")

    log_store["logins_count"]      += 1
    log_store["successful_logins"] += 1

    is_registration = (login_type == "Registration" or "register" in login_type.lower() or "create" in login_type.lower())
    event_type = "registration" if is_registration else "login_success"
    action_text = "New user registration via Email/Password" if is_registration else f"Successful login via {login_type}"

    record_log(
        "INFO", "Auth-Service",
        action_text,
        extra={
            "event_type": event_type,
            "login_type": login_type,
            "successful_logins": log_store["successful_logins"],
            "failed_logins":     log_store["failed_logins"]
        }
    )

    return jsonify({
        "status":            "tracked",
        "total_logins":      log_store["logins_count"],
        "successful_logins": log_store["successful_logins"],
        "failed_logins":     log_store["failed_logins"]
    }), 200


@app.route("/api/track-failed-login", methods=["POST"])
def track_failed_login():
    """Called by customer UI on failed login attempt."""
    data       = request.get_json() or {}
    reason     = data.get("reason", "Invalid credentials")
    login_type = data.get("type", "Email/Password")

    log_store["failed_logins"] += 1

    record_log(
        "WARNING", "Auth-Service",
        f"Failed login attempt via {login_type}: {reason}",
        extra={
            "event_type": "login_failed",
            "login_type": login_type,
            "error_code": reason,
            "successful_logins": log_store["successful_logins"],
            "failed_logins":     log_store["failed_logins"]
        }
    )

    return jsonify({
        "status":            "tracked",
        "successful_logins": log_store["successful_logins"],
        "failed_logins":     log_store["failed_logins"]
    }), 200


def _create_order_in_firestore(item: str, quantity: int):
    """
    Atomically generates the next sequential order ID and stores the order in Firestore.
    Uses a Firestore transaction to prevent duplicate IDs across concurrent Cloud Run instances.
    """
    global _gcp_firestore_client
    if not _gcp_firestore_client:
        _init_gcp()
    if not _gcp_firestore_client:
        raise RuntimeError("Google Cloud Firestore client is not available")

    counter_ref = _gcp_firestore_client.collection("metadata").document("order_counter")

    @gcp_firestore.transactional
    def _txn_step(txn):
        snapshot = counter_ref.get(transaction=txn)
        current_id = 0
        if snapshot.exists:
            current_id = snapshot.get("last_order_id") or 0

        next_order_id = current_id + 1

        # Atomically update counter
        txn.set(counter_ref, {
            "last_order_id": next_order_id,
            "updated_at": gcp_firestore.SERVER_TIMESTAMP
        }, merge=True)

        # Write persistent order document
        order_ref = _gcp_firestore_client.collection("orders").document(str(next_order_id))
        order_doc = {
            "order_id": next_order_id,
            "item": item,
            "quantity": quantity,
            "status": "Order Placed",
            "created_at": gcp_firestore.SERVER_TIMESTAMP
        }
        txn.set(order_ref, order_doc)
        return next_order_id

    transaction = _gcp_firestore_client.transaction()
    order_id = _txn_step(transaction)

    # Read back the actual stored Firestore timestamp as the single source of truth
    created_at = None
    try:
        doc = _gcp_firestore_client.collection("orders").document(str(order_id)).get()
        if doc.exists:
            created_at = doc.to_dict().get("created_at")
    except Exception as ex:
        logger.warning(f"Could not read back created_at for order #{order_id}: {ex}")

    return order_id, created_at


@app.route("/order", methods=["POST"])
def create_order():
    data     = request.get_json() or {}
    item     = data.get("item")
    quantity = data.get("quantity")

    if not item or not quantity:
        record_log("WARNING", "Order-Service",
                   "Failed order creation attempt: missing fields",
                   extra={"event_type": "order_failed"})
        return jsonify({"error": "Item and quantity are required"}), 400

    try:
        order_id, created_at = _create_order_in_firestore(item, quantity)
        ist_created_at = format_ist_timestamp(created_at)
        record_log("INFO", "Order-Service",
                   f"Order #{order_id} created: {quantity}x {item}",
                   extra={"event_type": "order_created", "order_id": order_id})
        return jsonify({
            "id": order_id,
            "status": "Order Placed",
            "created_at": ist_created_at
        }), 201
    except Exception as e:
        logger.error(f"Firestore order creation failed: {e}")
        record_log("ERROR", "Order-Service",
                   f"Order creation failed in database: {e}",
                   extra={"event_type": "order_error"})
        return jsonify({"error": "Failed to create order in database"}), 500


@app.route("/order/<int:order_id>", methods=["GET"])
def get_order(order_id):
    global _gcp_firestore_client
    if not _gcp_firestore_client:
        _init_gcp()
    if not _gcp_firestore_client:
        return jsonify({"error": "Database not initialized"}), 500

    try:
        doc = _gcp_firestore_client.collection("orders").document(str(order_id)).get()
        if not doc.exists:
            record_log("WARNING", "Order-Service",
                       f"Lookup failed: Order #{order_id} not found")
            return jsonify({"error": "Order not found"}), 404

        order_data = doc.to_dict()
        created_at = order_data.get("created_at")
        order_data["created_at"] = format_ist_timestamp(created_at)

        record_log("INFO", "Order-Service",
                   f"Successfully looked up Order #{order_id}")
        return jsonify(order_data), 200
    except Exception as e:
        logger.error(f"Firestore lookup error for order #{order_id}: {e}")
        return jsonify({"error": "Error retrieving order from database"}), 500


@app.route("/api/simulate-error", methods=["POST"])
def simulate_error():
    """
    Simulates an application incident for demonstration purposes.
    Protected by ENABLE_ERROR_SIMULATION environment variable (default: false).
    """
    if os.getenv("ENABLE_ERROR_SIMULATION", "false").lower() != "true":
        return jsonify({
            "status": "forbidden",
            "error": "Error simulation is disabled in this environment. Set ENABLE_ERROR_SIMULATION=true to enable."
        }), 403

    record_log("ERROR", "Order-Service",
               "Order processing timeout or API execution failure detected!",
               extra={"event_type": "order_error"})
    return jsonify({"status": "error_logged"}), 200



@app.route("/developer/dashboard")
def developer_dashboard():
    log_poll_ms = int(os.getenv("LOG_POLL_INTERVAL_MS", "5000"))
    metrics_poll_ms = int(os.getenv("METRICS_POLL_INTERVAL_MS", "5000"))
    return render_template(
        "developer_dashboard.html",
        logs=log_store,
        metrics={},
        log_poll_interval_ms=log_poll_ms,
        metrics_poll_interval_ms=metrics_poll_ms
    )


@app.route("/api/debug/metrics")
def debug_metrics_api():
    """
    Debug endpoint — shows raw GCP Monitoring response and internal diagnostic telemetry.
    Protected by ENABLE_DEBUG_ENDPOINTS environment variable (default: false).
    """
    if os.getenv("ENABLE_DEBUG_ENDPOINTS", "false").lower() != "true":
        return jsonify({
            "status": "forbidden",
            "error": "Debug endpoints are disabled in production. Set ENABLE_DEBUG_ENDPOINTS=true to enable."
        }), 403

    _init_gcp()
    gcp_raw  = None
    gcp_err  = None
    try:
        gcp_raw = _fetch_gcp_metrics()
    except Exception as ex:
        gcp_err = str(ex)

    # Also probe instance_count directly (simpler INT64 metric)
    instance_direct = None
    instance_err    = None
    if _gcp_metrics_client:
        try:
            now = int(time.time())
            interval = monitoring_v3.TimeInterval({
                "end_time":   {"seconds": now},
                "start_time": {"seconds": now - 7200}
            })
            agg = monitoring_v3.Aggregation({
                "alignment_period":   {"seconds": 60},
                "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN
            })
            rows = list(_gcp_metrics_client.list_time_series(request={
                "name":        f"projects/{GCP_PROJECT}",
                "filter":      f'metric.type="run.googleapis.com/container/instance_count" AND resource.labels.service_name="{GCP_SERVICE_NAME}"',
                "interval":    interval,
                "aggregation": agg,
                "view":        monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
            }))
            instance_direct = len(rows)
        except Exception as ex:
            instance_err = str(ex)

    return jsonify({
        "gcp_metrics_client_ready":  _gcp_metrics_client is not None,
        "gcp_raw_result":            gcp_raw,
        "gcp_fetch_error":           gcp_err,
        "gcp_service_filter":        GCP_SERVICE_NAME,
        "instance_count_direct_rows":instance_direct,
        "instance_count_error":      instance_err,
        "psutil_available":          False,
        "psutil_mem_mb":             None,
        "cache_state": {
            "metrics_cached": _cache["metrics"] is not None,
            "metrics_age_s":  round(time.time() - _cache["metrics_ts"], 1)
        }
    }), 200


# ─── Monitoring API: reads exclusively from Google Cloud ─────────────────────

@app.route("/api/monitoring/logs")
def monitoring_logs_api():
    """
    Returns logs ONLY from Google Cloud Logging (rolling 7-day window).
    No local fallback — all log data must originate from GCP.
    If GCP is unavailable or has no logs, an explicit empty state is returned.
    """
    gcp = _get_cached_logs()

    if gcp and gcp["total_logs"] > 0:
        return jsonify({**gcp, "source": "google-cloud-logging", "no_data": False}), 200

    # Cache is cold — attempt a direct live fetch from GCP
    if _gcp_log_client:
        gcp_direct = _fetch_gcp_logs()
        if gcp_direct and gcp_direct["total_logs"] > 0:
            with _cache_lock:
                _cache["logs"] = gcp_direct
                _cache["logs_ts"] = time.time()
            return jsonify({**gcp_direct, "source": "google-cloud-logging", "no_data": False}), 200

    # GCP returned no logs — explicit empty state, no local data substituted
    return jsonify({
        "total_logs":          0,
        "errors":              0,
        "active_alerts_count": 0,
        "active_error_logs":   [],
        "alert_history":       [],
        "warnings":            0,
        "info":                0,
        "logins_count":        0,
        "successful_logins":   0,
        "failed_logins":       0,
        "new_users":           0,
        "recent_logs":         [],
        "all_logs":            [],
        "source":              "google-cloud-logging",
        "no_data":             True,
        "message":             "No logs available from Google Cloud Logging."
    }), 200


@app.route("/api/monitoring/alerts/acknowledge", methods=["POST"])
def acknowledge_alert():
    """
    Acknowledge one or multiple active alerts.
    Persists acknowledgement status to Firestore collection 'acknowledged_alerts'.
    Does NOT delete logs from Google Cloud Logging.
    """
    global _gcp_firestore_client
    if not _gcp_firestore_client:
        _init_gcp()
    if not _gcp_firestore_client:
        return jsonify({"error": "Firestore database not available"}), 500

    data = request.get_json() or {}
    single_id = data.get("alert_id")
    raw_ids   = data.get("alert_ids")
    ack_all   = data.get("all", False)
    single_ts = data.get("alert_timestamp") or data.get("timestamp_utc")
    raw_alerts = data.get("alerts")

    # Map of alert_id -> original timestamp for exact 7-day lifecycle tracking
    alert_time_map = {}
    if single_id and single_ts:
        alert_time_map[str(single_id)] = single_ts
    if raw_alerts and isinstance(raw_alerts, list):
        for item in raw_alerts:
            if isinstance(item, dict):
                aid = str(item.get("id") or item.get("alert_id") or "")
                ats = item.get("alert_timestamp") or item.get("timestamp_utc")
                if aid and ats:
                    alert_time_map[aid] = ats

    cached_logs = _get_cached_logs() or {}
    active_errs = cached_logs.get("active_error_logs") or []

    # Also extract original timestamps from cached log pools
    all_logs_to_check = (
        active_errs +
        (cached_logs.get("alert_history") or []) +
        (cached_logs.get("recent_logs") or [])
    )
    for log_item in all_logs_to_check:
        aid = str(log_item.get("alert_id") or log_item.get("id") or "")
        ats = log_item.get("timestamp_utc") or log_item.get("alert_timestamp")
        if aid and ats and aid not in alert_time_map:
            alert_time_map[aid] = ats

    ids_to_ack = []
    if single_id:
        ids_to_ack.append(str(single_id))
    if raw_ids and isinstance(raw_ids, list):
        for aid in raw_ids:
            if aid:
                ids_to_ack.append(str(aid))

    if ack_all or not ids_to_ack:
        for err in active_errs:
            err_id = err.get("alert_id") or err.get("id")
            if err_id:
                ids_to_ack.append(err_id)

    ids_to_ack = list(set(ids_to_ack))
    if not ids_to_ack:
        return jsonify({"status": "no_op", "message": "No active alerts to acknowledge", "count": 0}), 200

    try:
        batch = _gcp_firestore_client.batch()
        batch_count = 0
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        for aid in ids_to_ack:
            safe_id = _safe_doc_id(aid)
            if not safe_id:
                continue

            # Base TTL expiration strictly on original alert event timestamp + 7 days
            alert_ts_val = alert_time_map.get(str(aid))
            alert_dt = None
            if alert_ts_val:
                try:
                    if isinstance(alert_ts_val, datetime.datetime):
                        alert_dt = alert_ts_val
                    elif isinstance(alert_ts_val, str):
                        alert_dt = datetime.datetime.fromisoformat(alert_ts_val.replace('Z', '+00:00'))
                    if alert_dt and alert_dt.tzinfo is None:
                        alert_dt = alert_dt.replace(tzinfo=datetime.timezone.utc)
                    elif alert_dt:
                        alert_dt = alert_dt.astimezone(datetime.timezone.utc)
                except Exception as ex:
                    logger.debug(f"Could not parse alert timestamp {alert_ts_val}: {ex}")
                    alert_dt = None

            # Fallback direct lookup in GCP logging if not found in payload/cache
            if not alert_dt and _gcp_log_client:
                try:
                    entries = list(_gcp_log_client.list_entries(
                        resource_names=[f"projects/{GCP_PROJECT}"],
                        filter_=f'logName="projects/{GCP_PROJECT}/logs/{GCP_LOG_NAME}" insertId="{aid}"',
                        page_size=1
                    ))
                    if entries and hasattr(entries[0], 'timestamp') and entries[0].timestamp:
                        alert_dt = entries[0].timestamp
                        if hasattr(alert_dt, 'astimezone'):
                            alert_dt = alert_dt.astimezone(datetime.timezone.utc)
                        elif alert_dt.tzinfo is None:
                            alert_dt = alert_dt.replace(tzinfo=datetime.timezone.utc)
                except Exception as ex:
                    logger.debug(f"GCP direct insertId lookup failed: {ex}")

            if not alert_dt:
                return jsonify({
                    "error": f"Original alert timestamp could not be resolved for alert: {aid}"
                }), 422

            # Expiration aligns strictly with Google Cloud Logging's 7-day retention window:
            # expires_at = alert_timestamp + 7 days
            expires_dt = alert_dt + datetime.timedelta(days=7)

            doc_ref = _gcp_firestore_client.collection("acknowledged_alerts").document(safe_id)
            batch.set(doc_ref, {
                "alert_id": aid,
                "status": "ACKNOWLEDGED",
                "acknowledged_at": gcp_firestore.SERVER_TIMESTAMP,
                "alert_timestamp": alert_dt,
                "expires_at": expires_dt
            }, merge=True)
            batch_count += 1
            if batch_count >= 450:
                batch.commit()
                batch = _gcp_firestore_client.batch()
                batch_count = 0

        if batch_count > 0:
            batch.commit()

        # Update in-memory cache immediately so subsequent calls reflect state
        now_ist = format_ist_timestamp(datetime.datetime.now(datetime.timezone.utc))
        with _cache_lock:
            if "ack_alerts_map" not in _cache or not isinstance(_cache["ack_alerts_map"], dict):
                _cache["ack_alerts_map"] = {}
            for aid in ids_to_ack:
                canonical_id = str(aid)
                # Keyed strictly by original canonical alert_id
                _cache["ack_alerts_map"][canonical_id] = {
                    "status": "ACKNOWLEDGED",
                    "acknowledged_at": now_ist
                }
            _cache["ack_alerts_ts"] = time.time()
            _cache["logs"] = None
            _cache["logs_ts"] = 0

        logger.info(f"Successfully acknowledged {len(ids_to_ack)} alert(s) in Firestore.")
        return jsonify({
            "status": "success",
            "message": f"Acknowledged {len(ids_to_ack)} alert(s)",
            "count": len(ids_to_ack)
        }), 200

    except Exception as e:
        logger.error(f"Error acknowledging alert in Firestore: {e}")
        return jsonify({"error": str(e)}), 500



def _get_container_memory_limit_mb() -> int:
    """
    Dynamically determine the Cloud Run container allocated memory limit in MB.
    1. Reads Linux container cgroup limits (/sys/fs/cgroup/memory.max or /sys/fs/cgroup/memory/memory.limit_in_bytes)
    2. Reads CLOUD_RUN_MEMORY_MB environment variable if set
    3. Returns None if limit cannot be detected (no synthetic fallback)
    """
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            if os.path.exists(path):
                with open(path, "r") as f:
                    val = f.read().strip()
                    if val and val != "max":
                        bytes_val = int(val)
                        if 16 * 1024 * 1024 <= bytes_val < 1024 * 1024 * 1024 * 1024:
                            return round(bytes_val / (1024 * 1024))
        except Exception:
            pass

    env_mem = os.environ.get("CLOUD_RUN_MEMORY_MB")
    if env_mem:
        try:
            return int(env_mem)
        except ValueError:
            pass

    return None


@app.route("/api/monitoring/metrics")
def monitoring_metrics_api():
    """
    Returns REAL container metrics directly from Google Cloud Monitoring.
    All values — CPU, Memory, Instance count — come exclusively from GCP.
    No simulated, estimated, or artificially scaled values are used.
    When GCP telemetry is unavailable, returns N/A and no_data=true.
    """
    try:
        mem_total_mb = _get_container_memory_limit_mb()

        # ── Fetch real GCP metrics ────────────────────────────────────────────
        gcp_m = _get_cached_metrics() or {}

        # CPU — from run.googleapis.com/container/cpu/utilizations (distribution mean × 100)
        # 100% genuine GCP metric. No artificial multipliers, no psutil or simulated fallbacks.
        gcp_cpu_pct = gcp_m.get("cpu_pct")
        if gcp_cpu_pct is not None and gcp_cpu_pct >= 0:
            cpu_raw         = round(gcp_cpu_pct, 1)
            cpu_display     = f"{cpu_raw}%"
            cpu_avg_display = f"{cpu_raw}%"
            cpu_no_data     = False
        else:
            cpu_raw         = None
            cpu_display     = "N/A"
            cpu_avg_display = "N/A"
            cpu_no_data     = True

        # Memory — from run.googleapis.com/container/memory/utilizations (distribution mean × 100)
        # Real Memory Used = (Real GCP Memory % / 100) × Actual Container Limit MB
        gcp_mem_pct = gcp_m.get("mem_pct")
        if gcp_mem_pct is not None and gcp_mem_pct >= 0 and mem_total_mb is not None:
            mem_pct         = round(min(100.0, gcp_mem_pct), 1)
            mem_mb          = round((mem_pct / 100.0) * mem_total_mb)
            mem_display     = f"{mem_mb} MB / {mem_total_mb} MB"
            mem_no_data     = False
        else:
            mem_pct         = None
            mem_mb          = None
            mem_total_mb    = None
            mem_display     = "N/A"
            mem_no_data     = True

        # Instance count — latest gauge value from run.googleapis.com/container/instance_count
        active_instances = gcp_m.get("active_instances")
        idle_instances   = gcp_m.get("idle_instances")
        inst_no_data     = (active_instances is None)

        any_core_metric_missing = (cpu_no_data or mem_no_data or inst_no_data)
        all_core_metrics_missing = (cpu_no_data and mem_no_data and inst_no_data)
        no_data = all_core_metrics_missing

        # Cost — Honest reporting (not connected to Google Cloud Billing API)
        estimated_monthly_cost = None
        cost_display = "N/A"
        cost_source = "not-connected"

        # ── Recommendations based on genuine observed values ─────────────────
        suggestions = []

        # Priority 1: Scaled to Zero (active_instances == 0)
        if active_instances == 0:
            suggestions.append({
                "icon": "💤",
                "text": "Service is currently scaled to zero. No container instances are actively running. Cloud Run is reducing idle infrastructure usage and cost. CPU and memory performance cannot be evaluated until the service receives traffic."
            })
            suggestions.append({
                "icon": "ℹ️",
                "text": "Cost data is currently unavailable because Cloud Billing integration is not connected."
            })

        # Priority 2: Missing Telemetry
        elif no_data or cpu_raw is None or mem_pct is None:
            suggestions.append({
                "icon": "ℹ️",
                "text": "Telemetry data is currently unavailable. Google Cloud Monitoring may still be collecting data or the service may not have recent monitoring observations."
            })
            suggestions.append({
                "icon": "ℹ️",
                "text": "Cost data is currently unavailable because Cloud Billing integration is not connected."
            })

        # Priority 3-6: Active Workload (active_instances > 0 and telemetry available)
        else:
            # 3. CPU Utilization Recommendation
            if cpu_raw < 40:
                suggestions.append({
                    "icon": "💡",
                    "text": "CPU utilization is low. The service currently has sufficient CPU capacity. Continue monitoring traffic before reducing allocated resources."
                })
            elif 40 <= cpu_raw <= 70:
                suggestions.append({
                    "icon": "⚡",
                    "text": "CPU utilization is moderate. Current resource allocation appears appropriate. Continue monitoring usage trends."
                })
            else:
                suggestions.append({
                    "icon": "⚠️",
                    "text": "High CPU utilization detected. Monitor application performance and traffic. Consider optimizing the application or increasing available CPU resources if high utilization continues."
                })

            # 4. Memory Utilization Recommendation
            if mem_pct < 50:
                suggestions.append({
                    "icon": "💡",
                    "text": "Memory utilization is low. Current memory capacity appears sufficient. Monitor usage over time before changing the container memory configuration."
                })
            elif 50 <= mem_pct <= 75:
                suggestions.append({
                    "icon": "⚡",
                    "text": "Memory utilization is moderate. Continue monitoring for sustained increases."
                })
            else:
                suggestions.append({
                    "icon": "⚠️",
                    "text": "High memory utilization detected. Consider investigating memory usage or increasing the Cloud Run memory allocation if high utilization is sustained."
                })

            # 5. Container Instance Scaling Recommendation
            if 1 <= active_instances <= 3:
                suggestions.append({
                    "icon": "✅",
                    "text": "Container scaling is operating normally. Continue monitoring traffic and instance behavior."
                })
            else:
                suggestions.append({
                    "icon": "⚠️",
                    "text": "Multiple container instances are active. Increased traffic may be causing Cloud Run to scale horizontally. Monitor traffic patterns and application efficiency."
                })

            # 6. Cost Recommendation
            suggestions.append({
                "icon": "ℹ️",
                "text": "Cost data is currently unavailable because Cloud Billing integration is not connected."
            })

        return jsonify({
            # Professional structured objects
            "cpu": {
                "value": cpu_raw,
                "display": cpu_display,
                "no_data": cpu_no_data
            },
            "memory": {
                "value": mem_pct,
                "used_mb": mem_mb,
                "total_mb": mem_total_mb,
                "display": mem_display,
                "no_data": mem_no_data
            },
            "instances": {
                "active": active_instances,
                "idle": idle_instances,
                "display": str(active_instances) if active_instances is not None else "N/A",
                "no_data": inst_no_data
            },
            "cost": {
                "estimated_monthly_cost": estimated_monthly_cost,
                "display": cost_display,
                "source": cost_source
            },
            "any_core_metric_missing": any_core_metric_missing,
            "all_core_metrics_missing": all_core_metrics_missing,
            # Backwards-compatible legacy fields for existing UI
            "cpu_raw":                cpu_raw,
            "cpu_avg":                cpu_avg_display,
            "cpu_utilization":        cpu_display,
            "memory_mb":              mem_mb,
            "memory_total_mb":        mem_total_mb,
            "memory_pct":             mem_pct,
            "memory_usage":           mem_display,
            "active_instances":       active_instances,
            "idle_instances":         idle_instances,
            "estimated_monthly_cost": estimated_monthly_cost,
            "cost_display":           cost_display,
            "cost_source":            cost_source,
            "metrics_source":         "google-cloud-monitoring",
            "no_data":                no_data,
            "suggestions":            suggestions
        }), 200

    except Exception as e:
        logger.error(f"metrics API error: {e}")
        return jsonify({
            "cpu": {"value": None, "display": "N/A", "no_data": True},
            "memory": {"value": None, "used_mb": None, "total_mb": None, "display": "N/A", "no_data": True},
            "instances": {"active": None, "idle": None, "display": "N/A", "no_data": True},
            "cost": {"estimated_monthly_cost": None, "display": "N/A", "source": "not-connected"},
            "any_core_metric_missing": True,
            "all_core_metrics_missing": True,
            "cpu_raw":                None,
            "cpu_avg":                "N/A",
            "cpu_utilization":        "N/A",
            "memory_mb":              None,
            "memory_total_mb":        None,
            "memory_pct":             None,
            "memory_usage":           "N/A",
            "active_instances":       None,
            "idle_instances":         None,
            "estimated_monthly_cost": None,
            "cost_display":           "N/A",
            "cost_source":            "not-connected",
            "metrics_source":         "google-cloud-monitoring",
            "no_data":                True,
            "suggestions": [
                {"icon": "⚠️", "text": "Unable to fetch Google Cloud Monitoring metrics. Check service account permissions."}
            ]
        }), 200


# ─── Start GCP async writer thread ───────────────────────────────────────────
_writer_thread = threading.Thread(
    target=_gcp_writer_loop, daemon=True, name="gcp-writer"
)
_writer_thread.start()
logger.info("GCP async writer thread started.")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)