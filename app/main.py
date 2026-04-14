import subprocess
import signal
import os
import re
import json
import time
from collections import defaultdict
from datetime import datetime
from typing import Optional

import requests

import broker_impl
from fastapi import FastAPI, HTTPException, Depends, Request, Query
from pydantic import BaseModel, Field, field_validator
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Worker Control API", docs_url=None, redoc_url=None, openapi_url=None)

API_KEY = os.getenv("API_KEY")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(BASE_DIR, "data")

os.makedirs(DATA_DIR, exist_ok=True)

# ── Registry ──
workers: dict[str, dict] = {}       # run_id -> {process, pid, args, ...} (active only)
worker_history: dict[str, dict] = {}  # run_id -> {pid, args, status, started_at, stopped_at, ...} (all)


# ── JSON persistence helpers ──

def _today_folder() -> str:
    folder = os.path.join(DATA_DIR, datetime.now().strftime("%d%b%Y"))
    os.makedirs(folder, exist_ok=True)
    return folder


def _save_worker_json(run_id: str, data: dict):
    path = os.path.join(_today_folder(), f"{run_id}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def _safe_client_file_tag(client_db_id: int | str, client_id: str) -> str:
    """Filesystem-safe prefix: {client_db_id}_{client_id} for shared broker JSON."""
    cid = re.sub(r"[^a-zA-Z0-9_-]", "_", str(client_id)).strip("_")[:80] or "client"
    return f"{client_db_id}_{cid}"


def _broker_snapshot_path(tag: str, snapshot: str) -> str:
    if snapshot not in ("orders", "positions", "margins"):
        raise ValueError(snapshot)
    return os.path.join(_today_folder(), f"{tag}_{snapshot}.json")


def _save_broker_snapshot(tag: str, snapshot: str, data: dict):
    path = _broker_snapshot_path(tag, snapshot)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def _sidecar_json_names() -> tuple[str, ...]:
    return ("_orders.json", "_positions.json", "_margins.json")


def _resolve_run_status_json_path(run_id: str) -> str | None:
    """IDP writes ``{run_id}_status.json`` next to the worker log file."""
    candidates: list[str] = []
    candidates.append(os.path.join(_today_folder(), f"{run_id}_status.json"))
    if run_id in worker_history:
        lf = worker_history[run_id].get("log_file")
        if lf:
            candidates.append(os.path.join(os.path.dirname(os.path.abspath(lf)), f"{run_id}_status.json"))
    seen: set[str] = set()
    for c in candidates:
        ap = os.path.abspath(c)
        if ap in seen:
            continue
        seen.add(ap)
        if os.path.isfile(ap):
            return ap
    return None


def _set_exit_flag_in_worker_json(run_id: str, command: str):
    """Set ``exit_trade`` or ``exit_algo`` to true in ``{run_id}.json`` (same dir as log)."""
    path = os.path.join(_today_folder(), f"{run_id}.json")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"No worker state file {path!r}. Is this run_id started today?")
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        raise HTTPException(status_code=500, detail=f"Cannot read worker json: {e}") from e
    if not isinstance(data, dict):
        data = {}
    if command == "exit_trade":
        data["exit_trade"] = True
    elif command == "exit_algo":
        data["exit_algo"] = True
    else:
        raise HTTPException(status_code=400, detail=f"Unknown command {command!r}")
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Cannot write worker json: {e}") from e
    if run_id in worker_history:
        worker_history[run_id] = data


def _load_today_history():
    """Load worker JSON files from today's folder into worker_history on startup."""
    folder = _today_folder()
    for filename in os.listdir(folder):
        if not filename.endswith(".json"):
            continue
        if any(filename.endswith(sfx) for sfx in _sidecar_json_names()):
            continue
        run_id = filename[:-5]
        with open(os.path.join(folder, filename)) as f:
            data = json.load(f)
        worker_history[run_id] = data


_load_today_history()


# ── Auth ──

def verify_api_key(request: Request):
    key = request.headers.get("X-API-Key")
    if not key:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            key = auth[7:]
    if not key:
        raise HTTPException(status_code=401, detail="Missing API key. Use 'X-API-Key' or 'Authorization: Bearer <key>'.")
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key.")
    return key


# ── Request Model ──

class StartRequest(BaseModel):
    client_db_id: Optional[int] = None
    client_id: Optional[str] = Field(None, max_length=20)

    strategy_id: Optional[str] = Field(None, max_length=20)
    run_id: str = Field(..., max_length=20)
    tranche: Optional[str] = Field(None, max_length=20)

    broker: str = Field(
        ...,
        max_length=20,
        description="Broker: kite / zerodha, or kotak (runs common worker.py).",
    )
    static_ip: Optional[str] = Field(None, max_length=20)

    api_key: Optional[str] = None
    access_token: Optional[str] = None
    root_url: Optional[str] = None

    instrument: Optional[str] = None
    is_investor_client: Optional[str] = None

    no_of_lots: Optional[int] = None
    entry_time: str = Field(..., description="Start time e.g. 09:21:30")
    exit_time: str = Field(..., description="Exit time e.g. 15:21:30")

    ce_prem: Optional[float] = None
    pe_prem: Optional[float] = None
    ce_hedge_prem: Optional[float] = None
    pe_hedge_prem: Optional[float] = None
    ce_sl_pts: Optional[float] = None
    pe_sl_pts: Optional[float] = None

    hedge_buy: Optional[str] = None
    move_to_cost: Optional[str] = None

    strategy_risk_management_id: Optional[str] = None

    @field_validator("broker")
    @classmethod
    def validate_broker(cls, v: str) -> str:
        b = broker_impl.normalize_broker(v)
        if b not in broker_impl.ALLOWED_BROKERS:
            raise ValueError(
                f"broker must be one of {sorted(broker_impl.ALLOWED_BROKERS)} "
                f"(e.g. kite, zerodha, kotak); got {v!r}"
            )
        return v


def _cleanup_dead_workers():
    """Move exited workers from active registry to history."""
    dead = [rid for rid, w in workers.items() if w["process"].poll() is not None]
    for rid in dead:
        w = workers.pop(rid)
        worker_history[rid]["status"] = "exited"
        worker_history[rid]["stopped_at"] = datetime.now().isoformat()
        _save_worker_json(rid, worker_history[rid])


def _process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _last_log_line(log_path: str) -> tuple[Optional[str], Optional[float]]:
    if not log_path or not os.path.isfile(log_path):
        return None, None
    try:
        mtime = os.path.getmtime(log_path)
        with open(log_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                return "", mtime
            chunk = min(4096, size)
            f.seek(-chunk, os.SEEK_END)
            data = f.read().decode(errors="replace")
        lines = [ln for ln in data.splitlines() if ln.strip()]
        return (lines[-1] if lines else ""), mtime
    except OSError:
        return None, None


def _worker_health_snapshot(run_id: str) -> dict:
    """Single worker health: process + optional log tail."""
    if run_id not in worker_history:
        return {"run_id": run_id, "healthy": False, "reason": "unknown_run_id"}

    h = worker_history[run_id]
    recorded = h.get("status", "unknown")
    pid = h.get("pid")
    log_file = h.get("log_file")
    last_line, log_mtime = _last_log_line(log_file) if log_file else (None, None)

    active = run_id in workers
    proc = workers.get(run_id, {}).get("process")
    poll = proc.poll() if proc is not None else None
    process_running = active and poll is None and _process_alive(pid)

    if active:
        if poll is not None:
            healthy, reason = False, "process_exited"
        elif not _process_alive(pid):
            healthy, reason = False, "pid_not_running"
        else:
            healthy, reason = True, "process_running"
    elif recorded == "running":
        healthy, reason = False, "registry_mismatch"
    elif recorded in ("killed", "exited"):
        healthy, reason = True, "stopped"
    else:
        healthy, reason = True, recorded or "idle"

    return {
        "run_id": run_id,
        "healthy": healthy,
        "reason": reason,
        "active_in_registry": active,
        "process_running": process_running,
        "pid": pid,
        "recorded_status": recorded,
        "last_log_line": last_line,
        "log_file_mtime": datetime.fromtimestamp(log_mtime).isoformat() if log_mtime else None,
    }


# ── Endpoints ──

@app.get("/health")
def health_liveness():
    """Liveness probe — no auth (for load balancers)."""
    return {"status": "ok", "service": "worker-control-api"}


@app.get("/health/workers")
def health_workers(_: str = Depends(verify_api_key)):
    """Health of all known workers (today's registry + active processes)."""
    _cleanup_dead_workers()
    items = []
    for run_id in sorted(worker_history.keys()):
        items.append(_worker_health_snapshot(run_id))
    ok = all(w["healthy"] for w in items if w.get("active_in_registry"))
    return {
        "overall": "ok" if ok else "degraded",
        "active_count": len(workers),
        "workers": items,
    }


@app.get("/health/{run_id}")
def health_one_worker(run_id: str, _: str = Depends(verify_api_key)):
    """Health for a single worker by run_id."""
    _cleanup_dead_workers()
    if run_id not in worker_history:
        raise HTTPException(status_code=404, detail=f"No worker found with run_id '{run_id}'.")
    return _worker_health_snapshot(run_id)

@app.post("/start")
def start_worker(req: StartRequest, _: str = Depends(verify_api_key)):
    _cleanup_dead_workers()

    if req.run_id in workers:
        raise HTTPException(status_code=409, detail=f"Worker with run_id '{req.run_id}' is already running. Kill it first.")

    if req.run_id in worker_history:
        prev = worker_history[req.run_id]
        raise HTTPException(
            status_code=409,
            detail=f"run_id '{req.run_id}' was already used (status: {prev['status']}, started: {prev['started_at']}). Use a unique run_id.",
        )

    log_file = os.path.join(_today_folder(), f"{req.run_id}.log")

    script = os.path.join(REPO_ROOT, "worker.py")
    if not os.path.isfile(script):
        raise HTTPException(status_code=500, detail=f"Missing worker entrypoint: {script}")

    cmd = ["python3", script, "--run_id", req.run_id, "--entry_time", req.entry_time, "--exit_time", req.exit_time, "--log_file", log_file]

    fields = req.model_dump(exclude={"run_id", "entry_time", "exit_time"})
    for key, value in fields.items():
        if value is not None:
            cmd.extend([f"--{key}", str(value)])

    process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    now = datetime.now().isoformat()
    workers[req.run_id] = {
        "process": process,
        "pid": process.pid,
        "args": req.model_dump(),
        "log_file": log_file,
        "started_at": now,
    }
    history_entry = {
        "pid": process.pid,
        "args": req.model_dump(),
        "log_file": log_file,
        "started_at": now,
        "stopped_at": None,
        "status": "running",
        "exit_trade": False,
        "exit_algo": False,
    }
    worker_history[req.run_id] = history_entry
    _save_worker_json(req.run_id, history_entry)

    return {
        "status": "started",
        "run_id": req.run_id,
        "pid": process.pid,
        "entry_time": req.entry_time,
        "exit_time": req.exit_time,
    }


@app.post("/kill/{run_id}")
def kill_worker(run_id: str, _: str = Depends(verify_api_key)):
    _cleanup_dead_workers()

    if run_id not in workers:
        raise HTTPException(status_code=404, detail=f"No running worker with run_id '{run_id}'.")

    w = workers.pop(run_id)
    pid = w["pid"]
    try:
        os.kill(pid, signal.SIGTERM)
        w["process"].wait(timeout=5)
    except ProcessLookupError:
        pass
    except subprocess.TimeoutExpired:
        os.kill(pid, signal.SIGKILL)

    if run_id in worker_history:
        worker_history[run_id]["status"] = "killed"
        worker_history[run_id]["stopped_at"] = datetime.now().isoformat()
        _save_worker_json(run_id, worker_history[run_id])

    return {"status": "killed", "run_id": run_id, "pid": pid}


@app.post("/kill-all")
def kill_all_workers(_: str = Depends(verify_api_key)):
    killed = []
    for run_id in list(workers.keys()):
        w = workers.pop(run_id)
        pid = w["pid"]
        try:
            os.kill(pid, signal.SIGTERM)
            w["process"].wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        killed.append({"run_id": run_id, "pid": pid})
        if run_id in worker_history:
            worker_history[run_id]["status"] = "killed"
            worker_history[run_id]["stopped_at"] = datetime.now().isoformat()
            _save_worker_json(run_id, worker_history[run_id])

    return {"status": "killed_all", "count": len(killed), "workers": killed}


@app.post("/exit_trade/{run_id}")
def exit_trade(run_id: str, _: str = Depends(verify_api_key)):
    """Signal worker to close all positions and exit gracefully."""
    _cleanup_dead_workers()
    if run_id not in workers:
        raise HTTPException(status_code=404, detail=f"No running worker with run_id '{run_id}'.")
    _set_exit_flag_in_worker_json(run_id, "exit_trade")
    return {"status": "signal_sent", "run_id": run_id, "command": "exit_trade", "pid": workers[run_id]["pid"]}


@app.post("/exit_algo/{run_id}")
def exit_algo(run_id: str, _: str = Depends(verify_api_key)):
    """Signal worker to stop immediately without making any trades/changes."""
    _cleanup_dead_workers()
    if run_id not in workers:
        raise HTTPException(status_code=404, detail=f"No running worker with run_id '{run_id}'.")
    _set_exit_flag_in_worker_json(run_id, "exit_algo")
    return {"status": "signal_sent", "run_id": run_id, "command": "exit_algo", "pid": workers[run_id]["pid"]}


@app.get("/log/{run_id}")
def read_log(run_id: str, tail: int = 50, _: str = Depends(verify_api_key)):
    log_file = None
    if run_id in worker_history:
        log_file = worker_history[run_id].get("log_file")
    if not log_file or not os.path.exists(log_file):
        raise HTTPException(status_code=404, detail=f"No log file for run_id '{run_id}'.")

    with open(log_file) as f:
        all_lines = f.readlines()

    lines = [line.rstrip("\n") for line in all_lines[-tail:]]
    return {"run_id": run_id, "lines": lines, "total_lines": len(all_lines), "showing_last": len(lines)}


@app.get("/workers")
def list_workers(status: Optional[str] = Query(None, description="Filter: running, killed, exited"), _: str = Depends(verify_api_key)):
    """List all workers. Optional ?status=running|killed|exited to filter."""
    _cleanup_dead_workers()

    # Update status for currently running ones
    for rid in workers:
        if rid in worker_history:
            worker_history[rid]["status"] = "running"

    result = []
    for run_id, w in worker_history.items():
        if status and w["status"] != status:
            continue
        result.append({
            "run_id": run_id,
            "pid": w["pid"],
            "status": w["status"],
            "started_at": w["started_at"],
            "stopped_at": w["stopped_at"],
            "args": w["args"],
        })

    return {"count": len(result), "workers": result}


@app.get("/worker/{run_id}")
def get_worker(run_id: str, _: str = Depends(verify_api_key)):
    """Get details for a specific worker by run_id."""
    _cleanup_dead_workers()

    if run_id not in worker_history:
        raise HTTPException(status_code=404, detail=f"No worker found with run_id '{run_id}'.")

    w = worker_history[run_id]
    if run_id in workers:
        w["status"] = "running"

    return {
        "run_id": run_id,
        "pid": w["pid"],
        "status": w["status"],
        "started_at": w["started_at"],
        "stopped_at": w["stopped_at"],
        "args": w["args"],
    }


@app.get("/status/{run_id}")
def get_run_id_status(run_id: str, _: str = Depends(verify_api_key)):
    """Return ``{run_id}_status.json`` (PnL, CE/PE legs, qty, etc.) written by IDP next to the log file."""
    _cleanup_dead_workers()
    path = _resolve_run_status_json_path(run_id)
    if not path:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No {{run_id}}_status.json for {run_id!r}. "
                "IDP writes it beside the worker log; try today's app/data/<ddMmmYYYY>/ or the path from worker history."
            ),
        )
    try:
        with open(path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        raise HTTPException(status_code=500, detail=f"Cannot read status file: {e}") from e
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="Status file must be a JSON object")
    out = dict(data)
    out["_status_file"] = path
    return out


@app.get("/status")
def overall_status(_: str = Depends(verify_api_key)):
    _cleanup_dead_workers()
    running = len(workers)
    total = len(worker_history)
    return {
        "active_workers": running,
        "total_workers": total,
        "workers": {rid: {"pid": w["pid"], "status": "running", "started_at": w["started_at"]} for rid, w in workers.items()},
    }


@app.get("/broker-workers")
def list_broker_workers(_: str = Depends(verify_api_key)):
    """Allowed broker values for POST /start (common worker.py)."""
    return {"brokers": sorted(broker_impl.ALLOWED_BROKERS)}


# ── Broker snapshots (orders / positions / margins) — per client, not per run_id ──


def _get_snapshot_creds_from_args(args: dict) -> dict:
    fn = getattr(broker_impl, "get_snapshot_creds", None)
    if fn is None or not callable(fn):
        raise HTTPException(status_code=500, detail="broker_impl has no get_snapshot_creds()")
    try:
        return fn(args)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def _resolve_worker_args_for_client(client_db_id: int, client_id: str) -> dict:
    """Latest worker_history args matching client_db_id + client_id (Kite creds live there)."""
    best_args: Optional[dict] = None
    best_started = ""
    for _rid, entry in worker_history.items():
        args = entry.get("args") or {}
        adb = args.get("client_db_id")
        aci = args.get("client_id")
        if adb is None or aci is None:
            continue
        if str(adb) != str(client_db_id) or str(aci) != str(client_id):
            continue
        started = entry.get("started_at") or ""
        if started >= best_started:
            best_started = started
            best_args = args
    if not best_args:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No stored worker credentials for client_db_id={client_db_id} client_id={client_id}. "
                "POST /start once for this client so api_key and access_token are saved."
            ),
        )
    return best_args


def _resolve_snapshot_context_for_client(client_db_id: int, client_id: str) -> tuple[str, dict, dict]:
    """Resolve storage tag, creds dict, and identity from latest /start args for this client."""
    cid = str(client_id).strip()
    adb = int(client_db_id)
    args = _resolve_worker_args_for_client(adb, cid)
    br = broker_impl.resolve_broker_from_args(args)
    if not br or br not in broker_impl.ALLOWED_BROKERS:
        raise HTTPException(
            status_code=400,
            detail="Stored worker args missing valid broker. POST /start with broker kite, zerodha, or kotak.",
        )
    creds = _get_snapshot_creds_from_args(args)
    tag = _safe_client_file_tag(adb, cid)
    identity = {"client_db_id": adb, "client_id": cid, "broker": br}
    return tag, creds, identity


# Min seconds between broker fetches per (snapshot_kind, storage tag) — BROKER_SNAPSHOT_MIN_INTERVAL_SEC in .env (0 = off).
_broker_snapshot_last_at: dict[tuple[str, str], float] = {}


def _broker_snapshot_throttle(snapshot_kind: str, tag: str, identity: dict) -> Optional[dict]:
    key = (snapshot_kind, tag)
    raw = os.getenv("BROKER_SNAPSHOT_MIN_INTERVAL_SEC", "5")
    try:
        interval = float(raw)
    except ValueError:
        interval = 5.0
    if interval <= 0:
        return None
    now = time.time()
    last = _broker_snapshot_last_at.get(key, 0.0)
    if now - last < interval:
        out: dict = {
            "status": "skipped",
            "reason": "min_interval",
            "snapshot": snapshot_kind,
            "min_interval_sec": interval,
            "retry_after_sec": round(interval - (now - last), 3),
        }
        for k in ("broker", "client_db_id", "client_id"):
            if identity.get(k) is not None:
                out[k] = identity[k]
        return out
    _broker_snapshot_last_at[key] = now
    return None


def _identity_error_payload(identity: dict) -> dict:
    return {k: v for k, v in identity.items() if v is not None}


# ── Orders Endpoints ──

@app.post("/orders/update")
def update_orders(
    client_db_id: int = Query(..., description="Client DB id from /start."),
    client_id: str = Query(..., max_length=64),
    _: str = Depends(verify_api_key),
):
    """Fetch orders via broker selected from stored broker on last /start for this client."""
    tag, creds, identity = _resolve_snapshot_context_for_client(client_db_id, client_id)
    skip = _broker_snapshot_throttle("orders", tag, identity)
    if skip is not None:
        return skip
    res_client_id, res_client_db_id = creds.get("client_id"), creds.get("client_db_id")
    fetch = getattr(broker_impl, "fetch_orders_json", None)
    if not callable(fetch):
        raise HTTPException(status_code=500, detail="broker_impl has no fetch_orders_json()")
    try:
        resp_json = fetch(creds)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Broker API request failed: {str(e)}") from e
    except ValueError as e:
        raise HTTPException(status_code=502, detail=f"Broker API returned invalid JSON: {e}") from e
    if not isinstance(resp_json, dict):
        raise HTTPException(status_code=502, detail="Broker module returned non-object JSON for orders.")
    resp_status = resp_json.get("status")

    if resp_status != "success":
        err = _identity_error_payload(identity)
        err.update(
            {
                "status": "error",
                "broker_status": resp_status,
                "error_type": resp_json.get("error_type"),
                "message": resp_json.get("message", "Check token or reconnect."),
            }
        )
        return err

    res = resp_json.get("data") or []
    if isinstance(res, dict):
        res = [res]
    elif not isinstance(res, list):
        res = []

    for o in res:
        if isinstance(o, dict):
            o["ClientName"] = res_client_id

    doc = {
        "type": resp_json.get("status"),
        "code": resp_json.get("error_type"),
        "description": resp_json.get("message"),
        "last_update": int(time.time()),
        "result": res,
    }

    grouped = defaultdict(list)
    for row in res:
        strategy = row.get("tag") or "NO_STRATEGY"
        grouped[strategy].append(row)

    grouped_orders = {}
    for strategy, rows in grouped.items():
        grouped_orders[strategy] = {
            "type": doc["type"],
            "code": doc["code"],
            "description": doc["description"],
            "last_update": doc["last_update"],
            "result": rows,
        }

    orders = resp_json.get("data", [])
    summary = {
        "total_orders": len(orders),
        "pending_orders": sum(1 for o in orders if o.get("status") in ("OPEN", "TRIGGER PENDING", "PENDING")),
        "rejected_orders": sum(1 for o in orders if o.get("status") == "REJECTED"),
        "cancelled_orders": sum(1 for o in orders if o.get("status") == "CANCELLED"),
        "buy_order_qty": sum(int(o.get("quantity", 0)) for o in orders if o.get("transaction_type") == "BUY"),
        "sell_order_qty": sum(int(o.get("quantity", 0)) for o in orders if o.get("transaction_type") == "SELL"),
    }

    full_doc = {
        **_identity_error_payload(identity),
        "client_id": res_client_id,
        "client_db_id": res_client_db_id,
        "last_update": doc["last_update"],
        "summary": summary,
        "orders": doc,
        "grouped_by_strategy": grouped_orders,
    }

    _save_broker_snapshot(tag, "orders", full_doc)

    return full_doc


@app.get("/orders")
def get_orders(
    client_db_id: int = Query(...),
    client_id: str = Query(..., max_length=64),
    _: str = Depends(verify_api_key),
):
    """Read last stored orders snapshot for this client."""
    tag, _, identity = _resolve_snapshot_context_for_client(client_db_id, client_id)
    orders_file = _broker_snapshot_path(tag, "orders")
    if not os.path.exists(orders_file):
        raise HTTPException(
            status_code=404,
            detail=f"No orders snapshot for {_identity_error_payload(identity)}. Call POST /orders/update first.",
        )

    with open(orders_file) as f:
        data = json.load(f)
    return data


# ── Positions & margins (broker portfolio) ──


@app.post("/positions/update")
def update_positions(
    client_db_id: int = Query(...),
    client_id: str = Query(..., max_length=64),
    _: str = Depends(verify_api_key),
):
    """Fetch net positions via broker module (Kite-style `data.net` list in response JSON)."""
    tag, creds, identity = _resolve_snapshot_context_for_client(client_db_id, client_id)
    skip = _broker_snapshot_throttle("positions", tag, identity)
    if skip is not None:
        return skip
    res_client_id, res_client_db_id = creds.get("client_id"), creds.get("client_db_id")
    fetch = getattr(broker_impl, "fetch_positions_json", None)
    if not callable(fetch):
        raise HTTPException(status_code=500, detail="broker_impl has no fetch_positions_json()")
    try:
        resp_json = fetch(creds)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Broker API request failed: {str(e)}") from e
    except ValueError as e:
        raise HTTPException(status_code=502, detail=f"Broker API returned invalid JSON: {e}") from e
    if not isinstance(resp_json, dict):
        raise HTTPException(status_code=502, detail="Broker module returned non-object JSON for positions.")

    resp_status = resp_json.get("status")
    if resp_status != "success":
        err = _identity_error_payload(identity)
        err.update(
            {
                "status": "error",
                "broker_status": resp_status,
                "error_type": resp_json.get("error_type"),
                "message": resp_json.get("message", "Check token or reconnect."),
            }
        )
        return err

    data = resp_json.get("data") or {}
    if isinstance(data, dict):
        res = data.get("net") or []
    else:
        res = []
    if not isinstance(res, list):
        res = []

    for row in res:
        if isinstance(row, dict):
            row["ClientName"] = res_client_id

    doc = {
        "type": resp_json.get("status"),
        "code": resp_json.get("error_type"),
        "description": resp_json.get("message"),
        "last_update": int(time.time()),
        "result": res,
    }

    full_doc = {
        **_identity_error_payload(identity),
        "client_id": res_client_id,
        "client_db_id": res_client_db_id,
        "last_update": doc["last_update"],
        "positions": doc,
    }
    _save_broker_snapshot(tag, "positions", full_doc)
    return full_doc


@app.get("/positions")
def get_positions(
    client_db_id: int = Query(...),
    client_id: str = Query(..., max_length=64),
    _: str = Depends(verify_api_key),
):
    tag, _, identity = _resolve_snapshot_context_for_client(client_db_id, client_id)
    path = _broker_snapshot_path(tag, "positions")
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail=f"No positions snapshot for {_identity_error_payload(identity)}. Call POST /positions/update first.",
        )
    with open(path) as f:
        return json.load(f)


@app.post("/margins/update")
def update_margins(
    client_db_id: int = Query(...),
    client_id: str = Query(..., max_length=64),
    _: str = Depends(verify_api_key),
):
    tag, creds, identity = _resolve_snapshot_context_for_client(client_db_id, client_id)
    skip = _broker_snapshot_throttle("margins", tag, identity)
    if skip is not None:
        return skip
    res_client_id, res_client_db_id = creds.get("client_id"), creds.get("client_db_id")
    fetch = getattr(broker_impl, "fetch_margins_json", None)
    if not callable(fetch):
        raise HTTPException(status_code=500, detail="broker_impl has no fetch_margins_json()")
    try:
        resp_json = fetch(creds)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Broker API request failed: {str(e)}") from e
    except ValueError as e:
        raise HTTPException(status_code=502, detail=f"Broker API returned invalid JSON: {e}") from e
    if not isinstance(resp_json, dict):
        raise HTTPException(status_code=502, detail="Broker module returned non-object JSON for margins.")

    resp_status = resp_json.get("status")
    if resp_status != "success":
        err = _identity_error_payload(identity)
        err.update(
            {
                "status": "error",
                "broker_status": resp_status,
                "error_type": resp_json.get("error_type"),
                "message": resp_json.get("message", "Check token or reconnect."),
            }
        )
        return err

    res = resp_json.get("data") or []
    if isinstance(res, dict):
        res = [res]
    elif not isinstance(res, list):
        res = []

    doc = {
        "type": resp_json.get("status"),
        "code": resp_json.get("error_type"),
        "description": resp_json.get("message"),
        "last_update": int(time.time()),
        "result": res,
    }

    full_doc = {
        **_identity_error_payload(identity),
        "client_id": res_client_id,
        "client_db_id": res_client_db_id,
        "last_update": doc["last_update"],
        "margins": doc,
    }
    _save_broker_snapshot(tag, "margins", full_doc)
    return full_doc


@app.get("/margins")
def get_margins(
    client_db_id: int = Query(...),
    client_id: str = Query(..., max_length=64),
    _: str = Depends(verify_api_key),
):
    tag, _, identity = _resolve_snapshot_context_for_client(client_db_id, client_id)
    path = _broker_snapshot_path(tag, "margins")
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail=f"No margins snapshot for {_identity_error_payload(identity)}. Call POST /margins/update first.",
        )
    with open(path) as f:
        return json.load(f)


# ── Deploy Endpoint ──

@app.post("/deploy")
def deploy(_: str = Depends(verify_api_key)):
    """Pull latest code from GitHub, install deps, and restart the service."""
    results = {}

    try:
        git_pull = subprocess.run(
            ["git", "pull", "origin", "main"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=30,
        )
        results["git_pull"] = {
            "stdout": git_pull.stdout.strip(),
            "stderr": git_pull.stderr.strip(),
            "returncode": git_pull.returncode,
        }
        if git_pull.returncode != 0:
            return {"status": "error", "step": "git_pull", "detail": results}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="git pull timed out.")

    try:
        pip_install = subprocess.run(
            [os.path.join(REPO_ROOT, "venv", "bin", "pip"), "install", "-q", "-r", "requirements.txt"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=60,
        )
        results["pip_install"] = {
            "stdout": pip_install.stdout.strip(),
            "stderr": pip_install.stderr.strip(),
            "returncode": pip_install.returncode,
        }
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="pip install timed out.")

    try:
        git_log = subprocess.run(
            ["git", "log", "--oneline", "-5"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=5,
        )
        results["recent_commits"] = git_log.stdout.strip().split("\n")
    except Exception:
        results["recent_commits"] = []

    subprocess.Popen(
        ["sudo", "systemctl", "restart", "fastapi"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    results["restart"] = "scheduled"

    return {"status": "deployed", "detail": results}
