"""
Shared trading worker lifecycle: signals, entry/exit window, heartbeat, optional orders snapshot.
Broker-specific behavior is passed via hooks (e.g. close_positions).
"""

from __future__ import annotations

import json
import os
import signal
import time
import urllib.parse
from datetime import datetime
from typing import Any, Protocol

import requests
from dotenv import load_dotenv

HEARTBEAT_INTERVAL = 5 * 60  # 5 minutes
SIGNAL_CHECK_INTERVAL = 5

running = True


def handle_signal(signum, frame):
    global running
    running = False


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def log(log_file: str, run_id: str, message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} | {run_id} | {message}"
    print(line, flush=True)
    with open(log_file, "a") as f:
        f.write(line + "\n")


def _exit_flag_truthy(v: Any) -> bool:
    if v is True:
        return True
    if isinstance(v, (int, float)) and v == 1:
        return True
    if isinstance(v, str) and v.strip().lower() in ("1", "true", "yes"):
        return True
    return False


def consume_exit_from_worker_json(worker_json_path: str) -> str | None:
    """Read `{run_id}.json`; if ``exit_algo`` or ``exit_trade`` is set, clear it and return the command.

    ``exit_algo`` is checked first (hard stop). Writes the file back with flags reset to false.
    """
    if not os.path.isfile(worker_json_path):
        return None
    try:
        with open(worker_json_path, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(data, dict):
        return None
    cmd: str | None = None
    if _exit_flag_truthy(data.get("exit_algo")):
        cmd = "exit_algo"
        data["exit_algo"] = False
        data["exit_trade"] = False
    elif _exit_flag_truthy(data.get("exit_trade")):
        cmd = "exit_trade"
        data["exit_trade"] = False
    if cmd is None:
        return None
    try:
        with open(worker_json_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    except OSError:
        return cmd
    return cmd


def today_at(time_str: str) -> datetime:
    t = datetime.strptime(time_str, "%H:%M:%S").time()
    return datetime.combine(datetime.today().date(), t)


def maybe_orders_snapshot(log_file: str, run_id: str, args: Any) -> None:
    """POST /orders/update using client_db_id + client_id; server resolves broker from worker_history."""
    v = str(os.environ.get("WORKER_ORDERS_SNAPSHOT", "1")).strip().lower()
    if v in ("0", "false", "no", "off"):
        return
    if getattr(args, "client_db_id", None) is None or not getattr(args, "client_id", None):
        return
    api_key = os.environ.get("API_KEY", "").strip()
    if not api_key:
        return
    base = os.environ.get("CONTROL_API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
    qs = urllib.parse.urlencode({"client_db_id": args.client_db_id, "client_id": args.client_id})
    try:
        url = f"{base}/orders/update?{qs}"
        r = requests.post(url, headers={"X-API-Key": api_key}, timeout=15)
        if r.status_code != 200:
            log(log_file, run_id, f"ORDERS_SNAPSHOT | http_status={r.status_code} body={r.text[:200]}")
            return
        data = r.json()
        if data.get("status") == "skipped":
            return
        if data.get("status") == "error":
            log(log_file, run_id, f"ORDERS_SNAPSHOT | broker_error={data.get('message', data)}")
    except Exception as e:
        log(log_file, run_id, f"ORDERS_SNAPSHOT | exception={e}")


class BrokerProcessHooks(Protocol):
    def close_positions(self, log_file: str, run_id: str, args: Any) -> None: ...


def add_common_worker_cli(parser) -> None:
    """Register argparse flags matching StartRequest / POST /start."""
    parser.add_argument("--run_id", type=str, required=True)
    parser.add_argument("--entry_time", type=str, required=True, help="e.g. 09:21:30")
    parser.add_argument("--exit_time", type=str, required=True, help="e.g. 15:21:30")
    parser.add_argument("--log_file", type=str, required=True)

    parser.add_argument("--broker", type=str, required=True, help="kite (zerodha) or kotak")

    parser.add_argument("--client_db_id", type=int)
    parser.add_argument("--client_id", type=str)
    parser.add_argument("--strategy_id", type=str)
    parser.add_argument("--tranche", type=str)

    parser.add_argument("--static_ip", type=str)

    parser.add_argument("--api_key", type=str)
    parser.add_argument("--access_token", type=str)
    parser.add_argument("--root_url", type=str)

    parser.add_argument("--instrument", type=str)
    parser.add_argument("--is_investor_client", type=str)

    parser.add_argument("--no_of_lots", type=int)

    parser.add_argument("--ce_prem", type=float)
    parser.add_argument("--pe_prem", type=float)
    parser.add_argument("--ce_hedge_prem", type=float)
    parser.add_argument("--pe_hedge_prem", type=float)
    parser.add_argument("--ce_sl_pts", type=float)
    parser.add_argument("--pe_sl_pts", type=float)

    parser.add_argument("--hedge_buy", type=str)
    parser.add_argument("--move_to_cost", type=str)
    parser.add_argument("--strategy_risk_management_id", type=str)


def run(args: Any, hooks: BrokerProcessHooks) -> None:
    """Main wait / heartbeat / exit loop."""
    global running

    run_id = args.run_id
    log_file = args.log_file
    entry_dt = today_at(args.entry_time)
    exit_dt = today_at(args.exit_time)

    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    worker_json_path = os.path.join(os.path.dirname(os.path.abspath(log_file)), f"{run_id}.json")

    params = {k: v for k, v in vars(args).items() if v is not None and k not in ("log_file",)}
    log(log_file, run_id, f"STARTED | params={params}")

    exit_reason = None

    while running:
        now = datetime.now()
        if now >= entry_dt:
            break

        cmd = consume_exit_from_worker_json(worker_json_path)
        if cmd:
            exit_reason = cmd
            log(log_file, run_id, f"SIGNAL received during wait: {cmd}")
            break

        wait = min(HEARTBEAT_INTERVAL, (entry_dt - now).total_seconds() + 1)
        log(log_file, run_id, f"WAITING | entry_time={args.entry_time} (in {int((entry_dt - now).total_seconds())}s)")
        slept = 0
        while slept < wait and running:
            time.sleep(min(SIGNAL_CHECK_INTERVAL, wait - slept))
            slept += SIGNAL_CHECK_INTERVAL
            cmd = consume_exit_from_worker_json(worker_json_path)
            if cmd:
                exit_reason = cmd
                log(log_file, run_id, f"SIGNAL received during wait: {cmd}")
                running = False
            maybe_orders_snapshot(log_file, run_id, args)

    if running and not exit_reason:
        log(log_file, run_id, "ACTIVE | trading window open")

    while running and not exit_reason:
        now = datetime.now()
        if now >= exit_dt:
            log(log_file, run_id, f"EXIT_TIME reached ({args.exit_time}) — shutting down")
            break

        remaining = int((exit_dt - now).total_seconds())
        log(
            log_file,
            run_id,
            f"HEARTBEAT | now={now.strftime('%H:%M:%S')} | remaining={remaining}s | lots={args.no_of_lots} | instrument={args.instrument}",
        )

        slept = 0
        sleep_total = min(HEARTBEAT_INTERVAL, remaining + 1)
        while slept < sleep_total and running:
            time.sleep(min(SIGNAL_CHECK_INTERVAL, sleep_total - slept))
            slept += SIGNAL_CHECK_INTERVAL
            cmd = consume_exit_from_worker_json(worker_json_path)
            if cmd:
                exit_reason = cmd
                log(log_file, run_id, f"SIGNAL received: {cmd}")
                break
            maybe_orders_snapshot(log_file, run_id, args)

    if exit_reason == "exit_trade":
        hooks.close_positions(log_file, run_id, args)
        log(log_file, run_id, "EXIT_TRADE | Graceful shutdown complete")
    elif exit_reason == "exit_algo":
        log(log_file, run_id, "EXIT_ALGO | Stopped without changes")
    elif not running:
        log(log_file, run_id, "KILLED | received SIGTERM")

    log(log_file, run_id, "STOPPED")


def load_worker_dotenv() -> None:
    root = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(root, ".env"))
