"""
Broker-specific snapshot creds, REST fetch helpers, and exit hooks for the common worker.
Canonical broker keys: kite, kotak (aliases: zerodha -> kite).
"""

from __future__ import annotations

from typing import Any

import requests

ALLOWED_BROKERS = frozenset({"kite", "kotak"})


def normalize_broker(raw: str) -> str:
    s = (raw or "").strip().lower()
    if s in ("kite", "zerodha"):
        return "kite"
    if s == "kotak":
        return "kotak"
    return s


def resolve_broker_from_args(args: dict) -> str:
    """Resolve canonical broker from args.broker or legacy args.worker_kind."""
    b = args.get("broker")
    if b is not None and str(b).strip():
        return normalize_broker(str(b))
    wk = args.get("worker_kind")
    if wk == "Kite_Worker":
        return "kite"
    if wk == "Kotak_Worker":
        return "kotak"
    return ""


def get_snapshot_creds(args: dict) -> dict:
    broker = resolve_broker_from_args(args)
    if broker not in ALLOWED_BROKERS:
        raise ValueError(
            f"broker must be one of {sorted(ALLOWED_BROKERS)} (e.g. kite, zerodha, kotak); "
            f"got broker={args.get('broker')!r} worker_kind={args.get('worker_kind')!r}"
        )
    if broker == "kite":
        return _kite_snapshot_creds(args)
    return _kotak_snapshot_creds(args)


def _kite_snapshot_creds(args: dict) -> dict:
    api_key = args.get("api_key")
    access_token = args.get("access_token")
    if not api_key or not access_token:
        raise ValueError("kite broker requires api_key and access_token in worker args")
    api_base = (args.get("root_url") or "https://api.kite.trade").rstrip("/") or "https://api.kite.trade"
    return {
        "broker": "kite",
        "api_key": api_key,
        "access_token": access_token,
        "client_id": args.get("client_id"),
        "client_db_id": args.get("client_db_id"),
        "api_base": api_base,
        "path_orders": "/orders",
        "path_positions": "/portfolio/positions",
        "path_margins": "/user/margins",
        "auth_style": "kite_token",
    }


def _kotak_snapshot_creds(args: dict) -> dict:
    api_key = args.get("api_key")
    access_token = args.get("access_token")
    if not api_key or not access_token:
        raise ValueError("kotak broker requires api_key and access_token in worker args")
    api_base = (args.get("root_url") or "").rstrip("/")
    if not api_base:
        raise ValueError("kotak broker requires root_url in worker args (REST base URL)")
    return {
        "broker": "kotak",
        "api_key": api_key,
        "access_token": access_token,
        "client_id": args.get("client_id"),
        "client_db_id": args.get("client_db_id"),
        "api_base": api_base,
        "path_orders": "/orders",
        "path_positions": "/portfolio/positions",
        "path_margins": "/user/margins",
        "auth_style": "kite_token",
    }


def _kite_headers(creds: dict) -> dict:
    return {"Authorization": f"token {creds['api_key']}:{creds['access_token']}"}


def fetch_orders_json(creds: dict) -> dict:
    b = creds.get("broker")
    if b == "kotak":
        return {
            "status": "error",
            "error_type": "NotImplemented",
            "message": "kotak fetch_orders_json: implement Kotak REST call and response mapping.",
        }
    p = creds.get("path_orders") or "/orders"
    if not str(p).startswith("/"):
        p = "/" + str(p)
    url = creds["api_base"].rstrip("/") + p
    r = requests.get(url, headers=_kite_headers(creds), timeout=15)
    return r.json()


def fetch_positions_json(creds: dict) -> dict:
    b = creds.get("broker")
    if b == "kotak":
        return {
            "status": "error",
            "error_type": "NotImplemented",
            "message": "kotak fetch_positions_json: implement Kotak REST call and response mapping.",
        }
    p = creds.get("path_positions") or "/portfolio/positions"
    if not str(p).startswith("/"):
        p = "/" + str(p)
    url = creds["api_base"].rstrip("/") + p
    r = requests.get(url, headers=_kite_headers(creds), timeout=15)
    return r.json()


def fetch_margins_json(creds: dict) -> dict:
    b = creds.get("broker")
    if b == "kotak":
        return {
            "status": "error",
            "error_type": "NotImplemented",
            "message": "kotak fetch_margins_json: implement Kotak REST call and response mapping.",
        }
    p = creds.get("path_margins") or "/user/margins"
    if not str(p).startswith("/"):
        p = "/" + str(p)
    url = creds["api_base"].rstrip("/") + p
    r = requests.get(url, headers=_kite_headers(creds), timeout=15)
    return r.json()


class KiteHooks:
    def close_positions(self, log_file: str, run_id: str, args: Any) -> None:
        from worker_runtime import log

        log(log_file, run_id, "EXIT_TRADE | Closing all positions (Kite placeholder)...")
        log(log_file, run_id, "EXIT_TRADE | All positions closed.")


class KotakHooks:
    def close_positions(self, log_file: str, run_id: str, args: Any) -> None:
        from worker_runtime import log

        log(log_file, run_id, "EXIT_TRADE | Kotak close_positions placeholder — implement broker exits.")


def hooks_for_broker(broker: str):
    if broker == "kotak":
        return KotakHooks()
    return KiteHooks()
