import os
import json
import time
import threading
import sys
from datetime import datetime, timedelta
import logging

try:
    import redis
except ModuleNotFoundError:
    sys.stderr.write(
        "Missing package 'redis'. You ran the system Python; use the project venv:\n"
        "  ./venv/bin/python IDP.py ...\n"
        "Or: source venv/bin/activate && python IDP.py ...\n"
    )
    raise SystemExit(1) from None

import worker_runtime as wr


def _truthy(val) -> bool:
    if val is None:
        return False
    return str(val).strip().lower() in ("1", "true", "yes", "y")


def setup_idp_logging(log_file: str, run_id: str) -> None:
    """File + stderr; no Redis logging."""
    path = os.path.abspath(log_file)
    parent = os.path.dirname(path)
    try:
        os.makedirs(parent, exist_ok=True)
    except OSError as e:
        raise SystemExit(
            f"Cannot create log directory {parent!r}: {e}\n"
            f"Use a writable path (not a placeholder). Examples:\n"
            f"  --log_file ./logs/{run_id}.log\n"
            f"  --log_file /tmp/{run_id}.log\n"
            f"  --log_file $HOME/fastapi-worker/app/data/{run_id}.log\n"
        ) from e
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter(f"%(asctime)s | {run_id} | %(levelname)s | %(message)s")
    fh = logging.FileHandler(path)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)


def rss_mb():
    # current RSS from /proc/self/statm (more useful than ru_maxrss which is peak)
    with open("/proc/self/statm", "r") as f:
        _size_pages, rss_pages = map(int, f.readline().split()[:2])
    return (rss_pages * os.sysconf("SC_PAGE_SIZE")) / (1024 * 1024)
    
POOL = redis.ConnectionPool(
    host="163.223.52.201",
    port=6379,
    password="UDAIpur2000",
    db=0,
    decode_responses=True,
    max_connections=5,        # tune as needed
    socket_timeout=5,          # fail fast on bad links
    retry_on_timeout=True,
    socket_keepalive=True,
    health_check_interval=30,  # background PINGs
)

# Thread-safe client that borrows connections from POOL
r = redis.Redis(connection_pool=POOL)

import argparse

# ---- argparse (aligned with POST /start / worker.py) ----
parser = argparse.ArgumentParser(description="IDP algo worker (Redis read-only: LTP + master; status JSON file)")
wr.add_common_worker_cli(parser)
parser.add_argument("--strategy_instance_id", type=str, help="For Node App purpose")
args = parser.parse_args()

wr.load_worker_dotenv()
setup_idp_logging(args.log_file, args.run_id)

client_db_id = args.client_db_id
client_id = args.client_id
strategy_id = args.run_id
api_key = args.api_key
access_token = args.access_token
static_ip = args.static_ip

inst = args.instrument
isinvestor = args.is_investor_client

no_of_lots = args.no_of_lots
entry_time = args.entry_time
exit_time = args.exit_time

for _name, _val in (
    ("ce_prem", args.ce_prem),
    ("pe_prem", args.pe_prem),
    ("ce_hedge_prem", args.ce_hedge_prem),
    ("pe_hedge_prem", args.pe_hedge_prem),
    ("ce_sl_pts", args.ce_sl_pts),
    ("pe_sl_pts", args.pe_sl_pts),
    ("instrument", inst),
    ("no_of_lots", no_of_lots),
):
    if _val is None:
        logging.error("missing required arg: %s", _name)
        sys.exit(1)

CE_Premium = float(args.ce_prem)
PE_Premium = float(args.pe_prem)
CE_Hedge_Premium = float(args.ce_hedge_prem)
PE_Hedge_Premium = float(args.pe_hedge_prem)

hedge_buy_today = 1 if _truthy(args.hedge_buy) else 0
Move_SL_to_cost = 1 if _truthy(args.move_to_cost) else 0

CE_SL_Pts = float(args.ce_sl_pts)
PE_SL_Pts = float(args.pe_sl_pts)

print(
    client_id,
    strategy_id,
    access_token,
    api_key,
    CE_Premium,
    PE_Premium,
    CE_SL_Pts,
    PE_SL_Pts,
    CE_Hedge_Premium,
    PE_Hedge_Premium,
    hedge_buy_today,
    Move_SL_to_cost,
    static_ip,
)

log_file = args.log_file
run_id = args.run_id
worker_json_path = os.path.join(os.path.dirname(os.path.abspath(log_file)), f"{run_id}.json")
if not os.path.isfile(worker_json_path):
    os.makedirs(os.path.dirname(worker_json_path), exist_ok=True)
    with open(worker_json_path, "w") as f:
        json.dump({"run_id": run_id, "exit_trade": False, "exit_algo": False}, f, indent=2)

params = {k: v for k, v in vars(args).items() if v is not None and k != "log_file"}
wr.log(log_file, run_id, f"STARTED | params={params}")

logging.info("Started strategy execution")

# exit_trade / exit_algo: booleans in `{run_id}.json` next to log_file (POST /exit_* sets them).
exittradenow = 0
exit_algo_now = 0

if inst=='NIFTY':
    exch = 'NSE'
    exch_segment='NSEFO'
    lot_size = 65
    freeze_limit = 1800
    step = 50
    strike_range = 2400
    Trigger_Pts_diff = 1

if inst=='SENSEX':
    exch = 'BSE'
    exch_segment='BSEFO'
    lot_size = 20
    freeze_limit = 1000
    step = 100
    strike_range = 2400
    Trigger_Pts_diff = 3


SL_Jump_Threshold = 3
Limit_Order_Delay = 3
Hedge_close_delay = 3
Hedge_Place_Delay = 10


qty = no_of_lots * lot_size
ATM = 0
strikes = []

logging.info(f"Unique Session ID: {strategy_id}")


first_order   = True
hedge_placed  = False
limit_order   = False

# Flags for SL legs
ce_sl_hit    = False
pe_sl_hit    = False
ce_sl_jumped = False
pe_sl_jumped = False
ce_mkt_rejected = False
pe_mkt_rejected = False

# To store actual filled average prices
ce_exit_price = None
pe_exit_price = None

# Hedge fill prices (not used for SL but logged)
ce_hedge_avg_price = None
pe_hedge_avg_price = None
ce_hedge_placed=False
pe_hedge_placed=False
# These will be populated by the threads:
ce_inst = None
pe_inst = None
ce_token = None 
pe_token = None
ce_ltp_entry = None  
pe_ltp_entry = None  
ce_sl_order_id = None
pe_sl_order_id = None

# Hedge instruments & order ids
ce_hedge_inst = None
pe_hedge_inst = None
ce_hedge_order_id = None
pe_hedge_order_id = None
ce_hedge_token = None
pe_hedge_token = None
ce_hedge_closed = False
pe_hedge_closed = False
ce_ltp = None
pe_ltp = None
pnl=0
min_pnl=9999
max_pnl=-9999
# pnl_hist=[]

# Must not exceed freeze limit
if (no_of_lots * lot_size) > freeze_limit:
    logging.warning(f"Qty greater than freeze limit")
    sys.exit()


def _run_status_json_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(log_file)), f"{run_id}_status.json")


def publish_idp_status(core: dict) -> None:
    """Write ``{run_id}_status.json`` next to the log (read by GET /status/{run_id})."""
    g = globals()
    ce = {
        "strike": g.get("ce_inst"),
        "instrument_token": g.get("ce_token"),
        "avg_price": g.get("ce_avg_price"),
        "ltp_entry": g.get("ce_ltp_entry"),
        "ltp": g.get("ce_ltp"),
        "sl_order_id": g.get("ce_sl_order_id"),
        "sl_hit": bool(g.get("ce_sl_hit")),
        "sl_jumped": bool(g.get("ce_sl_jumped")),
        "mkt_rejected": bool(g.get("ce_mkt_rejected")),
        "exit_price": g.get("ce_exit_price"),
    }
    pe = {
        "strike": g.get("pe_inst"),
        "instrument_token": g.get("pe_token"),
        "avg_price": g.get("pe_avg_price"),
        "ltp_entry": g.get("pe_ltp_entry"),
        "ltp": g.get("pe_ltp"),
        "sl_order_id": g.get("pe_sl_order_id"),
        "sl_hit": bool(g.get("pe_sl_hit")),
        "sl_jumped": bool(g.get("pe_sl_jumped")),
        "mkt_rejected": bool(g.get("pe_mkt_rejected")),
        "exit_price": g.get("pe_exit_price"),
    }
    if hedge_buy_today:
        ce["hedge"] = {
            "strike": g.get("ce_hedge_inst"),
            "instrument_token": g.get("ce_hedge_token"),
            "order_id": g.get("ce_hedge_order_id"),
            "avg_price": g.get("ce_hedge_avg_price"),
            "placed": bool(g.get("ce_hedge_placed")),
            "closed": bool(g.get("ce_hedge_closed")),
            "exit_price": g.get("ce_hedge_exit_price"),
            "ltp": g.get("ce_hedge_ltp"),
        }
        pe["hedge"] = {
            "strike": g.get("pe_hedge_inst"),
            "instrument_token": g.get("pe_hedge_token"),
            "order_id": g.get("pe_hedge_order_id"),
            "avg_price": g.get("pe_hedge_avg_price"),
            "placed": bool(g.get("pe_hedge_placed")),
            "closed": bool(g.get("pe_hedge_closed")),
            "exit_price": g.get("pe_hedge_exit_price"),
            "ltp": g.get("pe_hedge_ltp"),
        }
    doc = {
        "run_id": run_id,
        "client_id": client_id,
        "strategy_id": strategy_id,
        "instrument": inst,
        "lot_size": g.get("lot_size"),
        "no_of_lots": no_of_lots,
        "qty": g.get("qty"),
        "hedge_buy_today": bool(hedge_buy_today),
        "algo_status": core.get("Status"),
        "last_update_time": core.get("last_update_time"),
        "pnl": core.get("PNL"),
        "min_pnl": core.get("min_pnl"),
        "max_pnl": core.get("max_pnl"),
        "margin_used": core.get("margin_used"),
        "total_orders": core.get("total_orders"),
        "open_orders": core.get("open_orders"),
        "live_positions": core.get("live_positions"),
        "legs": {"CE": ce, "PE": pe},
    }
    path = _run_status_json_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(doc, f, indent=2, default=str)
    except OSError as e:
        logging.warning("Could not write %s: %s", path, e)


def _idp_set_status(status_update: dict) -> None:
    """Persist status only to ``{run_id}_status.json`` (Redis is read-only here for LTP + master)."""
    publish_idp_status(status_update)


def fetch_ltp(exch_seg,exch_id, retries=5, delay=1):
    for attempt in range(1, retries+1):
        try:
            val = r.get(f"ltp:{exch_id}")
            if val:
                try:
                    ltp = float(val)
                    if ltp > 0:
                        # logging.info(f" [Redis LTP] {exch_id} => {ltp}")
                        return ltp
                except ValueError:
                    logging.warning(f" [Redis LTP] Invalid value for {exch_id}: {val}")
        except Exception as re:
            logging.error(f" Redis error while fetching {exch_id}: {re}")
            time.sleep(delay)

    # after retries
    raise RuntimeError(f"Could not fetch LTP for {exch_id} after {retries} attempts")

def fetch_fno_ltp(exchange_id_list, exchange_segment, retries=5, delay=1):
    """
    Fetch LTPs for multiple tokens from Redis in one call (mget).
    Returns dict {token: ltp}
    """
    for attempt in range(1, retries + 1):
        try:
            # mget returns values in the same order as exchange_id_list
            keys = [f"ltp:{exch_id}" for exch_id in exchange_id_list]
            values = r.mget(keys)
            # values = r.mget(exchange_id_list)

            ltp_dict = {}
            for token, val in zip(exchange_id_list, values):
                if val:
                    try:
                        ltp = float(val)
                        if ltp > 0:
                            ltp_dict[token] = ltp
                        else:
                            logging.warning(f" [Redis LTP] Non-positive value for {token}: {val}")
                    except ValueError:
                        logging.warning(f" [Redis LTP] Invalid value for {token}: {val}")
                else:
                    logging.debug(f" [Redis LTP] No value for {token}")

            if ltp_dict:
                return ltp_dict

        except Exception as e:
            logging.error(f" Redis error while fetching {exchange_id_list}: {e}")
            time.sleep(delay)

    raise RuntimeError(f"Could not fetch LTPs for {exchange_id_list} after {retries} attempts")

def get_strike_prem(prem=100, inst_type="CE"):

    token_map = strike_map[inst_type]
    tokens = list(token_map.values())

    ltp_dict = fetch_fno_ltp(tokens, exchange_segment=None)

    if not ltp_dict:
        raise RuntimeError("No LTP data available in Redis")

    best_strike = None
    best_token = None
    best_ltp = None
    min_diff = float("inf")

    # iterate only over tokens that EXIST in Redis
    for strike, token in token_map.items():
        ltp = ltp_dict.get(token)
        if ltp is None:
            continue

        diff = abs(ltp - prem)
        if diff < min_diff:
            min_diff = diff
            best_strike = strike
            best_token = token
            best_ltp = ltp

    if best_token is None:
        raise RuntimeError("All strikes missing LTP in Redis")

    return best_strike, best_ltp, best_token


def parse_expiry(s: str) -> datetime | None:
    if not s:
        return None
    # handles "2025-12-26", "2025-12-26T00:00:00", "2025-12-26 00:00:00"
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass
    return None


def _fetch_redis_json_value(redis_key: str):
    """Load JSON from Redis: use ``GET`` only for STRING keys; use ``JSON.GET`` for RedisJSON keys.

    Calling ``GET`` on a RedisJSON key causes WRONGTYPE; we inspect ``TYPE`` first to avoid that.
    """
    kt = r.type(redis_key)
    if isinstance(kt, (bytes, bytearray)):
        kt = kt.decode("utf-8")
    if not kt or str(kt).lower() == "none":
        raise ValueError(f"Redis key {redis_key!r} does not exist")

    def _loads_maybe_wrap(text: str):
        data = json.loads(text)
        if isinstance(data, list) and len(data) == 1 and isinstance(data[0], list):
            return data[0]
        if isinstance(data, dict):
            for k in ("data", "result", "instruments", "rows", "items"):
                v = data.get(k)
                if isinstance(v, list):
                    return v
        return data

    if kt == "string":
        raw = r.get(redis_key)
        if raw is None or raw == "":
            raise ValueError(f"Redis key {redis_key!r} missing or empty (load instruments master first)")
        text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
        return _loads_maybe_wrap(text)

    last_err: Exception | None = None
    for args in (("JSON.GET", redis_key), ("JSON.GET", redis_key, "."), ("JSON.GET", redis_key, "$")):
        try:
            raw = r.execute_command(*args)
        except redis.exceptions.RedisError as e:
            last_err = e
            continue
        if raw is None or raw == "" or raw == b"":
            continue
        text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
        try:
            return _loads_maybe_wrap(text)
        except json.JSONDecodeError as e:
            last_err = e
            continue

    raise ValueError(
        f"Redis key {redis_key!r} has type {kt!r}; expected STRING JSON or RedisJSON module (JSON.GET). "
        f"Last error: {last_err!r}"
    ) from last_err


def load_master_from_redis(redis_key: str):
    rows = _fetch_redis_json_value(redis_key)
    if not isinstance(rows, list):
        raise ValueError(f"Redis master {redis_key!r} must be a JSON array of instruments, got {type(rows).__name__}")

    nearest = None
    for row in rows:
        dt = parse_expiry(row.get("expiry"))
        if dt and (nearest is None or dt < nearest):
            nearest = dt

    strike_map = {"CE": {}, "PE": {}}
    lot_size = None

    for row in rows:
        dt = parse_expiry(row.get("expiry"))
        if dt != nearest:
            continue

        inst_type = row.get("instrument_type")
        strike = row.get("strike")
        token = row.get("exchange_token")

        if inst_type in ("CE", "PE") and strike and token:
            strike_map[inst_type][int(strike)] = token

        if lot_size is None and row.get("lot_size"):
            lot_size = int(row["lot_size"])

    return nearest, strike_map, lot_size



def place_ce_hedge():
    global ce_hedge_inst, ce_hedge_avg_price, ce_hedge_order_id,ce_hedge_token,ce_hedge_placed
    try:
        # 1) Choose CE hedge strike
        print("Starting CE hedge placement",flush=True)
        ce_hedge_inst, ce_hedge_price, ce_hedge_token = get_strike_prem(prem=CE_Hedge_Premium, inst_type="CE")
        logging.info(f" [CE-HEDGE] Strike: {ce_hedge_inst}, LTP approx {ce_hedge_price}")
        ce_hedge_order_id = 1111111111
        # ce_hedge_token = token
        logging.info(f" [CE-HEDGE] MARKET order placed, ID = {ce_hedge_order_id}. Waiting to fill…")

        # 3) Poll until filled
        while True:
            status, status = "FILLED","FILLED"
            if status == "FILLED":
                ce_hedge_avg_price=ce_hedge_price
                ce_hedge_placed=True
                logging.info(f" [CE-HEDGE] MARKET {ce_hedge_order_id} filled @ avg {ce_hedge_avg_price}")
                break
            if status in ("CANCELLED", "REJECTED"):
                logging.info(f" [CE-HEDGE] MARKET {ce_hedge_order_id} {status}.")
                break
            time.sleep(1)

    except Exception:
        logging.exception(f" [CE-HEDGE] unexpected error")


def place_pe_hedge():
    global pe_hedge_inst, pe_hedge_avg_price, pe_hedge_order_id,pe_hedge_token,pe_hedge_placed
    try:
        # 1) Choose PE hedge strike
        print("Starting PE hedge placement",flush=True)
        pe_hedge_inst, pe_hedge_price, pe_hedge_token = get_strike_prem(prem=PE_Hedge_Premium, inst_type="PE")
        logging.info(f" [PE-HEDGE] Strike: {pe_hedge_inst}, LTP approx {pe_hedge_price}")
        pe_hedge_order_id = 2222222222
        # pe_hedge_token = token
        logging.info(f" [PE-HEDGE] MARKET order placed, ID = {pe_hedge_order_id}. Waiting to fill…")

        # 3) Poll until filled
        while True:
            status, status = "FILLED","FILLED"
            if status == "FILLED":
                pe_hedge_placed=True
                pe_hedge_avg_price=pe_hedge_price
                logging.info(f" [PE-HEDGE] MARKET {pe_hedge_order_id} filled @ avg {pe_hedge_avg_price}")
                # append_position_buy(pe_hedge_inst,"NSEFO","MIS",pe_hedge_avg_price,0,qty,0,qty,pe_hedge_avg_price)
                # update_strategy_orders()
                # log_order(order_id=pe_hedge_order_id,symbol=pe_hedge_inst,leg="PE_Hedge",action="ENTRY_FILLED",order_type="MARKET",quantity=qty,avg_price=pe_hedge_avg_price,status="FILLED")
                break

            if status in ("CANCELLED", "REJECTED"):
                logging.info(f" [PE-HEDGE] MARKET {pe_hedge_order_id} {status}.")
                break

            time.sleep(1)

    except Exception:
        logging.exception(f" [PE-HEDGE] unexpected error")

def place_ce_market_then_sl():
    global ce_inst, ce_token, ce_ltp_entry, ce_sl_order_id, ce_sl_hit, ce_mkt_rejected,ce_avg_price
    try:
        if hedge_buy_today == 0 or ce_hedge_placed:
            # 1) Choose CE strike based on CE_Premium
            ce_inst, ce_price, ce_token = get_strike_prem(prem=CE_Premium, inst_type="CE")
            logging.info(f" [CE] Strike: {ce_inst}, LTP approx {ce_price}")
            ce_order_id = 3333333333
            logging.info(f" [CE-SELL] MARKET order placed, ID = {ce_order_id}. Waiting to fill…")

            # 3) Poll until filled
            while True:
                status, status = "FILLED","FILLED"
                if status == "FILLED":
                    ce_avg_price=ce_price
                    ce_ltp_entry=ce_avg_price
                    logging.info(f" [CE-SELL] MARKET {ce_order_id} filled @ avg {ce_avg_price}")
                    # append_position_sell(ce_inst,"NSEFO","MIS",0,ce_avg_price,0,qty,qty,ce_avg_price)
                    # update_strategy_orders()
                    # log_order(order_id=ce_order_id,symbol=ce_inst,leg="CE_SELL",action="ENTRY_FILLED",order_type="MARKET",quantity=qty,avg_price=ce_avg_price,status="FILLED")
                    break
                if status == "REJECTED":
                    logging.info(f" [CE] MARKET order {ce_order_id} REJECTED. Skipping SL.")
                    ce_sl_hit = True
                    ce_mkt_rejected = True
                    return

                if status == "CANCELLED":
                    logging.info(f" [CE] MARKET order {ce_order_id} CANCELLED. Skipping SL.")
                    ce_sl_hit = True
                    ce_mkt_rejected = True
                    return
                time.sleep(1)

            if status == "FILLED":
                # 3a) Delay 3 seconds before placing SL
                logging.info(f" [CE] Waiting 3 seconds before placing SL…")
                time.sleep(Limit_Order_Delay)

                # 4) Compute CE SL limit & trigger based on ce_ltp_entry (avg fill)
                raw_limit   = ce_ltp_entry + CE_SL_Pts
                raw_trigger = ce_ltp_entry + CE_SL_Pts - Trigger_Pts_diff

                # round(x/0.05) * 0.05 => nearest multiple of 0.05
                ce_sl_limit   = round(raw_limit   / 0.05) * 0.05
                ce_sl_trigger = round(raw_trigger / 0.05) * 0.05
                logging.info(f" [CE] SL: limit={ce_sl_limit}, trigger={ce_sl_trigger}")
                ce_sl_order_id = 5555555555

        else:
            logging.info("Hedge not placed, skipping CE leg")
            ce_sl_hit = True
            ce_mkt_rejected = True
            return

    except Exception as e:
        # In the unlikely event of another exception, mark CE as done so SL loop doesn't spin forever
        logging.info(f" [CE Thread Exception] {e}")
        ce_sl_hit = True
        ce_mkt_rejected = True
        return


def place_pe_market_then_sl():
    global pe_inst, pe_token, pe_ltp_entry, pe_sl_order_id, pe_sl_hit, pe_mkt_rejected,pe_avg_price
    try:
        if hedge_buy_today == 0 or pe_hedge_placed:
            # 1) Choose PE strike based on PE_Premium
            pe_inst, pe_price, pe_token = get_strike_prem(prem=PE_Premium, inst_type="PE")
            logging.info(f" [PE] Strike: {pe_inst}, LTP approx {pe_price}")
            pe_order_id = 4444444444
            logging.info(f" [PE-HEDGE] MARKET order placed, ID = {pe_order_id}. Waiting to fill…")

            # 3) Poll until filled
            while True:
                status, status = "FILLED","FILLED"
                if status == "FILLED":
                    pe_avg_price=pe_price
                    pe_ltp_entry=pe_avg_price
                    logging.info(f" [PE-SELL] MARKET {pe_order_id} filled @ avg {pe_avg_price}")
                    # append_position_sell(pe_inst,"NSEFO","MIS",0,pe_avg_price,0,qty,qty,pe_avg_price)
                    # update_strategy_orders()
                    # log_order(order_id=pe_order_id,symbol=pe_inst,leg="PE_SELL",action="ENTRY_FILLED",order_type="MARKET",quantity=qty,avg_price=pe_avg_price,status="FILLED")
                    break
                if status == "REJECTED":
                    logging.info(f" [PE] MARKET order {pe_order_id} REJECTED. Skipping SL.")
                    pe_sl_hit = True
                    pe_mkt_rejected = True
                    return

                if status == "CANCELLED":
                    logging.info(f" [PE] MARKET order {pe_order_id} CANCELLED. Skipping SL.")
                    pe_sl_hit = True
                    pe_mkt_rejected = True
                    return

                time.sleep(1)
            if status == "FILLED":
                # 3a) Delay 3 seconds before placing SL
                logging.info(f" [PE] Waiting 3 seconds before placing SL…")
                time.sleep(Limit_Order_Delay)

                # 4) Compute PE SL limit & trigger based on ce_ltp_entry (avg fill)
                raw_limit   = pe_ltp_entry + PE_SL_Pts
                raw_trigger = pe_ltp_entry + PE_SL_Pts - Trigger_Pts_diff

                # round(x/0.05) * 0.05 => nearest multiple of 0.05
                pe_sl_limit   = round(raw_limit   / 0.05) * 0.05
                pe_sl_trigger = round(raw_trigger / 0.05) * 0.05
                logging.info(f" [PE] SL: limit={pe_sl_limit}, trigger={pe_sl_trigger}")
                pe_sl_order_id = 6666666666

        else:
            logging.info("Hedge not placed, skipping PE leg")
            pe_sl_hit = True
            pe_mkt_rejected = True
            return            
    except Exception as e:
        # In the unlikely event of another exception, mark PE as done so SL loop doesn't spin forever
        logging.info(f" [PE Thread Exception] {e}")
        pe_sl_hit = True
        return




######################################################################################################################################################



# ─────────────────────────────────────────────────────────────────────────────
# Main program start
logging.info(f"Program started")

try:
    status_update = {
        "last_update_time": datetime.now().isoformat(timespec="seconds"),
        "Status": "INIT",
        "PNL": "-",
        "margin_used": 999999,
        "total_orders": 6,
        "open_orders": 0,
        "live_positions": 0,
        "min_pnl": 0,
        "max_pnl": 0
    }
    _idp_set_status(status_update)
except Exception as ex:
    print(f"Error writing run status file: {ex}")
###################################################################

"""Get Master Instruments Request (NO pandas)"""
try:
    if inst == "NIFTY":
        redis_key = "0_Master_Data:nifty_weekly_kite"
    elif inst == "SENSEX":
        redis_key = "0_Master_Data:sensex_weekly_kite"
    else:
        raise ValueError(f"Unsupported inst: {inst}")

    nearest_expiry_dt, strike_map, ls = load_master_from_redis(redis_key)

    if ls is not None:
        lot_size = ls

    if not strike_map["CE"] or not strike_map["PE"]:
        raise ValueError(
            f"No CE/PE strikes in master for nearest expiry (CE={len(strike_map['CE'])} PE={len(strike_map['PE'])}). "
            f"Check Redis key {redis_key!r} and expiry rows."
        )

    logging.info(f"Master loaded from Redis: {redis_key}, expiry={nearest_expiry_dt}")

except Exception as e1:
    logging.error(f"Redis master load failed: {e1}")
    logging.error("Cannot run without strike_map; exiting (CSV fallback not implemented).")
    sys.exit(1)


try:
    if inst=='NIFTY':
        spot_ltp = fetch_ltp(1,1001)
    elif inst=='SENSEX':    
        spot_ltp = fetch_ltp(11,1)
    ATM      = int(spot_ltp / step) * step
    strikes  = list(range(ATM - strike_range, ATM + strike_range + step, step))
    logging.info(f"ATM = {ATM}, strikes computed")
except:
    logging.error(f"Index Spot LTP not found in DB. Exiting algo.. ")
    print(f"{datetime.now()}LTP not fetched for Spot Index.",flush=True)
    try:
        status_update = {
            "last_update_time": datetime.now().isoformat(timespec="seconds"),
            "Status": "Spot Err",
            "PNL": "-",
            "margin_used": 999999,
            "total_orders": 6,
            "open_orders": 0,
            "live_positions": 0,
            "min_pnl": 0,
            "max_pnl": 0
        }
        _idp_set_status(status_update)
    except Exception as ex:
        print(f"Error writing run status file: {ex}")
    time.sleep(5)
    sys.exit()



try:
    # Check leg SL and get exit price
    status_update = {
        "last_update_time": datetime.now().isoformat(timespec="seconds"),
        "Status": "READY",
        "PNL": "-",
        "margin_used": 0,
        "total_orders": 0,
        "open_orders": 0,
        "live_positions": 0,
        "min_pnl": 0,
        "max_pnl": 0
    }

    _idp_set_status(status_update)
    print(f"Status initialised")


except Exception as ex:
    print(f"Error in getting orders/positions: {ex}")

entry_dt = wr.today_at(entry_time)
exit_dt = wr.today_at(exit_time)

# hedge_target_dt = entry_dt + timedelta(seconds=Hedge_Place_Delay)
hedge_target_dt = entry_dt - timedelta(seconds=Hedge_Place_Delay)
# Main while‐loop for hedges + SL monitoring and modification
last_trigger_time = datetime.now()
last_minute = -1
last_run = 0
interval = 10  # seconds
logging.info(f"Waiting for Entry {entry_time}")
logging.warning(f"RSS after init: {rss_mb()} MB")

# Worker-style wait until entry_time (signal file + orders snapshot + SIGTERM)
exit_reason_pre = None
while wr.running:
    now = datetime.now()
    if now >= entry_dt:
        break
    cmd = wr.consume_exit_from_worker_json(worker_json_path)
    if cmd:
        exit_reason_pre = cmd
        wr.log(log_file, run_id, f"SIGNAL received during wait: {cmd}")
        break
    wait = min(wr.HEARTBEAT_INTERVAL, (entry_dt - now).total_seconds() + 1)
    wr.log(log_file, run_id, f"WAITING | entry_time={entry_time} (in {int((entry_dt - now).total_seconds())}s)")
    slept = 0
    while slept < wait and wr.running:
        time.sleep(min(wr.SIGNAL_CHECK_INTERVAL, wait - slept))
        slept += wr.SIGNAL_CHECK_INTERVAL
        cmd = wr.consume_exit_from_worker_json(worker_json_path)
        if cmd:
            exit_reason_pre = cmd
            wr.log(log_file, run_id, f"SIGNAL received during wait: {cmd}")
            wr.running = False
            break
    wr.maybe_orders_snapshot(log_file, run_id, args)

if exit_reason_pre == "exit_algo":
    wr.log(log_file, run_id, "EXIT_ALGO | Stopped during wait")
    sys.exit(0)
if exit_reason_pre == "exit_trade":
    wr.log(log_file, run_id, "EXIT_TRADE | Stopped during wait (no open algo yet)")
    sys.exit(0)
if not wr.running:
    wr.log(log_file, run_id, "KILLED | SIGTERM during wait")
    sys.exit(0)

wr.log(log_file, run_id, "ACTIVE | IDP trading loop")

while True:
    try:
        if not wr.running:
            logging.warning("SIGTERM: stopping IDP loop")
            sys.exit(0)

        sig = wr.consume_exit_from_worker_json(worker_json_path)
        if sig == "exit_algo":
            exit_algo_now = 1
        elif sig == "exit_trade":
            exittradenow = 1

        time.sleep(0.5)
        now = datetime.now()
        now_str = now.strftime("%H%M%S")
        cur_HHMMSS = int(now_str)
        # print(f"Time: {now_str} Entry: {entry_dt.time()} Exit: {exit_dt.time()} Hedge: {hedge_target_dt.time()}",flush=True)
        if exit_algo_now==1:
            logging.warning(f"Kill algo triggered. Exiting algo. User to close positions and open orders")

            status_update = {
                "last_update_time": now.isoformat(timespec="seconds"),
                "Status": "EXITED",
                "PNL": pnl,
                "margin_used": 999999,
                "total_orders": 6,
                "open_orders": 0,
                "live_positions": 0,
                "min_pnl": min_pnl,
                "max_pnl": max_pnl
            }
            _idp_set_status(status_update)            
            sys.exit()


        # ──────────────── Hedge logic ────────────────────────────────────────
        # if hedge_buy_today == 1 and not hedge_placed and cur_HHMMSS >= int(f"{Hedge_Entry_hour:02d}{Hedge_Entry_min:02d}{Hedge_Entry_sec:02d}"):
        if hedge_buy_today == 1 and not hedge_placed and now >= hedge_target_dt:
            # If current time >= hedge entry time, place hedges
            # if cur_HHMMSS >= int(f"{Hedge_Entry_hour:02d}{Hedge_Entry_min:02d}{Hedge_Entry_sec:02d}"):
            hedge_placed = True
            logging.info(f" Hedge entry time reached. Placing hedges.")

            # Launch CE-HEDGE and PE-HEDGE threads
            t_ce_h = threading.Thread(target=place_ce_hedge)
            t_pe_h = threading.Thread(target=place_pe_hedge)
            t_ce_h.start()
            t_pe_h.start()
            t_ce_h.join()
            t_pe_h.join()

            logging.info(f"Hedge orders: CE {ce_hedge_order_id} at {ce_hedge_avg_price}, "
                  f"PE {pe_hedge_order_id} at {pe_hedge_avg_price}")
            # add logic to check order status and exit if both orders are rejected.

        # ──────────────── Main entry logic ─────────────────────────────────
        if first_order and now >= entry_dt:
            first_order = False
            # sleep_until_precise(first_entry_hour, first_entry_min, first_entry_sec)
            logging.info(f"Entry Started")

            # Launch CE and PE threads to place market then SL
            t_ce = threading.Thread(target=place_ce_market_then_sl)
            t_pe = threading.Thread(target=place_pe_market_then_sl)
            t_ce.start()
            t_pe.start()
            t_ce.join()
            t_pe.join()

            if ce_sl_order_id and pe_sl_order_id:
                logging.info(f"CE SL ID: {ce_sl_order_id}    PE SL ID: {pe_sl_order_id}")
            else:
                logging.warning(f"One or more of SL orders not placed. Check positions and limit orders")


        # ──────────────── SL monitoring & modification ───────────────────────
        elif first_order==False:
            # ce_ltp = fetch_ltp(2,ce_token)
            # pe_ltp = fetch_ltp(2,pe_token)
            try:
                ltp_dict=fetch_fno_ltp([ce_token,pe_token],2)
                ce_ltp=ltp_dict[ce_token]
                pe_ltp=ltp_dict[pe_token]

            except:
                try:
                    logging.info("Temp: Error getting ltp for both ce pe")
                    ce_ltp = fetch_ltp(2,ce_token)
                    pe_ltp = fetch_ltp(2,pe_token)
                except:
                    print(f"Error in getting Sell LTPs")            
            try:
                
                if hedge_buy_today ==1:
                    ce_hedge_ltp = fetch_ltp(2,ce_hedge_token)
                    pe_hedge_ltp = fetch_ltp(2,pe_hedge_token)  
            except:
                print("Error in hedge token")            
                    


            # 1) Check CE leg if pending
                        
            if not ce_sl_hit and not ce_sl_jumped and ce_sl_order_id:
                ce_sl_limit_orig = ce_ltp_entry + CE_SL_Pts
                if ce_ltp>ce_sl_limit_orig:
                    PE_SL_Pts = - Trigger_Pts_diff
                    status, status = "FILLED","FILLED"
                    # logging.info(status)

                    if status == "PENDINGNEW":
                        print(f" [CE] SL is TRIGGER PENDING (LTP={ce_ltp})")
                    elif status == "OPEN" or status == "NEW" or status == "REPLACED":
                        pass
                        # print(f" [CE] SL is NEW/OPEN at limit {ce_sl_limit_orig} (LTP={ce_ltp})")

                    elif status == "FILLED":
                        ce_sl_hit = True
                        ce_exit_price=ce_ltp
                        logging.info(f" [CE] SL filled @ {ce_exit_price}")
                        # update_position(ce_inst,"NSEFO","MIS",ce_exit_price,ce_avg_price,qty,qty,0,ce_exit_price)
                        # update_strategy_orders()
                        if ce_hedge_placed:
                            logging.info(f"Closing CE hedge.")
                            # CE hedge close
                            if ce_hedge_order_id and ce_hedge_inst and not ce_hedge_closed:
                                try:
                                    ce_hed_exit_id = 7777777777
                                    logging.info(f" [CE-HEDGE] Placed hedge SELL order {ce_hed_exit_id}. Waiting to fill…")
                                    while True:
                                        st, _ = "FILLED","FILLED"
                                        if st == "FILLED":
                                            ce_hedge_closed=True
                                            ce_hedge_exit_price=ce_hedge_ltp
                                            # update_position(ce_hedge_inst,"NSEFO","MIS",ce_hedge_avg_price,ce_hedge_exit_price,qty,qty,0,ce_hedge_exit_price)
                                            # update_strategy_orders()                                            
                                            logging.info(f" [CE-HEDGE] Hedge SELL {ce_hed_exit_id} filled @ avg {ce_hedge_exit_price}")
                                            break
                                        if st in ("CANCELLED", "REJECTED"):
                                            logging.info(f" [CE-HEDGE] Hedge SELL {ce_hed_exit_id} {st}.")
                                            break
                                        time.sleep(1)
                                except Exception as e:
                                    logging.info(f" [CE-HEDGE] Error closing hedge: {e}")


                        if not pe_sl_hit and not pe_mkt_rejected:
                            # Otherwise, modify PE SL to break‐even
                            pe_new_limit   = round(pe_ltp_entry / 0.05) * 0.05
                            pe_new_trigger = pe_new_limit - Trigger_Pts_diff

                            # Fetch current PE LTP
                            # pe_current_ltp = fetch_ltp(2,pe_token)
                            if pe_new_trigger <= pe_ltp:
                                logging.info(f" [CE>PE] Desired PE trigger ({pe_new_trigger}) "
                                      f"≤ current LTP ({pe_ltp}). Exiting PE at market.")
                                try:
                                    pe_exit_id = 8888888888
                                    logging.info(f" [PE] MARKET exit order placed, ID = {pe_exit_id}. Waiting to fill…")
                                    while True:
                                        st, _ = "FILLED","FILLED"
                                        if st == "FILLED":
                                            pe_sl_hit = True
                                            pe_exit_price=pe_ltp
                                            logging.info(f" [PE] Exit MARKET {pe_exit_id} filled @ avg {pe_exit_price}")
                                            # update_position(pe_inst,"NSEFO","MIS",pe_exit_price,pe_avg_price,qty,qty,0,pe_exit_price)
                                            # update_strategy_orders()                                            
                                            break
                                        if st in ("CANCELLED", "REJECTED"):
                                            logging.info(f" [PE] Exit MARKET {pe_exit_id} {st}.")
                                            break
                                        time.sleep(1)
                                    if st=='FILLED' and pe_hedge_placed and not pe_hedge_closed:
                                    # Close hedge
                                        if pe_hedge_order_id and pe_hedge_inst:
                                            try:
                                                pe_hed_exit_id = 1212121212
                                                logging.info(f" [PE-HEDGE] Placed hedge SELL order {pe_hed_exit_id}. Waiting to fill…")
                                                while True:
                                                    st, _ = "FILLED","FILLED"
                                                    if st == "FILLED":
                                                        pe_hedge_closed=True
                                                        pe_hedge_exit_price=pe_hedge_ltp
                                                        logging.info(f" [PE-HEDGE] Hedge SELL {pe_hed_exit_id} filled @ avg {pe_hedge_exit_price}")
                                                        # update_position(pe_hedge_inst,"NSEFO","MIS",pe_hedge_avg_price,pe_hedge_exit_price,qty,qty,0,pe_hedge_exit_price)
                                                        # update_strategy_orders()
                                                        break
                                                    if st in ("CANCELLED", "REJECTED"):
                                                        logging.info(f" [PE-HEDGE] Hedge SELL {pe_hed_exit_id} {st}.")
                                                        break
                                                    time.sleep(1)
                                            except Exception as e:
                                                logging.info(f" [PE-HEDGE] Error closing hedge: {e}")                                    
                                except Exception as ex:
                                    logging.info(f" [CE>PE] Error placing PE market exit: {ex}")


            # 2) Check PE leg if pending
            
            if not pe_sl_hit and not pe_sl_jumped and pe_sl_order_id:
                pe_sl_limit_orig = pe_ltp_entry + PE_SL_Pts

                if pe_ltp>pe_sl_limit_orig:
                    CE_SL_Pts = - Trigger_Pts_diff
                    status, status = "FILLED","FILLED"

                    if status == "PENDINGNEW":
                        print(f" [PE] SL is TRIGGER PENDING (LTP={pe_ltp})")
                    elif status == "OPEN" or status == "NEW" or status == "REPLACED":
                        pass
                        # print(f" [PE] SL is NEW/OPEN at limit {pe_sl_limit_orig} (LTP={pe_ltp})")
                        
                    elif status == "FILLED":
                        pe_sl_hit = True
                        pe_exit_price =pe_ltp
                        logging.info(f" [PE] SL filled @ {pe_exit_price}")
                        # update_position(pe_inst,"NSEFO","MIS",pe_exit_price,pe_avg_price,qty,qty,0,pe_exit_price)

                        if pe_hedge_placed and not pe_hedge_closed:
                        # Close hedge
                            if pe_hedge_order_id and pe_hedge_inst:
                                try:
                                    pe_hed_exit_id = 1414141414
                                    logging.info(f" [PE-HEDGE] Placed hedge SELL order {pe_hed_exit_id}. Waiting to fill…")
                                    while True:
                                        st, _ = "FILLED","FILLED"
                                        if st == "FILLED":
                                            pe_hedge_closed=True
                                            pe_hedge_exit_price=pe_hedge_ltp
                                            logging.info(f" [PE-HEDGE] Hedge SELL {pe_hed_exit_id} filled @ avg {pe_hedge_exit_price}")
                                            # update_position(pe_hedge_inst,"NSEFO","MIS",pe_hedge_avg_price,pe_hedge_exit_price,qty,qty,0,pe_hedge_exit_price)
                                            # update_strategy_orders()
                                            break
                                        if st in ("CANCELLED", "REJECTED"):
                                            logging.info(f" [PE-HEDGE] Hedge SELL {pe_hed_exit_id} {st}.")
                                            break
                                        time.sleep(1)
                                except Exception as e:
                                    logging.info(f" [PE-HEDGE] Error closing hedge: {e}") 

                        if not ce_sl_hit and not ce_mkt_rejected:
                            # Otherwise, modify CE SL to break‐even
                            ce_new_limit   = round(ce_ltp_entry/0.05) * 0.05
                            ce_new_trigger = ce_new_limit - Trigger_Pts_diff

                            # Fetch current CE LTP
                            # ce_ltp = fetch_ltp(2,ce_token)
                            if ce_new_trigger <= ce_ltp:
                                logging.info(f" [PE>CE] Desired CE trigger ({ce_new_trigger}) "
                                      f"≤ current LTP ({ce_ltp}). Exiting CE at market.")
                                try:
                                    ce_exit_id = 7777777777
                                    logging.info(f" [CE] MARKET exit order placed, ID = {ce_exit_id}. Waiting to fill…")
                                    while True:
                                        st, _ = "FILLED","FILLED"
                                        if st == "FILLED":
                                            ce_exit_price=ce_ltp
                                            # update_position(ce_inst,"NSEFO","MIS",ce_exit_price,ce_avg_price,qty,qty,0,ce_exit_price)
                                            # update_strategy_orders()
                                            logging.info(f" [CE] Exit MARKET {ce_exit_id} filled @ avg {ce_exit_price}")
                                            ce_sl_hit = True
                                            break
                                        if st in ("CANCELLED", "REJECTED"):
                                            logging.info(f" [CE] Exit MARKET {ce_exit_id} {st}.")
                                            break
                                        time.sleep(1)

                                    if ce_hedge_placed and st=='FILLED':
                                        logging.info(f"Closing CE hedge.")
                                        # CE hedge close
                                        if ce_hedge_order_id and ce_hedge_inst and not ce_hedge_closed:
                                            try:
                                                ce_hed_exit_id = 1212121212
                                                logging.info(f" [CE-HEDGE] Placed hedge SELL order {ce_hed_exit_id}. Waiting to fill…")
                                                while True:
                                                    st, _ = "FILLED","FILLED"
                                                    if st == "FILLED":
                                                        ce_hedge_closed=True
                                                        ce_hedge_exit_price=ce_hedge_ltp
                                                        # update_position(ce_hedge_inst,"NSEFO","MIS",ce_hedge_avg_price,ce_hedge_exit_price,qty,qty,0,ce_hedge_exit_price)
                                                        # update_strategy_orders()
                                                        logging.info(f" [CE-HEDGE] Hedge SELL {ce_hed_exit_id} filled @ avg {ce_hedge_exit_price}")
                                                        break
                                                    if st in ("CANCELLED", "REJECTED"):
                                                        logging.info(f" [CE-HEDGE] Hedge SELL {ce_hed_exit_id} {st}.")
                                                        break
                                                    time.sleep(1)
                                            except Exception as e:
                                                logging.info(f" [CE-HEDGE] Error closing hedge: {e}")
                                               

                                except Exception as ex:
                                    logging.info(f" [PE>CE] Error placing CE market exit: {ex}")



            # 3) If exit_time reached, cancel & close remaining legs (and hedges)
            if now >= exit_dt or exittradenow>0:
                logging.info(f"Exit time reached ({now_str}) or exittradenow. Closing pending legs and hedges…")

                # Cancel or exit CE SL if still pending
                if not ce_sl_hit and not ce_sl_jumped and ce_sl_order_id:
                    try:
                        logging.info(f"CE Limit cancelled.")
                    except Exception as ex:
                        logging.info(f"CE Limit order Cancel: {ex}")

                    try:
                        ce_exit_id = 1515151515
                        logging.info(f" [CE] MARKET exit order placed, ID = {ce_exit_id}. Waiting to fill…")
                        while True:
                            st, _ = "FILLED","FILLED"
                            if st == "FILLED":
                                ce_exit_price=ce_ltp
                                # update_position(ce_inst,"NSEFO","MIS",ce_exit_price,ce_avg_price,qty,qty,0,ce_exit_price)
                                # update_strategy_orders()
                                logging.info(f" [CE] Exit MARKET {ce_exit_id} filled @ avg {ce_exit_price}")
                                ce_sl_hit = True
                                break
                            if st in ("CANCELLED", "REJECTED"):
                                logging.info(f" [CE] Exit MARKET {ce_exit_id} {st}.")
                                break
                            time.sleep(1)
                    except Exception as ex:
                        logging.info(f"CE Exit-time error: {ex}")

                # Cancel or exit PE SL if still pending
                if not pe_sl_hit and not pe_sl_jumped and pe_sl_order_id:
                    try:
                        logging.info(f"PE Limit Order Cancelled.")
                    except Exception as ex:
                        logging.info(f"PE Limit order Cancel: {ex}")

                    try:
                        pe_exit_id = 1616161616
                        logging.info(f" [PE] MARKET exit order placed, ID = {pe_exit_id}. Waiting to fill…")
                        while True:
                            st, _ = "FILLED","FILLED"
                            if st == "FILLED":
                                pe_sl_hit = True
                                pe_exit_price=pe_ltp
                                logging.info(f" [PE] Exit MARKET {pe_exit_id} filled @ avg {pe_exit_price}")
                                # update_position(pe_inst,"NSEFO","MIS",pe_exit_price,pe_avg_price,qty,qty,0,pe_exit_price)
                                # update_strategy_orders()                                            
                                break
                            if st in ("CANCELLED", "REJECTED"):
                                logging.info(f" [PE] Exit MARKET {pe_exit_id} {st}.")
                                break
                            time.sleep(1)                        
                        logging.info(f"PE exit: closed PE at market.")
                    except Exception as ex:
                        logging.info(f"PE Exit-time error: {ex}")

                # Close hedges if placed
                if ce_hedge_placed:
                    logging.info(f"Closing hedges at exit time.")
                    # CE hedge close
                    time.sleep(Hedge_close_delay)
                    if int(ce_hedge_order_id)>0 and ce_hedge_inst and not ce_hedge_closed:
                        try:

                            ce_hed_exit_id = 1717171717
                            logging.info(f" [CE-HEDGE] Placed hedge SELL order {ce_hed_exit_id}. Waiting to fill…")
                            while True:
                                st, _ = "FILLED","FILLED"
                                if st == "FILLED":
                                    ce_hedge_closed=True
                                    ce_hedge_exit_price=ce_hedge_ltp
                                    # update_position(ce_hedge_inst,"NSEFO","MIS",ce_hedge_avg_price,ce_hedge_exit_price,qty,qty,0,ce_hedge_exit_price)
                                    # update_strategy_orders()
                                    logging.info(f" [CE-HEDGE] Hedge SELL {ce_hed_exit_id} filled @ avg {ce_hedge_exit_price}")
                                    break
                                if st in ("CANCELLED", "REJECTED"):
                                    logging.info(f" [CE-HEDGE] Hedge SELL {ce_hed_exit_id} {st}.")
                                    break
                                time.sleep(1)
                        except Exception as e:
                            logging.info(f" [CE-HEDGE] Error closing hedge: {e}")
                if pe_hedge_placed:
                    # PE hedge close
                    time.sleep(Hedge_close_delay)
                    if int(pe_hedge_order_id)>0 and pe_hedge_inst and not pe_hedge_closed:
                        try:
                            pe_hed_exit_id = 1818181818
                            logging.info(f" [PE-HEDGE] Placed hedge SELL order {pe_hed_exit_id}. Waiting to fill…")
                            while True:
                                st, _ = "FILLED","FILLED"
                                if st == "FILLED":
                                    pe_hedge_closed=True
                                    pe_hedge_exit_price=pe_hedge_ltp
                                    # update_position(pe_hedge_inst,"NSEFO","MIS",pe_hedge_avg_price,pe_hedge_exit_price,qty,qty,0,pe_hedge_exit_price)
                                    # update_strategy_orders()
                                    logging.info(f" [PE-HEDGE] Hedge SELL {pe_hed_exit_id} filled @ avg {pe_hedge_exit_price}")
                                    break
                                if st in ("CANCELLED", "REJECTED"):
                                    logging.info(f" [PE-HEDGE] Hedge SELL {pe_hed_exit_id} {st}.")
                                    break
                                time.sleep(1)
                        except Exception as e:
                            logging.info(f" [PE-HEDGE] Error closing hedge: {e}")

                # Validate if any open orders with strategy ID and generate Error
                try:
                    ce_m2m = ce_exit_price if ce_sl_hit and ce_exit_price is not None else ce_ltp
                    pe_m2m = pe_exit_price if pe_sl_hit and pe_exit_price is not None else pe_ltp
                    ce_leg = ce_avg_price - ce_m2m
                    pe_leg = pe_avg_price - pe_m2m

                    # Hedge legs: use exit price if closed, else LTP


                    if hedge_buy_today==1:
                        ce_hedge_m2m = ce_hedge_exit_price if ce_hedge_closed and ce_hedge_exit_price is not None else ce_hedge_ltp
                        pe_hedge_m2m = pe_hedge_exit_price if pe_hedge_closed and pe_hedge_exit_price is not None else pe_hedge_ltp
                        print(now,client_id,ce_sl_hit,pe_sl_hit,ce_ltp,ce_avg_price,pe_ltp,pe_avg_price,
                            ce_hedge_ltp,ce_hedge_avg_price,pe_hedge_ltp,pe_hedge_avg_price,(ce_leg+pe_leg))
                        pnl_sell = (ce_leg + pe_leg) * qty
                        # pnl_buy  = ((ce_hedge_avg_price - ce_hedge_m2m) + (pe_hedge_avg_price - pe_hedge_m2m)) * qty
                        pnl_buy  = ((ce_hedge_m2m - ce_hedge_avg_price) + (pe_hedge_m2m - pe_hedge_avg_price)) * qty
                        pnl = pnl_sell + pnl_buy
                    else:
                        print(now,client_id,ce_sl_hit,pe_sl_hit,ce_ltp,ce_avg_price,pe_ltp,pe_avg_price,(ce_leg+pe_leg))
                        pnl = (ce_leg + pe_leg) * qty

                    max_pnl = max(max_pnl, pnl)
                    min_pnl = min(min_pnl, pnl)

                    status_update = {
                        "last_update_time": now.isoformat(timespec="seconds"),
                        "Status": "CLOSED",
                        "PNL": pnl,
                        "margin_used": 999999,
                        "total_orders": 6,
                        "open_orders": 0,
                        "live_positions": 0,
                        "min_pnl": min_pnl,
                        "max_pnl": max_pnl
                    }
                    _idp_set_status(status_update)
                except Exception as ex:
                    print(f"Error in calculating PNL: {ex}")

                sys.exit()

####################################################################################################################################


            # if now.second <60 and now.minute != last_minute:
            #     last_minute = now.minute 
            if time.time() - last_run >= interval:
                last_run = time.time()
                wr.maybe_orders_snapshot(log_file, run_id, args)

                try:
                    # Check leg SL and get exit price
                    try:
                        ce_m2m = ce_exit_price if ce_sl_hit and ce_exit_price is not None else ce_ltp
                        pe_m2m = pe_exit_price if pe_sl_hit and pe_exit_price is not None else pe_ltp
                        ce_leg = ce_avg_price - ce_m2m
                        pe_leg = pe_avg_price - pe_m2m

                        # Hedge legs: use exit price if closed, else LTP


                        if hedge_buy_today==1:
                            ce_hedge_m2m = ce_hedge_exit_price if ce_hedge_closed and ce_hedge_exit_price is not None else ce_hedge_ltp
                            pe_hedge_m2m = pe_hedge_exit_price if pe_hedge_closed and pe_hedge_exit_price is not None else pe_hedge_ltp
                            print(now,client_id,ce_sl_hit,pe_sl_hit,ce_ltp,ce_avg_price,pe_ltp,pe_avg_price,
                                ce_hedge_ltp,ce_hedge_avg_price,pe_hedge_ltp,pe_hedge_avg_price,(ce_leg+pe_leg))
                            pnl_sell = (ce_leg + pe_leg) * qty
                            # pnl_buy  = ((ce_hedge_avg_price - ce_hedge_m2m) + (pe_hedge_avg_price - pe_hedge_m2m)) * qty
                            pnl_buy  = ((ce_hedge_m2m - ce_hedge_avg_price) + (pe_hedge_m2m - pe_hedge_avg_price)) * qty
                            pnl = pnl_sell + pnl_buy
                        else:
                            print(now,client_id,ce_sl_hit,pe_sl_hit,ce_ltp,ce_avg_price,pe_ltp,pe_avg_price,(ce_leg+pe_leg))
                            pnl = (ce_leg + pe_leg) * qty

                        max_pnl = max(max_pnl, pnl)
                        min_pnl = min(min_pnl, pnl)


                    except Exception as ex:
                        # print(f"Error in calculating PNL: {ex}")
                        pnl=0
                        max_pnl=0
                        min_pnl=0


                    if ce_sl_hit and not pe_sl_hit:
                        status_update = {
                            "last_update_time": now.isoformat(timespec="seconds"),
                            "Status": "CE SL Hit",
                            "PNL": pnl,
                            "margin_used": 999999,
                            "total_orders": 5,
                            "open_orders": 1,
                            "live_positions": 2,
                            "min_pnl": min_pnl,
                            "max_pnl": max_pnl
                        }
                        _idp_set_status(status_update)
                    elif pe_sl_hit and not ce_sl_hit:
                        status_update = {
                            "last_update_time": now.isoformat(timespec="seconds"),
                            "Status": "PE SL Hit",
                            "PNL": pnl,
                            "margin_used": 999999,
                            "total_orders": 5,
                            "open_orders": 1,
                            "live_positions": 2,
                            "min_pnl": min_pnl,
                            "max_pnl": max_pnl
                        }
                        _idp_set_status(status_update)
                    elif pe_sl_hit and ce_sl_hit:
                        status_update = {
                            "last_update_time": now.isoformat(timespec="seconds"),
                            "Status": "CLOSED",
                            "PNL": pnl,
                            "margin_used": 999999,
                            "total_orders": 6,
                            "open_orders": 0,
                            "live_positions": 0,
                            "min_pnl": min_pnl,
                            "max_pnl": max_pnl
                        }
                        _idp_set_status(status_update)
                    else:
                        status_update = {
                            "last_update_time": now.isoformat(timespec="seconds"),
                            "Status": "RUNNING",
                            "PNL": pnl,
                            "margin_used": 999999,
                            "total_orders": 4,
                            "open_orders": 2,
                            "live_positions": 4,
                            "min_pnl": min_pnl,
                            "max_pnl": max_pnl
                        }
                        _idp_set_status(status_update)
                except Exception as ex:
                    print(f"Error in getting orders/positions: {ex}", flush=True)



##########################################################################################################
            if ce_sl_hit and pe_sl_hit:
                logging.info(f" Both SL hit exiting algo")
                try:
                    ce_m2m = ce_exit_price if ce_sl_hit and ce_exit_price is not None else ce_ltp
                    pe_m2m = pe_exit_price if pe_sl_hit and pe_exit_price is not None else pe_ltp
                    ce_leg = ce_avg_price - ce_m2m
                    pe_leg = pe_avg_price - pe_m2m

                    # Hedge legs: use exit price if closed, else LTP


                    if hedge_buy_today==1:
                        ce_hedge_m2m = ce_hedge_exit_price if ce_hedge_closed and ce_hedge_exit_price is not None else ce_hedge_ltp
                        pe_hedge_m2m = pe_hedge_exit_price if pe_hedge_closed and pe_hedge_exit_price is not None else pe_hedge_ltp
                        print(now,client_id,ce_sl_hit,pe_sl_hit,ce_ltp,ce_avg_price,pe_ltp,pe_avg_price,
                            ce_hedge_ltp,ce_hedge_avg_price,pe_hedge_ltp,pe_hedge_avg_price,(ce_leg+pe_leg))
                        pnl_sell = (ce_leg + pe_leg) * qty
                        # pnl_buy  = ((ce_hedge_avg_price - ce_hedge_m2m) + (pe_hedge_avg_price - pe_hedge_m2m)) * qty
                        pnl_buy  = ((ce_hedge_m2m - ce_hedge_avg_price) + (pe_hedge_m2m - pe_hedge_avg_price)) * qty
                        pnl = pnl_sell + pnl_buy
                    else:
                        print(now,client_id,ce_sl_hit,pe_sl_hit,ce_ltp,ce_avg_price,pe_ltp,pe_avg_price,(ce_leg+pe_leg))
                        pnl = (ce_leg + pe_leg) * qty

                    max_pnl = max(max_pnl, pnl)
                    min_pnl = min(min_pnl, pnl)

                    status_update = {
                        "last_update_time": now.isoformat(timespec="seconds"),
                        "Status": "CLOSED",
                        "PNL": pnl,
                        "margin_used": 999999,
                        "total_orders": 6,
                        "open_orders": 0,
                        "live_positions": 0,
                        "min_pnl": min_pnl,
                        "max_pnl": max_pnl
                    }
                    _idp_set_status(status_update)

                except Exception as ex:
                    print(f"Error in calculating PNL: {ex}")
                sys.exit()
##################################################################################################

        time.sleep(0.5)
    except Exception as e:
        logging.info(f" [Main Loop Exception] {e}")
        time.sleep(2)


