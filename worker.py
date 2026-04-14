"""
Common algo worker entrypoint. Broker is selected via --broker (same as POST /start payload).
"""

from __future__ import annotations

import argparse
import sys

import broker_impl
from worker_runtime import add_common_worker_cli, load_worker_dotenv, run


def main() -> None:
    parser = argparse.ArgumentParser(description="Trading algo worker (broker via --broker)")
    add_common_worker_cli(parser)
    args = parser.parse_args()
    broker = broker_impl.resolve_broker_from_args(vars(args))
    if broker not in broker_impl.ALLOWED_BROKERS:
        print(
            f"broker must be one of {sorted(broker_impl.ALLOWED_BROKERS)} "
            f"(e.g. kite, zerodha, kotak); got {args.broker!r}",
            file=sys.stderr,
        )
        sys.exit(1)
    load_worker_dotenv()
    run(args, broker_impl.hooks_for_broker(broker))


if __name__ == "__main__":
    main()
