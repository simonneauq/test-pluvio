from __future__ import annotations

import argparse

from lgv_pluvio.config import load_config
from lgv_pluvio.data_pipeline.refresh_service import run_refresh


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh des donnees LGV")
    parser.add_argument("--daily", action="store_true", help="Refresh quotidien")
    parser.add_argument("--full", action="store_true", help="Refresh complet")
    args = parser.parse_args()

    mode = "full" if args.full else "daily"
    result = run_refresh(load_config(), mode=mode)
    print(result.message)


if __name__ == "__main__":
    main()
