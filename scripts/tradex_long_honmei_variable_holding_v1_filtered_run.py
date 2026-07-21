from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    source = pd.read_parquet(args.source)
    if "holding_days" in source.columns:
        source = source[source["holding_days"].eq(3)].copy()
    duplicate_count = int(source.duplicated(["code", "ymd", "bar_index"]).sum())
    if duplicate_count:
        raise RuntimeError(f"duplicate source events: {duplicate_count}")

    with tempfile.TemporaryDirectory(prefix="tradex-long-variable-") as temporary:
        filtered = Path(temporary) / "unique_source.parquet"
        source.to_parquet(filtered, index=False)
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("tradex_long_honmei_variable_holding_v1.py")),
                "--source",
                str(filtered),
                "--output",
                args.output,
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
