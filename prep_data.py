"""
prep_data.py
────────────
Converts the raw Kaggle CSV into a filtered Parquet file of tech tickers.
Run once before opening the Malloy notebook.

Usage:
    python prep_data.py [--csv path/to/all_stocks_5yr.csv]
"""

import argparse
import pathlib
import sys

try:
    import duckdb
except ImportError:
    sys.exit("Install duckdb first:  pip install duckdb")

# ── Tech tickers to keep ─────────────────────────────────────────────────────
TECH_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG",
    "AMZN", "AMD",  "INTC", "ORCL",  "NFLX",
    "ADBE", "CRM",  "QCOM", "AVGO",  "TXN",
    "MU",   "PYPL",
]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        default="data/all_stocks_5yr.csv",
        help="Path to the raw Kaggle CSV (default: data/all_stocks_5yr.csv)",
    )
    args = parser.parse_args()

    csv_path = pathlib.Path(args.csv)
    if not csv_path.exists():
        sys.exit(
            f"\n  CSV not found: {csv_path}\n"
            "  Download from:\n"
            "  https://www.kaggle.com/datasets/jakewright/"
            "9000-tickers-of-stock-market-data-full-history\n"
            "  then place it at data/all_stocks_5yr.csv\n"
        )

    out_path = pathlib.Path("data/tech_stocks.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tickers_sql = ", ".join(f"'{t}'" for t in TECH_TICKERS)

    print(f"Reading  {csv_path} ...")
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            SELECT
                CAST(date   AS DATE)   AS date,
                CAST(open   AS DOUBLE) AS open,
                CAST(high   AS DOUBLE) AS high,
                CAST(low    AS DOUBLE) AS low,
                CAST(close  AS DOUBLE) AS close,
                CAST(volume AS BIGINT) AS volume,
                Name                   AS ticker
            FROM '{csv_path}'
            WHERE Name IN ({tickers_sql})
            ORDER BY Name, date
        )
        TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    stats = conn.execute(f"""
        SELECT
            COUNT(*)              AS rows,
            COUNT(DISTINCT ticker) AS tickers,
            MIN(date)             AS date_min,
            MAX(date)             AS date_max
        FROM '{out_path}'
    """).fetchone()

    print(f"\n  Wrote  {out_path}")
    print(f"  Rows   {stats[0]:,}")
    print(f"  Tickers {stats[1]}")
    print(f"  Range  {stats[2]}  →  {stats[3]}")
    print("\n  Done! Open tech_stocks.malloynb in VS Code.\n")


if __name__ == "__main__":
    main()
