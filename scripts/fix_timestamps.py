"""
fix_timestamps.py — orders.order_purchase_timestamp null 복구

문제:
  load_data.py가 CSV의 ISO 8601 "Z" suffix datetime을 UTC-aware Timestamp로 파싱,
  pymysql이 MySQL DATETIME에 삽입 시 null로 저장.

해결:
  orders_initial_70 CSV에서 order_id 기준으로 timestamp를 읽어 UPDATE.
  timezone strip 후 적재.

사용법:
  python scripts/fix_timestamps.py
  python scripts/fix_timestamps.py --dry-run  # 실제 UPDATE 없이 확인만
"""

import argparse
import glob
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    _ROOT = Path(__file__).parent.parent
    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

import pandas as pd
import pymysql

DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ.get("MYSQL_DATABASE", "ecommerce")
DB_USER = os.environ.get("MYSQL_USER", "appuser")
DB_PASS = os.environ.get("MYSQL_PASSWORD", "apppass")

DATA_DIR = Path(__file__).parent.parent / "spark-submit" / "data"
ORDERS_DIR = DATA_DIR / "orders_initial_70"

TIMESTAMP_COLS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def load_csv() -> pd.DataFrame:
    parts = sorted(ORDERS_DIR.glob("part-*.csv"))
    if not parts:
        print(f"[ERROR] no part-*.csv in {ORDERS_DIR}")
        sys.exit(1)
    csv_path = parts[0]
    print(f"[info] reading {csv_path}")
    df = pd.read_csv(csv_path, parse_dates=TIMESTAMP_COLS)
    # Strip timezone
    for col in TIMESTAMP_COLS:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            if df[col].dt.tz is not None:
                df[col] = df[col].dt.tz_convert(None)
    print(f"[info] loaded {len(df):,} rows from CSV")
    return df


def fix_timestamps(df: pd.DataFrame, dry_run: bool) -> None:
    conn = pymysql.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )
    updated = 0
    skipped = 0

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM orders WHERE order_purchase_timestamp IS NULL")
            null_before = cur.fetchone()["cnt"]
        print(f"[info] null timestamps before fix: {null_before:,}")

        if dry_run:
            print("[dry-run] would UPDATE timestamps -- skipping actual writes")
            print(f"[dry-run] rows to fix: {len(df):,}")
            return

        chunksize = 500
        for i in range(0, len(df), chunksize):
            chunk = df.iloc[i:i + chunksize]
            with conn.cursor() as cur:
                for _, row in chunk.iterrows():
                    vals = {col: (None if pd.isna(row[col]) else row[col].isoformat())
                            for col in TIMESTAMP_COLS if col in row}
                    set_parts = ", ".join(f"{c} = %s" for c in vals)
                    sql = f"UPDATE orders SET {set_parts} WHERE order_id = %s"
                    params = list(vals.values()) + [row["order_id"]]
                    n = cur.execute(sql, params)
                    if n > 0:
                        updated += 1
                    else:
                        skipped += 1
            conn.commit()
            print(f"[info] processed {min(i + chunksize, len(df)):,}/{len(df):,} rows", end="\r")

        print()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM orders WHERE order_purchase_timestamp IS NULL")
            null_after = cur.fetchone()["cnt"]

        print(f"[info] updated: {updated:,}, skipped (not in DB): {skipped:,}")
        print(f"[info] null timestamps after fix: {null_after:,}")
        if null_after == 0:
            print("[PASS] all timestamps restored")
        else:
            print(f"[WARN] {null_after:,} nulls remain")

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix orders timestamp nulls")
    parser.add_argument("--dry-run", action="store_true",
                        help="print what would happen without writing")
    args = parser.parse_args()

    df = load_csv()
    fix_timestamps(df, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
