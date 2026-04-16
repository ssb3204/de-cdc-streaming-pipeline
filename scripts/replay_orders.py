"""
replay_orders.py — Event Replayer for CDC pipeline (ADR-003, ADR-008)

목적: orders_future_30 CSV를 order_purchase_timestamp 기준 정렬 후
     시간 비례 압축해서 MySQL에 순차 INSERT, Debezium CDC 이벤트 스트림 생성.

사용법:
    python scripts/replay_orders.py --dry-run
    python scripts/replay_orders.py --limit 10
    python scripts/replay_orders.py --duration-minutes 10
    python scripts/replay_orders.py --include-updates --update-ratio 0.2
"""
import argparse
import glob
import logging
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pymysql  # pymysql.err.IntegrityError 용
from sqlalchemy import create_engine

try:
    from dotenv import load_dotenv
    _ROOT = Path(__file__).parent.parent
    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

# ---------------------------------------------------------------------------
# 기본값 상수
# ---------------------------------------------------------------------------
DEFAULT_DURATION_MINUTES = 10
DEFAULT_UPDATE_RATIO = 0.1
DEFAULT_UPDATE_DELAY_MIN = 1.0
DEFAULT_UPDATE_DELAY_MAX = 3.0

_ROOT = Path(__file__).parent.parent
_DATA_GLOB = str(_ROOT / "spark-submit" / "data" / "orders_future_30" / "*.csv")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ReplayConfig:
    csv_path: str
    duration_minutes: Optional[float]
    compression_ratio: Optional[float]
    limit: Optional[int]
    include_updates: bool
    update_ratio: float
    update_delay_min: float
    update_delay_max: float
    dry_run: bool
    log_level: str


def parse_args() -> ReplayConfig:
    parser = argparse.ArgumentParser(
        description="Event Replayer: orders_future_30 → MySQL (time-compressed CDC stream)"
    )
    parser.add_argument("--csv", default=None, help="CSV 파일 경로 (기본: spark-submit/data/orders_future_30/*.csv)")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--duration-minutes", type=float, default=None,
                       help=f"총 재생 시간(분). 기본값 {DEFAULT_DURATION_MINUTES}분")
    group.add_argument("--compression-ratio", type=float, default=None,
                       help="고정 압축 비율 (--duration-minutes와 상호 배타)")

    parser.add_argument("--limit", type=int, default=None, help="처음 N건만 처리 (테스트용)")
    parser.add_argument("--include-updates", action="store_true", help="UPDATE 이벤트 주입 활성화")
    parser.add_argument("--update-ratio", type=float, default=DEFAULT_UPDATE_RATIO,
                        help=f"UPDATE 주입 비율 (기본 {DEFAULT_UPDATE_RATIO})")
    parser.add_argument("--update-delay-range", default=f"{DEFAULT_UPDATE_DELAY_MIN},{DEFAULT_UPDATE_DELAY_MAX}",
                        help="UPDATE 지연 범위(초), 'MIN,MAX' 형식 (기본 1,3)")
    parser.add_argument("--dry-run", action="store_true", help="계획만 출력, DB 접속 없음")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="로그 레벨")

    args = parser.parse_args()

    delay_parts = args.update_delay_range.split(",")
    if len(delay_parts) != 2:
        parser.error("--update-delay-range 형식: 'MIN,MAX' (예: 1,3)")
    delay_min, delay_max = float(delay_parts[0]), float(delay_parts[1])

    csv_path = args.csv
    if csv_path is None:
        matches = glob.glob(_DATA_GLOB)
        if not matches:
            parser.error(f"CSV 파일을 찾을 수 없습니다: {_DATA_GLOB}")
        csv_path = matches[0]

    return ReplayConfig(
        csv_path=csv_path,
        duration_minutes=args.duration_minutes,
        compression_ratio=args.compression_ratio,
        limit=args.limit,
        include_updates=args.include_updates,
        update_ratio=args.update_ratio,
        update_delay_min=delay_min,
        update_delay_max=delay_max,
        dry_run=args.dry_run,
        log_level=args.log_level,
    )


# ---------------------------------------------------------------------------
# 데이터 로드
# ---------------------------------------------------------------------------
def load_events(cfg: ReplayConfig) -> pd.DataFrame:
    logging.info("CSV 로드 중: %s", cfg.csv_path)
    df = pd.read_csv(
        cfg.csv_path,
        parse_dates=["order_purchase_timestamp"],
    )
    df = df.sort_values("order_purchase_timestamp").reset_index(drop=True)
    df = df.dropna(subset=["order_purchase_timestamp"])

    if cfg.limit is not None:
        df = df.iloc[: cfg.limit]
        logging.info("--limit %d 적용: %d건으로 제한", cfg.limit, len(df))

    return df


# ---------------------------------------------------------------------------
# 압축 비율 계산
# ---------------------------------------------------------------------------
def compute_ratio(df: pd.DataFrame, cfg: ReplayConfig) -> float:
    if cfg.compression_ratio is not None:
        return cfg.compression_ratio

    ts = df["order_purchase_timestamp"]
    span_seconds = (ts.iloc[-1] - ts.iloc[0]).total_seconds()
    if span_seconds <= 0:
        raise ValueError("timestamp 범위가 0초 — 압축 비율을 계산할 수 없습니다.")

    minutes = cfg.duration_minutes if cfg.duration_minutes is not None else DEFAULT_DURATION_MINUTES
    target_seconds = minutes * 60
    return span_seconds / target_seconds


# ---------------------------------------------------------------------------
# 계획 출력 (dry-run)
# ---------------------------------------------------------------------------
def print_plan(df: pd.DataFrame, ratio: float, cfg: ReplayConfig) -> None:
    ts = df["order_purchase_timestamp"]
    span_seconds = (ts.iloc[-1] - ts.iloc[0]).total_seconds()
    planned_seconds = span_seconds / ratio
    avg_rate = len(df) / planned_seconds if planned_seconds > 0 else 0

    print()
    print("=" * 60)
    print("  Event Replayer: 실행 계획 (dry-run)")
    print("=" * 60)
    print(f"  CSV 파일      : {cfg.csv_path}")
    print(f"  이벤트 수     : {len(df):,}건")
    print(f"  원본 span     : {span_seconds / 86400:.1f}일 ({span_seconds:,.0f}초)")
    print(f"  압축 비율     : {ratio:,.0f}x")
    print(f"  계획 재생시간 : {planned_seconds / 60:.1f}분 ({planned_seconds:,.0f}초)")
    print(f"  평균 속도     : {avg_rate:.1f} events/s")
    print(f"  첫 timestamp  : {ts.iloc[0]}")
    print(f"  마지막 ts     : {ts.iloc[-1]}")
    print(f"  UPDATE 주입   : {'ON' if cfg.include_updates else 'OFF'}")
    if cfg.include_updates:
        est_updates = int(len(df) * cfg.update_ratio)
        print(f"  예상 UPDATE   : ~{est_updates:,}건 (ratio {cfg.update_ratio:.0%})")
    print("=" * 60)
    print()


# ---------------------------------------------------------------------------
# DB 연결 (load_data.py 와 동일한 SQLAlchemy mysql+pymysql URL 방식 — ADR-008 결정2)
# Windows Kerberos 환경에서 pymysql.connect() 직접 호출 시 auth_gssapi_client 오류 우회
# ---------------------------------------------------------------------------
def make_connection():
    url = (
        f"mysql+pymysql://{os.environ['MYSQL_USER']}:{os.environ['MYSQL_PASSWORD']}"
        f"@{os.environ.get('DB_HOST', '127.0.0.1')}:{os.environ.get('DB_PORT', '3306')}"
        f"/{os.environ.get('MYSQL_DATABASE', 'ecommerce')}?charset=utf8mb4"
    )
    engine = create_engine(url)
    conn = engine.raw_connection()
    conn.autocommit(True)  # ADR-008 결정2: 건당 즉시 commit
    return conn


# ---------------------------------------------------------------------------
# INSERT 루프 (시간 압축)
# ---------------------------------------------------------------------------
_INSERT_SQL = """
INSERT IGNORE INTO orders (
    order_id, customer_id, order_status,
    order_purchase_timestamp, order_approved_at,
    order_delivered_carrier_date, order_delivered_customer_date,
    order_estimated_delivery_date
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

_UPDATE_SQL = """
UPDATE orders
SET order_status = %s,
    order_delivered_customer_date = %s
WHERE order_id = %s
"""

_CANCEL_SQL = """
UPDATE orders
SET order_status = 'canceled'
WHERE order_id = %s
"""

_STATUS_DELIVERED = "delivered"
_STATUS_CANCELED  = "canceled"
_STATUS_INITIAL   = "invoiced"   # UPDATE 주입 시 첫 INSERT는 이 상태로
_STATUS_PRE_CANCEL = "shipped"   # canceled 전이용 초기 상태


def _to_mysql_dt(val) -> Optional[str]:
    """pandas Timestamp / NaT / None → MySQL DATETIME 문자열 또는 None."""
    if val is None or (hasattr(val, '__class__') and val.__class__.__name__ == 'NaTType'):
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    ts = pd.Timestamp(val)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def run_replay(df: pd.DataFrame, ratio: float, cfg: ReplayConfig) -> None:
    inserted = 0
    skipped = 0
    updates_sent = 0
    pending_updates: list[tuple[float, str, Optional[str], bool]] = []  # (fire_at, order_id, delivered_date, is_cancel)

    ts_col = df["order_purchase_timestamp"]
    start_wall = time.monotonic()
    planned_seconds = (ts_col.iloc[-1] - ts_col.iloc[0]).total_seconds() / ratio

    logging.info("INSERT 루프 시작: %d건, 계획 %.1f분", len(df), planned_seconds / 60)

    conn = make_connection()
    try:
        cursor = conn.cursor()
        try:
            prev_ts = ts_col.iloc[0]

            for i, row in enumerate(df.itertuples(index=False), start=1):
                cur_ts = row.order_purchase_timestamp

                # 이벤트 간 시간 간격만큼 sleep (압축 비율 적용)
                gap = (cur_ts - prev_ts).total_seconds() / ratio
                if gap > 0:
                    # 대기 중인 UPDATE 먼저 발행 (O(n) — list comprehension)
                    now = time.monotonic()
                    remaining: list[tuple[float, str, Optional[str], bool]] = []
                    for upd in pending_updates:
                        fire_at, oid, ddate, is_cancel = upd
                        if now >= fire_at:
                            try:
                                if is_cancel:
                                    cursor.execute(_CANCEL_SQL, (oid,))
                                    updates_sent += 1
                                    logging.debug("UPDATE sent: %s -> canceled", oid)
                                else:
                                    cursor.execute(_UPDATE_SQL, (_STATUS_DELIVERED, ddate, oid))
                                    updates_sent += 1
                                    logging.debug("UPDATE sent: %s -> delivered", oid)
                            except Exception as e:
                                logging.warning("UPDATE 실패 %s: %s", oid, e)
                        else:
                            remaining.append(upd)
                    pending_updates = remaining

                    time.sleep(gap)

                prev_ts = cur_ts

                # INSERT 상태 결정 (UPDATE 주입 활성화 시 일부는 초기 상태로 시작)
                inject_update = (
                    cfg.include_updates
                    and row.order_status == _STATUS_DELIVERED
                    and random.random() < cfg.update_ratio
                )
                # canceled 주문은 100% UPDATE 전이 (shipped → canceled)
                inject_cancel = (
                    cfg.include_updates
                    and row.order_status == _STATUS_CANCELED
                )
                if inject_cancel:
                    insert_status = _STATUS_PRE_CANCEL
                elif inject_update:
                    insert_status = _STATUS_INITIAL
                else:
                    insert_status = row.order_status
                delivered_date = _to_mysql_dt(getattr(row, "order_delivered_customer_date", None))

                try:
                    affected = cursor.execute(
                        _INSERT_SQL,
                        (
                            row.order_id,
                            row.customer_id,
                            insert_status,
                            _to_mysql_dt(row.order_purchase_timestamp),
                            _to_mysql_dt(getattr(row, "order_approved_at", None)),
                            _to_mysql_dt(getattr(row, "order_delivered_carrier_date", None)),
                            delivered_date,
                            _to_mysql_dt(getattr(row, "order_estimated_delivery_date", None)),
                        ),
                    )
                    if affected == 1:
                        inserted += 1
                        if inject_cancel:
                            delay = random.uniform(cfg.update_delay_min, cfg.update_delay_max)
                            pending_updates.append((time.monotonic() + delay, row.order_id, None, True))
                        elif inject_update:
                            delay = random.uniform(cfg.update_delay_min, cfg.update_delay_max)
                            pending_updates.append((time.monotonic() + delay, row.order_id, delivered_date, False))
                    else:
                        skipped += 1
                        logging.warning("SKIP (중복 PK): %s", row.order_id)
                except pymysql.err.IntegrityError as e:
                    skipped += 1
                    logging.warning("SKIP (FK/제약 위반) %s: %s", row.order_id, e)

                if i % 500 == 0 or i == len(df):
                    elapsed = time.monotonic() - start_wall
                    logging.info(
                        "[%d/%d] inserted=%d skipped=%d updates=%d elapsed=%.0fs",
                        i, len(df), inserted, skipped, updates_sent, elapsed,
                    )

            # 루프 종료 후 잔여 UPDATE 발행
            if pending_updates:
                logging.info("잔여 UPDATE %d건 발행 중...", len(pending_updates))
                for _, oid, ddate, is_cancel in pending_updates:
                    try:
                        if is_cancel:
                            cursor.execute(_CANCEL_SQL, (oid,))
                        else:
                            cursor.execute(_UPDATE_SQL, (_STATUS_DELIVERED, ddate, oid))
                        updates_sent += 1
                    except Exception as e:
                        logging.warning("UPDATE 실패 %s: %s", oid, e)

        finally:
            cursor.close()
    finally:
        conn.close()

    elapsed = time.monotonic() - start_wall
    drift_pct = (elapsed - planned_seconds) / planned_seconds * 100 if planned_seconds > 0 else 0

    print()
    print("=" * 60)
    print("  완료 요약")
    print("=" * 60)
    print(f"  inserted      : {inserted:,}")
    print(f"  skipped       : {skipped:,}")
    print(f"  updates_sent  : {updates_sent:,}")
    print(f"  planned       : {planned_seconds:.0f}s ({planned_seconds/60:.1f}min)")
    print(f"  actual        : {elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"  drift         : {drift_pct:+.1f}%")
    if abs(drift_pct) > 10:
        print("  [WARN] drift > 10%: --duration-minutes 를 늘리거나 --limit 줄이세요")
    print("=" * 60)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    cfg = parse_args()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    df = load_events(cfg)
    ratio = compute_ratio(df, cfg)

    print_plan(df, ratio, cfg)

    if cfg.dry_run:
        logging.info("--dry-run: 여기서 종료합니다.")
        return

    run_replay(df, ratio, cfg)


if __name__ == "__main__":
    main()
