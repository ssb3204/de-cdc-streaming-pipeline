"""
check_dq.py — Phase 7: Data Quality 3-Layer Check

Layer 1 (MySQL Source):
  - PK null / 중복 체크
  - FK 무결성 체크
  - Row count (CSV 원본 대비)
  - 핵심 컬럼 null 비율

Layer 2 (Parquet CDC):
  - 이벤트 op 분포
  - order_id 중복 이벤트 (dedup 관점)
  - 스키마 컬럼 존재 여부

Layer 3 (Consistency):
  - MySQL orders row count vs Parquet 이벤트 수 비교

사용법:
  python scripts/check_dq.py            # 전체 실행
  python scripts/check_dq.py --layer 1  # Layer 1만
  python scripts/check_dq.py --layer 2  # Layer 2만
  python scripts/check_dq.py --layer 3  # Layer 3만
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    _ROOT = Path(__file__).parent.parent
    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────

DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ.get("MYSQL_DATABASE", "ecommerce")
DB_USER = os.environ.get("MYSQL_USER", "appuser")
DB_PASS = os.environ.get("MYSQL_PASSWORD", "apppass")

PARQUET_BASE = Path(__file__).parent.parent / "data" / "cdc_output"
ORDERS_PARQUET = PARQUET_BASE / "topic=ecommerce.ecommerce.orders"

# CSV 원본 row 수 (spark split 전 olist 전체 기준)
EXPECTED_ROWS = {
    "orders": 99441,
    "customers": 99441,
    "order_items": 112650,
    "products": 32951,
}

# 핵심 null 체크 컬럼 (컬럼명: 허용 null 비율 %)
NULL_THRESHOLDS: dict[str, float] = {
    "orders.order_purchase_timestamp": 5.0,
    "orders.order_status": 1.0,
    "orders.customer_id": 0.0,
    "order_items.order_id": 0.0,
    "order_items.product_id": 0.0,
    "order_items.price": 1.0,
}

# 행 수 허용 오차
INITIAL_LOAD_RATIO: float = 0.7
ROW_COUNT_TOLERANCE_PCT: float = 0.02
ROW_COUNT_MIN_TOLERANCE: int = 10

# consistency 커버리지 임계값
MIN_COVERAGE_PCT: float = 95.0

SEVERITY_PASS  = "[PASS]"
SEVERITY_WARN  = "[WARN]"
SEVERITY_FAIL  = "[FAIL]"
SEVERITY_SKIP  = "[SKIP]"
SEVERITY_INFO  = "[INFO]"

findings: list[dict] = []

# SQL identifier allowlist — 테이블/컬럼명 f-string 삽입 전 반드시 통과
_ALLOWED_IDENTIFIERS: frozenset[str] = frozenset({
    # tables
    "orders", "customers", "order_items", "products",
    # columns
    "order_id", "customer_id", "product_id", "order_item_id",
    "order_purchase_timestamp", "order_status", "price",
})


def _safe(name: str) -> str:
    """SQL identifier allowlist 검증 — 허용 목록에 없으면 즉시 예외."""
    if name not in _ALLOWED_IDENTIFIERS:
        raise ValueError(f"Disallowed SQL identifier: {name!r}")
    return name


def record(severity: str, check: str, detail: str) -> None:
    findings.append({"severity": severity, "check": check, "detail": detail})
    tag = {
        SEVERITY_FAIL: "CRITICAL",
        SEVERITY_WARN: "HIGH",
        SEVERITY_PASS: "OK",
        SEVERITY_SKIP: "SKIP",
        SEVERITY_INFO: "INFO",
    }.get(severity, "?")
    logging.info("  %s [%s] %s: %s", severity, tag, check, detail)


# ─────────────────────────────────────────────
# DB 연결
# ─────────────────────────────────────────────

def get_connection() -> Any:
    try:
        import pymysql
    except ImportError:
        logging.error("pymysql not installed -- pip install pymysql")
        sys.exit(1)

    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )


def query(conn: Any, sql: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def scalar(conn: Any, sql: str) -> Any:
    rows = query(conn, sql)
    if rows:
        return list(rows[0].values())[0]
    return None


# ─────────────────────────────────────────────
# Layer 1: MySQL Source DQ — 서브함수
# ─────────────────────────────────────────────

def _check_row_counts(conn: Any) -> None:
    """1-A. Row count (vs CSV 원본의 70%)"""
    logging.info("\n-- 1-A. Row count --")
    tables = ["orders", "customers", "order_items", "products"]
    for tbl in tables:
        cnt = scalar(conn, f"SELECT COUNT(*) FROM {_safe(tbl)}")
        expected_full = EXPECTED_ROWS[tbl]
        expected_70 = int(expected_full * INITIAL_LOAD_RATIO)
        expected = expected_70 if tbl in ("orders", "order_items") else expected_full
        diff = abs(cnt - expected)
        tol = max(int(expected * ROW_COUNT_TOLERANCE_PCT), ROW_COUNT_MIN_TOLERANCE)
        sev = SEVERITY_PASS if diff <= tol else SEVERITY_WARN
        record(sev, f"{tbl}.row_count",
               f"{cnt:,} (expected ~{expected:,}, diff={diff:,})")


def _check_pk_integrity(conn: Any) -> None:
    """1-B. PK null / 중복"""
    logging.info("\n-- 1-B. PK null / duplicate --")
    pk_map = {
        "orders": "order_id",
        "customers": "customer_id",
        "products": "product_id",
    }
    for tbl, pk in pk_map.items():
        null_cnt = scalar(conn,
            f"SELECT COUNT(*) FROM {_safe(tbl)} WHERE {_safe(pk)} IS NULL")
        dup_cnt = scalar(conn,
            f"SELECT COUNT(*) FROM "
            f"(SELECT {_safe(pk)} FROM {_safe(tbl)} GROUP BY {_safe(pk)} HAVING COUNT(*) > 1) t")
        record(SEVERITY_PASS if null_cnt == 0 else SEVERITY_FAIL,
               f"{tbl}.{pk}_null", f"{null_cnt}")
        record(SEVERITY_PASS if dup_cnt == 0 else SEVERITY_FAIL,
               f"{tbl}.{pk}_duplicate", f"{dup_cnt}")

    # order_items는 복합PK (order_id + order_item_id)
    dup_items = scalar(conn,
        "SELECT COUNT(*) FROM ("
        "  SELECT order_id, order_item_id FROM order_items"
        "  GROUP BY order_id, order_item_id HAVING COUNT(*) > 1"
        ") t")
    record(
        SEVERITY_PASS if dup_items == 0 else SEVERITY_FAIL,
        "order_items.composite_pk_duplicate", f"{dup_items}"
    )


def _check_fk_integrity(conn: Any) -> None:
    """1-C. FK 무결성"""
    logging.info("\n-- 1-C. FK integrity --")
    orphan_orders = scalar(conn,
        "SELECT COUNT(*) FROM orders o"
        "  LEFT JOIN customers c ON o.customer_id = c.customer_id"
        "  WHERE c.customer_id IS NULL")
    orphan_items = scalar(conn,
        "SELECT COUNT(*) FROM order_items oi"
        "  LEFT JOIN orders o ON oi.order_id = o.order_id"
        "  WHERE o.order_id IS NULL")
    record(
        SEVERITY_PASS if orphan_orders == 0 else SEVERITY_FAIL,
        "orders.customer_id FK orphan", f"{orphan_orders}"
    )
    record(
        SEVERITY_PASS if orphan_items == 0 else SEVERITY_FAIL,
        "order_items.order_id FK orphan", f"{orphan_items}"
    )


def _check_null_rates(conn: Any) -> None:
    """1-D. 핵심 컬럼 null 비율 — NULL_THRESHOLDS 단일 소스"""
    logging.info("\n-- 1-D. Critical null rates --")
    null_checks = [
        (k.split(".")[0], k.split(".")[1], v)
        for k, v in NULL_THRESHOLDS.items()
    ]
    for tbl, col, threshold in null_checks:
        total = scalar(conn, f"SELECT COUNT(*) FROM {_safe(tbl)}")
        null_cnt = scalar(conn,
            f"SELECT COUNT(*) FROM {_safe(tbl)} WHERE {_safe(col)} IS NULL")
        pct = (null_cnt / total * 100) if total else 0.0
        sev = SEVERITY_PASS if pct <= threshold else SEVERITY_FAIL
        note = ""
        if tbl == "orders" and col == "order_purchase_timestamp" and pct > 5:
            note = " [known issue: UTC-aware datetime -> MySQL DATETIME incompatibility in load_data.py]"
        record(sev, f"{tbl}.{col}_null_rate",
               f"{pct:.1f}% ({null_cnt:,}/{total:,}){note}")


def layer1_mysql(conn: Any) -> None:
    logging.info("\n" + "=" * 55)
    logging.info("[ Layer 1: MySQL Source DQ ]")
    logging.info("=" * 55)

    _check_row_counts(conn)
    _check_pk_integrity(conn)
    _check_fk_integrity(conn)
    _check_null_rates(conn)


# ─────────────────────────────────────────────
# Layer 2: Parquet CDC DQ
# ─────────────────────────────────────────────

def _parse_json_field(val: Any, field: str) -> Any:
    """after/before JSON 문자열에서 특정 필드 값 추출. 파싱 실패 시 None 반환."""
    if val is None:
        return None
    try:
        return json.loads(val).get(field)
    except Exception as e:
        logging.debug("JSON parse error for value %r: %s", val, e)
        return None


def _json_parse_ok(val: Any) -> bool:
    """after JSON 파싱 성공 여부. None은 파싱 대상이 아니므로 True."""
    if val is None:
        return True
    try:
        json.loads(val)
        return True
    except Exception as e:
        logging.debug("JSON parse error for value %r: %s", val, e)
        return False


def _has_field_in_json(val: Any, field: str) -> bool:
    """JSON 문자열에 특정 필드가 존재하는지 확인."""
    try:
        return field in json.loads(val) if val else False
    except Exception as e:
        logging.debug("JSON parse error for value %r: %s", val, e)
        return False


def layer2_parquet() -> None:
    logging.info("\n" + "=" * 55)
    logging.info("[ Layer 2: Parquet CDC DQ ]")
    logging.info("=" * 55)

    try:
        import pandas as pd
    except ImportError:
        logging.warning("pandas/pyarrow not installed -- skipping Layer 2")
        return

    if not ORDERS_PARQUET.exists():
        record(SEVERITY_SKIP, "parquet.orders", f"path not found: {ORDERS_PARQUET}")
        return

    parquet_files = list(ORDERS_PARQUET.rglob("*.parquet"))
    if not parquet_files:
        record(SEVERITY_SKIP, "parquet.orders", "no parquet files found")
        return

    df = pd.read_parquet(ORDERS_PARQUET)
    total = len(df)
    record(SEVERITY_INFO, "parquet.total_events", f"{total:,}")

    # 2-A. op 분포
    logging.info("\n-- 2-A. op distribution --")
    op_counts = df["op"].value_counts().to_dict()
    for op, cnt in sorted(op_counts.items()):
        label = {"r": "READ(snapshot)", "c": "CREATE", "u": "UPDATE", "d": "DELETE"}.get(op, op)
        record(SEVERITY_INFO, f"parquet.op={op}", f"{cnt:,} ({label})")

    # 2-B. order_id 필드 존재 + null 체크
    logging.info("\n-- 2-B. Required field null check --")
    insert_rows = df[df["op"].isin(["r", "c"])].copy()
    if not insert_rows.empty:
        null_order_id = insert_rows["after"].map(
            lambda v: _parse_json_field(v, "order_id")
        ).isna().sum()
        pct = null_order_id / len(insert_rows) * 100
        record(
            SEVERITY_PASS if null_order_id == 0 else SEVERITY_FAIL,
            "parquet.after.order_id_null",
            f"{null_order_id} ({pct:.1f}%)"
        )

    # 2-C. 스키마 컬럼 검증 (is_late_delivery)
    logging.info("\n-- 2-C. Schema evolution column check --")
    update_rows = df[df["op"] == "u"].copy()
    if update_rows.empty:
        record(SEVERITY_SKIP, "parquet.is_late_delivery", "no UPDATE events found")
    else:
        has_col = update_rows["after"].map(
            lambda v: _has_field_in_json(v, "is_late_delivery")
        ).sum()
        sev = SEVERITY_PASS if has_col > 0 else SEVERITY_WARN
        record(sev, "parquet.is_late_delivery",
               f"UPDATE {len(update_rows)} events, {has_col} contain is_late_delivery")

    # 2-D. after 필드 파싱 실패율
    logging.info("\n-- 2-D. after parse failure rate --")
    parse_fail = (~df["after"].map(_json_parse_ok)).sum()
    pct = parse_fail / total * 100 if total else 0
    record(
        SEVERITY_PASS if parse_fail == 0 else SEVERITY_WARN,
        "parquet.after_parse_fail",
        f"{parse_fail} ({pct:.2f}%)"
    )


# ─────────────────────────────────────────────
# Layer 3: MySQL vs Parquet Consistency
# ─────────────────────────────────────────────

def layer3_consistency(conn: Any) -> None:
    logging.info("\n" + "=" * 55)
    logging.info("[ Layer 3: MySQL vs Parquet Consistency ]")
    logging.info("=" * 55)

    try:
        import pandas as pd
    except ImportError:
        record(SEVERITY_SKIP, "consistency", "pandas/pyarrow not installed")
        return

    if not ORDERS_PARQUET.exists():
        record(SEVERITY_SKIP, "consistency.orders", f"parquet path not found: {ORDERS_PARQUET}")
        return

    parquet_files = list(ORDERS_PARQUET.rglob("*.parquet"))
    if not parquet_files:
        record(SEVERITY_SKIP, "consistency.orders", "no parquet files found")
        return

    df = pd.read_parquet(ORDERS_PARQUET)

    # 3-A. MySQL orders 수 vs Parquet 비 중복 order_id 수
    logging.info("\n-- 3-A. Unique order coverage --")
    mysql_orders = scalar(conn, "SELECT COUNT(DISTINCT order_id) FROM orders")

    ids_after = df["after"].map(lambda v: _parse_json_field(v, "order_id")).dropna()
    ids_before = (
        df["before"].map(lambda v: _parse_json_field(v, "order_id")).dropna()
        if "before" in df.columns else pd.Series([], dtype=str)
    )
    parquet_order_ids: set[str] = set(ids_after) | set(ids_before)
    parquet_unique = len(parquet_order_ids)

    # Note: initial bulk load (69,608 rows) bypassed CDC and went directly to MySQL.
    # Only events from replay_orders.py (~129) are in Parquet. Low coverage is expected.
    coverage_pct = (parquet_unique / mysql_orders * 100) if mysql_orders else 0.0
    sev = SEVERITY_PASS if coverage_pct >= MIN_COVERAGE_PCT else SEVERITY_WARN
    note = " [expected: bulk load bypassed CDC; only replay events in Parquet]" if coverage_pct < 5 else ""
    record(sev, "consistency.order_id_coverage",
           f"Parquet {parquet_unique:,} / MySQL {mysql_orders:,} ({coverage_pct:.1f}%){note}")

    # 3-B. CREATE vs INSERT 수 비교
    logging.info("\n-- 3-B. CREATE event count --")
    parquet_creates = len(df[df["op"].isin(["r", "c"])])
    mysql_total = scalar(conn, "SELECT COUNT(*) FROM orders")
    ratio = parquet_creates / mysql_total if mysql_total else 0
    sev = SEVERITY_PASS if 0.95 <= ratio <= 1.10 else SEVERITY_WARN
    note = " [expected: only replay events captured; bulk load not in CDC]" if ratio < 0.1 else ""
    record(sev, "consistency.create_vs_mysql",
           f"Parquet CREATE/READ={parquet_creates:,} vs MySQL rows={mysql_total:,} (ratio={ratio:.2f}){note}")

    # 3-C. UPDATE 이벤트 수 sanity check
    logging.info("\n-- 3-C. UPDATE event count --")
    parquet_updates = len(df[df["op"] == "u"])
    record(SEVERITY_INFO, "consistency.update_event_count", f"{parquet_updates:,}")


# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="Phase 7: Data Quality 3-Layer Check")
    parser.add_argument("--layer", type=int, choices=[1, 2, 3],
                        help="특정 레이어만 실행 (기본: 전체)")
    args = parser.parse_args()

    run_all = args.layer is None
    run_l1 = run_all or args.layer == 1
    run_l2 = run_all or args.layer == 2
    run_l3 = run_all or args.layer == 3

    conn = None
    if run_l1 or run_l3:
        conn = get_connection()

    try:
        if run_l1:
            layer1_mysql(conn)

        if run_l2:
            layer2_parquet()

        if run_l3:
            layer3_consistency(conn)

    finally:
        if conn:
            conn.close()

    # 최종 요약
    logging.info("\n" + "=" * 55)
    logging.info("[ DQ Summary ]")
    logging.info("=" * 55)
    fails  = [f for f in findings if f["severity"] == SEVERITY_FAIL]
    warns  = [f for f in findings if f["severity"] == SEVERITY_WARN]
    passes = [f for f in findings if f["severity"] == SEVERITY_PASS]

    logging.info("  PASS : %d", len(passes))
    logging.info("  WARN : %d", len(warns))
    logging.info("  FAIL : %d", len(fails))

    if fails:
        logging.info("\n[CRITICAL items]")
        for f in fails:
            logging.info("  - %s: %s", f["check"], f["detail"])

    if warns:
        logging.info("\n[WARNING items]")
        for f in warns:
            logging.info("  - %s: %s", f["check"], f["detail"])

    logging.info("")
    if fails:
        logging.info("[FAIL] DQ check failed -- review CRITICAL items above.")
        sys.exit(1)
    elif warns:
        logging.info("[WARN] DQ check warnings -- review before proceeding.")
        sys.exit(0)
    else:
        logging.info("[PASS] DQ check complete -- no issues found.")
        sys.exit(0)


if __name__ == "__main__":
    main()
