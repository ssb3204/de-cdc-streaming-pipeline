"""
measure_latency.py — End-to-end latency 측정 (Phase 4, ADR-010)

MySQL 변경 발생 시각(ts_ms) → Spark Parquet 저장 시각(processed_at) 차이 계산.

사용법:
    python scripts/measure_latency.py
    python scripts/measure_latency.py --topic orders
    python scripts/measure_latency.py --op c
    python scripts/measure_latency.py --raw         # 개별 레코드 출력
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).parent.parent
PARQUET_DIR = _ROOT / "data" / "cdc_output"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CDC 파이프라인 end-to-end latency 측정"
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="필터링할 테이블 이름 (orders / order_items / customers / products)",
    )
    parser.add_argument(
        "--op",
        default=None,
        choices=["c", "u", "d"],
        help="이벤트 타입 필터 (c=insert, u=update, d=delete)",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="집계 대신 레코드별 latency 출력",
    )
    return parser.parse_args()


def load_parquet(parquet_dir: Path) -> pd.DataFrame:
    if not parquet_dir.exists():
        print(f"[ERROR] Parquet 디렉토리 없음: {parquet_dir}", file=sys.stderr)
        print("  Spark 스트리밍 잡을 먼저 실행하세요.", file=sys.stderr)
        sys.exit(1)

    try:
        df = pd.read_parquet(parquet_dir)
    except Exception as e:
        print(f"[ERROR] Parquet 읽기 실패: {e}", file=sys.stderr)
        sys.exit(1)

    return df


def compute_latency(df: pd.DataFrame) -> pd.DataFrame:
    """processed_at이 있는 레코드에서 latency_ms 계산."""
    if "processed_at" not in df.columns:
        print(
            "[ERROR] 'processed_at' 컬럼 없음.\n"
            "  stream_cdc.py에 processed_at 컬럼이 추가된 후 수집된 데이터가 필요합니다.\n"
            "  Spark 스트리밍 잡을 재시작 후 이벤트를 주입하세요.",
            file=sys.stderr,
        )
        sys.exit(1)

    # processed_at이 없는 기존 레코드 제외
    df = df.dropna(subset=["processed_at", "ts_ms"]).copy()

    if df.empty:
        print("[WARN] processed_at이 있는 레코드가 없습니다.", file=sys.stderr)
        print("  Spark 재시작 후 새 이벤트를 주입하면 측정 가능합니다.", file=sys.stderr)
        sys.exit(0)

    # ts_ms: Debezium이 기록한 MySQL 이벤트 발생 시각 (Unix ms)
    df["ts_ms"] = pd.to_numeric(df["ts_ms"], errors="coerce")
    df = df.dropna(subset=["ts_ms"])

    df["ts_ms_dt"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)

    # processed_at: Spark가 레코드를 처리한 시각
    if df["processed_at"].dt.tz is None:
        df["processed_at"] = df["processed_at"].dt.tz_localize("UTC")
    else:
        df["processed_at"] = df["processed_at"].dt.tz_convert("UTC")

    df["latency_ms"] = (df["processed_at"] - df["ts_ms_dt"]).dt.total_seconds() * 1000

    # 음수 latency 제거 (시계 스큐 또는 이전 데이터)
    df = df[df["latency_ms"] >= 0]

    return df


def print_stats(df: pd.DataFrame) -> None:
    def _stats(series: pd.Series) -> dict:
        return {
            "count": len(series),
            "p50_ms": series.quantile(0.50),
            "p95_ms": series.quantile(0.95),
            "p99_ms": series.quantile(0.99),
            "max_ms": series.max(),
            "mean_ms": series.mean(),
        }

    print()
    print("=" * 62)
    print("  CDC 파이프라인 End-to-End Latency 측정 결과")
    print("  (MySQL 이벤트 발생 → Spark Parquet 저장)")
    print("=" * 62)

    # 전체 통계
    overall = _stats(df["latency_ms"])
    print(f"\n[전체 {overall['count']:,}건]")
    _print_row(overall)

    # topic별 통계
    if "topic" in df.columns:
        print("\n[테이블별]")
        for topic, grp in df.groupby("topic", observed=True):
            table = topic.split(".")[-1] if "." in topic else topic
            st = _stats(grp["latency_ms"])
            print(f"  {table:<18} n={st['count']:>5,}", end="")
            _print_row(st)

    # op별 통계
    if "op" in df.columns:
        op_labels = {"c": "INSERT", "u": "UPDATE", "d": "DELETE"}
        print("\n[이벤트 타입별]")
        for op, grp in df.groupby("op", observed=True):
            st = _stats(grp["latency_ms"])
            label = op_labels.get(op, op)
            print(f"  {label:<8} n={st['count']:>5,}", end="")
            _print_row(st)

    print()
    print("=" * 62)
    print()


def _print_row(st: dict) -> None:
    print(
        f"  p50={st['p50_ms']:>7.0f}ms"
        f"  p95={st['p95_ms']:>7.0f}ms"
        f"  p99={st['p99_ms']:>7.0f}ms"
        f"  max={st['max_ms']:>7.0f}ms"
    )


def print_raw(df: pd.DataFrame) -> None:
    cols = ["topic", "op", "ts_ms_dt", "processed_at", "latency_ms"]
    available = [c for c in cols if c in df.columns]
    display = df[available].sort_values("latency_ms", ascending=False).head(50)
    display = display.copy()
    if "topic" in display.columns:
        display["topic"] = display["topic"].str.split(".").str[-1]
    print(display.to_string(index=False))


def main() -> None:
    args = parse_args()

    df = load_parquet(PARQUET_DIR)
    df = compute_latency(df)

    # 필터 적용
    if args.topic:
        mask = df["topic"].str.contains(args.topic, na=False)
        df = df[mask]
        if df.empty:
            print(f"[WARN] '{args.topic}' 토픽 레코드 없음")
            sys.exit(0)

    if args.op:
        df = df[df["op"] == args.op]
        if df.empty:
            print(f"[WARN] op='{args.op}' 레코드 없음")
            sys.exit(0)

    if args.raw:
        print_raw(df)
    else:
        print_stats(df)


if __name__ == "__main__":
    main()
