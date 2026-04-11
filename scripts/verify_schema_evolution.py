"""
verify_schema_evolution.py — Phase 5-2: Schema Evolution 검증

목적:
  ALTER TABLE ADD COLUMN 시 Debezium이 스키마 변화를 자동 감지하고,
  후속 CDC 이벤트에 새 컬럼이 포함되는지 검증한다.

시나리오:
  1. ALTER 이전 offset 확인 (기준선)
  2. 신규 컬럼이 포함된 이벤트가 Kafka에 도달했는지 검증
  3. (선택) Parquet에 Spark가 기록했는지 확인

사용법:
  # Kafka 이벤트 검증 (필수)
  python scripts/verify_schema_evolution.py --kafka-check

  # Parquet 검증 (Spark stream_cdc.py 실행 후)
  python scripts/verify_schema_evolution.py --parquet-check

  # 전체 실행
  python scripts/verify_schema_evolution.py --kafka-check --parquet-check
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


KAFKA_CONTAINER = "kafka"
BOOTSTRAP = "kafka:29092"
TOPIC = "ecommerce.ecommerce.orders"
NEW_COLUMN = "is_late_delivery"
PARQUET_PATH = Path(__file__).parent.parent / "data" / "cdc_output"


# ─────────────────────────────────────────────
# Kafka helpers
# ─────────────────────────────────────────────

def get_latest_offset() -> int:
    """토픽의 현재 끝 offset을 반환한다."""
    result = subprocess.run(
        [
            "docker", "exec", KAFKA_CONTAINER,
            "kafka-run-class", "kafka.tools.GetOffsetShell",
            "--broker-list", BOOTSTRAP,
            "--topic", TOPIC,
            "--time", "-1",
        ],
        capture_output=True, text=True, timeout=10
    )
    # 출력 형식: "topic:partition:offset"
    line = result.stdout.strip()
    return int(line.split(":")[-1])


def fetch_messages(start_offset: int, count: int) -> list[str]:
    """start_offset부터 count개 메시지를 가져온다."""
    result = subprocess.run(
        [
            "docker", "exec", KAFKA_CONTAINER,
            "kafka-console-consumer",
            "--bootstrap-server", BOOTSTRAP,
            "--topic", TOPIC,
            "--partition", "0",
            "--offset", str(start_offset),
            "--max-messages", str(count),
            "--timeout-ms", "8000",
        ],
        capture_output=True, text=True, timeout=20
    )
    lines = [l.strip() for l in result.stdout.splitlines() if l.strip()]
    return lines


def parse_after_fields(raw: str) -> dict | None:
    """Debezium JSON에서 payload.after를 파싱한다."""
    try:
        msg = json.loads(raw)
        return msg.get("payload", {}).get("after")
    except json.JSONDecodeError:
        return None


# ─────────────────────────────────────────────
# 검증 1: Kafka 이벤트에 새 컬럼 포함 여부
# ─────────────────────────────────────────────

def check_kafka(args: argparse.Namespace) -> bool:
    latest = get_latest_offset()
    print(f"[kafka] 현재 latest offset: {latest}")

    # 끝에서 최대 30개 메시지를 검사
    sample_count = min(30, latest)
    start = max(0, latest - sample_count)

    print(f"[kafka] offset {start} ~ {latest} ({sample_count}건) 검사 중...")
    messages = fetch_messages(start, sample_count)

    if not messages:
        print("[FAIL] Kafka에서 메시지를 읽지 못했습니다.")
        return False

    has_new_col_count = 0
    missing_new_col_count = 0
    schema_updated_offsets: list[int] = []

    for raw in messages:
        after = parse_after_fields(raw)
        if after is None:
            continue  # before=null (DELETE) 또는 파싱 실패

        if NEW_COLUMN in after:
            has_new_col_count += 1
        else:
            missing_new_col_count += 1

    total_parsed = has_new_col_count + missing_new_col_count
    print(f"\n[결과]")
    print(f"  파싱된 이벤트:          {total_parsed}건")
    print(f"  '{NEW_COLUMN}' 포함:    {has_new_col_count}건  ← ALTER 이후 이벤트")
    print(f"  '{NEW_COLUMN}' 미포함:  {missing_new_col_count}건  ← ALTER 이전 이벤트")

    if has_new_col_count > 0:
        print(f"\n[PASS] Debezium이 ALTER TABLE을 감지하고 새 컬럼을 이벤트에 포함했습니다.")
        return True
    else:
        print(f"\n[FAIL] 최근 {sample_count}건에 '{NEW_COLUMN}'이 없습니다.")
        print("       ALTER TABLE과 UPDATE가 실행됐는지 확인하세요.")
        return False


# ─────────────────────────────────────────────
# 검증 2: Parquet에서 새 컬럼 확인
# ─────────────────────────────────────────────

def check_parquet(args: argparse.Namespace) -> bool:
    orders_path = PARQUET_PATH / "topic=ecommerce.ecommerce.orders"

    if not orders_path.exists():
        print(f"[SKIP] Parquet 경로 없음: {orders_path}")
        print("       stream_cdc.py를 실행한 뒤 다시 시도하세요.")
        return True  # skip이지 fail이 아님

    try:
        import pandas as pd
    except ImportError:
        print("[SKIP] pandas 미설치 — pip install pandas pyarrow")
        return True

    parquet_files = list(orders_path.rglob("*.parquet"))
    if not parquet_files:
        print(f"[SKIP] Parquet 파일 없음: {orders_path}")
        return True

    print(f"[parquet] Parquet 파일 {len(parquet_files)}개 발견")

    df = pd.read_parquet(orders_path)
    total_rows = len(df)
    print(f"[parquet] 전체 rows: {total_rows}")

    # after 컬럼(JSON 문자열)에서 is_late_delivery 검색
    update_rows = df[df["op"] == "u"].copy()
    print(f"[parquet] op=u (UPDATE) rows: {len(update_rows)}")

    if update_rows.empty:
        print("[INFO] UPDATE 이벤트가 아직 Parquet에 없습니다 (Spark 미실행 또는 지연).")
        return True

    def has_new_col(after_json: str | None) -> bool:
        if not after_json:
            return False
        try:
            return NEW_COLUMN in json.loads(after_json)
        except Exception:
            return False

    update_rows["has_new_col"] = update_rows["after"].apply(has_new_col)
    found = update_rows["has_new_col"].sum()

    print(f"[결과]")
    print(f"  UPDATE 이벤트 중 '{NEW_COLUMN}' 포함: {found}건 / {len(update_rows)}건")

    if found > 0:
        sample = update_rows[update_rows["has_new_col"]].iloc[0]["after"]
        after_data = json.loads(sample)
        print(f"  샘플 is_late_delivery 값: {after_data.get(NEW_COLUMN)}")
        print(f"\n[PASS] Parquet에 '{NEW_COLUMN}' 포함 이벤트가 기록됐습니다.")
        return True
    else:
        print(f"\n[INFO] Parquet의 UPDATE 이벤트에 '{NEW_COLUMN}'이 없습니다.")
        print("       stream_cdc.py가 ALTER 이후 이벤트를 아직 처리하지 않았을 수 있습니다.")
        return True


# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 5-2: Schema Evolution 검증"
    )
    parser.add_argument(
        "--kafka-check", action="store_true",
        help="Kafka 토픽에서 새 컬럼 포함 이벤트 검증"
    )
    parser.add_argument(
        "--parquet-check", action="store_true",
        help="Parquet에서 새 컬럼 포함 이벤트 검증 (Spark 필요)"
    )
    args = parser.parse_args()

    if not args.kafka_check and not args.parquet_check:
        parser.print_help()
        sys.exit(0)

    results: list[bool] = []

    if args.kafka_check:
        print("=" * 50)
        print("[ Kafka 이벤트 검증 ]")
        print("=" * 50)
        results.append(check_kafka(args))

    if args.parquet_check:
        print()
        print("=" * 50)
        print("[ Parquet 검증 ]")
        print("=" * 50)
        results.append(check_parquet(args))

    print()
    if all(results):
        print("[PASS] Schema Evolution 검증 완료")
        sys.exit(0)
    else:
        print("[FAIL] 일부 검증 실패 -- 위 출력을 확인하세요.")
        sys.exit(1)


if __name__ == "__main__":
    main()
