"""
verify_restart_recovery.py — Phase 5-1: 재시작 복구 검증

목적: Spark 컨테이너 재시작 후 checkpoint에서 resume되는지 검증.
     - earliest 재처리 없이 신규 이벤트만 처리하는지 확인 (at-least-once)

사용법:
    # 1) 재시작 전 스냅샷
    python scripts/verify_restart_recovery.py --snapshot

    # 2) Spark 컨테이너 중지 (수동)
    #    docker stop spark-master

    # 3) Spark 다운 상태에서 이벤트 주입
    python scripts/verify_restart_recovery.py --inject 10

    # 4) Spark 재시작 + job submit (수동)
    #    docker start spark-master
    #    docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /workspace/spark/stream_cdc.py

    # 5) 재시작 후 검증
    python scripts/verify_restart_recovery.py --verify 10
"""
import argparse
import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv

    _ROOT = Path(__file__).parent.parent
    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "cdc_output"
CHECKPOINT_DIR = BASE_DIR / "checkpoints" / "cdc_stream"
SNAPSHOT_FILE = Path(__file__).parent / ".restart_snapshot.json"


# ---------------------------------------------------------------------------
# 공통 헬퍼
# ---------------------------------------------------------------------------

def count_parquet_rows() -> dict[str, int]:
    """토픽별 Parquet 파일 행 수 집계"""
    counts: dict[str, int] = {}
    for topic_dir in sorted(DATA_DIR.glob("topic=*")):
        topic = topic_dir.name.replace("topic=", "")
        files = list(topic_dir.glob("*.parquet"))
        if files:
            counts[topic] = sum(len(pd.read_parquet(f)) for f in files)
    return counts


def read_latest_checkpoint_offset() -> dict:
    """checkpoint/offsets 디렉토리에서 최신 배치 offset 반환"""
    offsets_dir = CHECKPOINT_DIR / "offsets"
    if not offsets_dir.exists():
        return {}
    batch_files = [f for f in offsets_dir.iterdir() if not f.name.startswith(".")]
    if not batch_files:
        return {}
    latest = max(batch_files, key=lambda f: int(f.name))
    lines = latest.read_text().splitlines()
    # 형식: line0=v1, line1=conf JSON, line2=offset JSON
    if len(lines) < 3:
        return {}
    return json.loads(lines[2])


def make_connection():
    url = (
        f"mysql+pymysql://{os.environ['MYSQL_USER']}:{os.environ['MYSQL_PASSWORD']}"
        f"@{os.environ.get('DB_HOST', '127.0.0.1')}:{os.environ.get('DB_PORT', '3306')}"
        f"/{os.environ['MYSQL_DATABASE']}"
    )
    return create_engine(url)


# ---------------------------------------------------------------------------
# 명령 구현
# ---------------------------------------------------------------------------

def do_snapshot() -> None:
    """재시작 전 상태를 스냅샷 파일에 저장"""
    counts = count_parquet_rows()
    offsets = read_latest_checkpoint_offset()

    snapshot = {
        "timestamp": datetime.now().isoformat(),
        "parquet_rows": counts,
        "checkpoint_offsets": offsets,
    }
    SNAPSHOT_FILE.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False))

    print("[SNAPSHOT] 저장 완료")
    print(f"  저장 위치: {SNAPSHOT_FILE}")
    print("\n  Parquet 행 수 (토픽별):")
    for topic, cnt in counts.items():
        print(f"    {topic}: {cnt:,}행")
    print("\n  Checkpoint offset (최신 배치):")
    for topic, partitions in offsets.items():
        print(f"    {topic}: partition 0 → offset {partitions.get('0', '?')}")
    print("\n다음 단계: docker stop spark-master")


def do_inject(n: int) -> None:
    """MySQL UPDATE로 n건 shipped→delivered 이벤트 주입"""
    engine = make_connection()
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"UPDATE orders SET order_status='delivered' "
                f"WHERE order_status='shipped' LIMIT {n}"
            )
        )
        affected = result.rowcount

    print(f"[INJECT] MySQL UPDATE 완료: {affected}건 (shipped → delivered)")

    if affected == 0:
        # shipped가 없으면 processing으로 fallback
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    f"UPDATE orders SET order_status='delivered' "
                    f"WHERE order_status='processing' LIMIT {n}"
                )
            )
            affected = result.rowcount
        print(f"  (shipped 소진, processing → delivered로 fallback): {affected}건")

    if affected == 0:
        print("  ⚠️  주입 가능한 행 없음 — MySQL에서 직접 상태 확인 필요")
        print("       SELECT order_status, COUNT(*) FROM orders GROUP BY order_status;")
    else:
        print(f"\n다음 단계: docker start spark-master 후 spark-submit 실행")
        print(
            "  docker exec spark-master spark-submit \\\n"
            "    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \\\n"
            "    /workspace/spark/stream_cdc.py"
        )


def do_verify(expected_delta: int) -> None:
    """재시작 후 Parquet delta 검증 + checkpoint offset 진행 확인"""
    if not SNAPSHOT_FILE.exists():
        print("[ERROR] 스냅샷 파일 없음 — 먼저 --snapshot 실행")
        return

    snapshot = json.loads(SNAPSHOT_FILE.read_text())
    old_counts: dict[str, int] = snapshot["parquet_rows"]
    old_offsets: dict = snapshot["checkpoint_offsets"]

    new_counts = count_parquet_rows()
    new_offsets = read_latest_checkpoint_offset()

    print(f"[VERIFY] 재시작 복구 검증 (스냅샷: {snapshot['timestamp']})")
    print()

    # ── 행 수 비교 ──────────────────────────────────────────────────────────
    print("  [1] Parquet 행 수 변화:")
    total_delta = 0
    all_topics = sorted(set(list(old_counts.keys()) + list(new_counts.keys())))
    for topic in all_topics:
        old = old_counts.get(topic, 0)
        new = new_counts.get(topic, 0)
        delta = new - old
        total_delta += delta
        icon = "[OK]" if delta >= 0 else "[!!]"
        print(f"    {icon} {topic}: {old:,} -> {new:,} (+{delta})")

    print(f"\n    총 delta: {total_delta}건 / 예상: {expected_delta}건")

    # ── checkpoint offset 비교 ────────────────────────────────────────────
    print("\n  [2] Checkpoint offset 진행 여부:")
    all_offset_topics = sorted(set(list(old_offsets.keys()) + list(new_offsets.keys())))
    offset_advanced = False
    for topic in all_offset_topics:
        old_o = old_offsets.get(topic, {}).get("0", "?")
        new_o = new_offsets.get(topic, {}).get("0", "?")
        moved = str(old_o) != str(new_o)
        if moved:
            offset_advanced = True
        icon = "[OK]" if moved else "[--]"
        print(f"    {icon} {topic}: {old_o} -> {new_o}")

    # ── 판정 ──────────────────────────────────────────────────────────────
    print()

    # earliest 재처리 감지: delta가 예상의 10배 이상이면 의심
    reprocessed = total_delta > expected_delta * 10 and total_delta > 100

    if reprocessed:
        print("FAIL -- delta가 너무 큼. startingOffsets=earliest로 전체 재처리 의심")
        print(f"   예상 {expected_delta}건인데 {total_delta}건 처리됨")
        print("   → checkpoint 경로 확인 필요 (stream_cdc.py CHECKPOINT_PATH)")
    elif total_delta >= expected_delta and offset_advanced:
        print("PASS -- checkpoint resume 성공")
        print(f"   신규 {total_delta}건 처리 (at-least-once 보장 범위 내)")
    elif total_delta >= expected_delta and not offset_advanced:
        print("PARTIAL -- 행은 늘었으나 checkpoint offset 미진행")
        print("   → Spark job이 아직 실행 중이거나 checkpoint 반영 전일 수 있음")
    else:
        print(f"FAIL -- delta({total_delta}) < expected({expected_delta})")
        print("   → Spark가 신규 이벤트를 처리하지 못했거나 아직 실행 중")


# ---------------------------------------------------------------------------
# 엔트리포인트
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 5-1 재시작 복구 검증")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--snapshot", action="store_true", help="재시작 전 상태 저장")
    group.add_argument("--inject", type=int, metavar="N", help="MySQL UPDATE N건 주입")
    group.add_argument("--verify", type=int, metavar="N", help="재시작 후 N건 delta 검증")
    args = parser.parse_args()

    if args.snapshot:
        do_snapshot()
    elif args.inject is not None:
        do_inject(args.inject)
    elif args.verify is not None:
        do_verify(args.verify)


if __name__ == "__main__":
    main()
