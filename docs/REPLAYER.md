# Event Replayer

`scripts/replay_orders.py` — orders_future_30 CSV를 MySQL에 시간 압축 재생해서 Debezium CDC 스트림을 생성한다.

설계 근거: [ADR-003](DECISIONS.md#adr-003), [ADR-008](DECISIONS.md#adr-008)

---

## 동작 원리

```
orders_future_30.csv  (미래 30% 주문 데이터)
    │
    │  order_purchase_timestamp 기준 정렬
    │  시간 간격 비례 sleep (압축 비율 적용)
    ▼
MySQL INSERT  →  Debezium binlog 감지  →  Kafka op=c / op=u
```

- 이벤트 간 실제 시간 간격을 압축 비율로 나눠 sleep → 스트리밍 효과
- `--include-updates`: 일부 주문을 `invoiced`로 INSERT 후 `delivered`로 UPDATE 주입

---

## 사용법

### dry-run (계획 확인, DB 접속 없음)
```bash
python scripts/replay_orders.py --dry-run
```

### 빠른 테스트 (5건, 6초)
```bash
python scripts/replay_orders.py --limit 5 --duration-minutes 0.1
```

### UPDATE 이벤트 포함 테스트
```bash
python scripts/replay_orders.py --limit 10 --include-updates --update-ratio 1.0 --duration-minutes 0.1
```

### 전체 실행 (약 3만건, 10분)
```bash
python scripts/replay_orders.py --duration-minutes 10
```

### Spark Streaming과 동시 실행 (실제 데모)
```bash
# 터미널 1: Spark Streaming 시작
docker exec spark-master spark-submit ...

# 터미널 2: Event Replayer 시작
python scripts/replay_orders.py --duration-minutes 10
```

---

## 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--duration-minutes` | 10 | 총 재생 시간(분). compression-ratio와 상호 배타 |
| `--compression-ratio` | - | 고정 압축 비율 (예: 1000 → 원본 1000배 빠르게) |
| `--limit N` | - | 처음 N건만 처리 (테스트용) |
| `--include-updates` | OFF | UPDATE 이벤트 주입 활성화 |
| `--update-ratio` | 0.1 | UPDATE 주입 비율 (0.0~1.0) |
| `--dry-run` | OFF | 계획만 출력, DB 접속 없음 |

---

## 검증 방법

### MySQL INSERT 확인
```bash
docker exec mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" \
  -e "SELECT COUNT(*) FROM ecommerce.orders;"
```

### Kafka 이벤트 확인
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.ecommerce.orders \
  --from-beginning --max-messages 5 \
  --timeout-ms 5000
```

이벤트 타입:
- `op=c` — INSERT (CREATE)
- `op=u` — UPDATE
- `op=r` — 초기 스냅샷 READ (Replayer와 무관)

---

## 주의사항

- `INSERT IGNORE` 사용 — 동일 `order_id` 중복 실행 시 skip됨
- 재실행 시 MySQL에서 해당 행 DELETE 후 재시작
- Docker MySQL 포트: **3307** (로컬 MariaDB와 충돌 방지)
- `.env`에서 `DB_HOST`, `DB_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` 읽음
