# Real-time CDC Streaming Pipeline

MySQL 변경 데이터를 실시간으로 캡처해 Kafka로 흘리고, Spark Structured Streaming으로 처리하는 end-to-end 데이터 파이프라인.

```
MySQL ──► Debezium ──► Kafka ──► Spark Structured Streaming ──► Parquet
          (CDC)       (broker)   (stream processing)            (sink)
```

---

## 데이터셋

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 주문/고객/상품 데이터 (~35만 행, ~50MB)

> **데이터 파일은 git에 포함되지 않습니다.** Kaggle에서 직접 다운로드 후 아래 경로에 배치하세요.

```
spark-submit/data/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_customers_dataset.csv
└── olist_products_dataset.csv
```

Spark 분할 작업 (`spark-submit/split_orders_70_30.py`) 실행 후 `orders_initial_70/`, `orders_future_30/` 디렉토리가 생성됩니다.

| 분할 | 비율 | 용도 |
|------|------|------|
| orders_initial_70 | 70% | 초기 배치 적재 (역사 데이터) |
| orders_future_30 | 30% | Event Replayer로 CDC 스트림 시뮬레이션 |

**분리 근거**: 실제 운영 환경처럼 "이미 쌓인 데이터 + 실시간으로 들어오는 데이터"를 재현. [ADR-002](docs/DECISIONS.md)

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│  Docker Compose                                             │
│                                                             │
│  MySQL 8.0 ──binlog──► Debezium ──► Kafka ──► Spark        │
│     ▲          (Kafka Connect)    (broker)   Structured     │
│     │                                        Streaming      │
│  Event Replayer                                   │         │
│  (scripts/replay_orders.py)                       ▼         │
│                                              Parquet sink   │
└─────────────────────────────────────────────────────────────┘
```

- **Debezium**: MySQL binlog를 읽어 INSERT/UPDATE/DELETE를 JSON 이벤트로 발행
- **Event Replayer**: orders_future_30을 시간 압축해서 MySQL에 순차 주입 → 실시간 CDC 스트림 생성
- **Spark**: Kafka 구독 → `payload.after` JSON string sink → Parquet 저장

---

## 스택

| 컴포넌트 | 기술 |
|---------|------|
| Source DB | MySQL 8.0 |
| CDC | Debezium 2.7.3 (Kafka Connect) |
| Message Broker | Apache Kafka (Confluent 7.6.0) |
| Stream Processing | Spark Structured Streaming 3.5.7 |
| Batch Loading | pandas + SQLAlchemy (pymysql) |
| Orchestration | Docker Compose |

---

## 실행 방법

### 1. 환경 변수 설정

```bash
cp .env.example .env
# .env 수정: MYSQL_ROOT_PASSWORD, MYSQL_USER, MYSQL_PASSWORD
```

### 2. 데이터 준비

```bash
# Kaggle에서 다운로드 후 spark-submit/data/에 CSV 배치
# Spark로 70/30 분할
docker exec spark-master /opt/spark/bin/spark-submit \
  /workspace/spark-submit/split_orders_70_30.py
```

### 3. 인프라 실행

```bash
cd docker
docker compose up -d
```

> MySQL 호스트 포트: **3307** (로컬 MariaDB와 충돌 방지)

> Kafka 재시작 시 ZooKeeper stale node 문제가 발생하면:
> `docker restart zookeeper` 먼저 실행 후 kafka 재시작

### 4. Debezium 커넥터 등록

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @docker/register-mysql-debezium.json
```

### 5. 초기 데이터 적재 (70%)

```bash
python spark/load_data.py
```

### 6. Event Replayer 실행 (30% CDC 스트림)

```bash
# dry-run으로 계획 확인
python scripts/replay_orders.py --dry-run

# 전체 실행
python scripts/replay_orders.py --duration-minutes 10
```

### 7. Spark Streaming 실행

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --jars /workspace/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.7.jar,\
/workspace/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.7.jar,\
/workspace/jars/org.apache.kafka_kafka-clients-3.4.1.jar,\
/workspace/jars/org.apache.commons_commons-pool2-2.11.1.jar,\
/workspace/jars/mysql-connector-j-8.0.33.jar \
  /workspace/spark/stream_cdc.py
```

---

## 검증 스크립트

| 스크립트 | 목적 |
|----------|------|
| `scripts/check_dq.py` | DQ 3계층 검증 (MySQL / Parquet / Consistency) |
| `scripts/measure_latency.py` | CDC end-to-end latency 측정 (p50/p95/p99) |
| `scripts/verify_restart_recovery.py` | Spark 재시작 후 checkpoint resume 검증 |
| `scripts/verify_schema_evolution.py` | ALTER TABLE 후 Debezium schema 자동 감지 검증 |
| `scripts/replay_orders.py` | 시간 압축 Event Replayer |

```bash
# DQ 전체 검증
python scripts/check_dq.py

# Layer 1만 (MySQL 무결성)
python scripts/check_dq.py --layer 1

# latency 측정
python scripts/measure_latency.py
```

---

## CDC 이벤트 구조

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.ecommerce.orders \
  --from-beginning --max-messages 10 --timeout-ms 5000
```

| op | 의미 |
|----|------|
| `r` | 초기 스냅샷 (READ) |
| `c` | INSERT (CREATE) |
| `u` | UPDATE |
| `d` | DELETE |

---

## 측정 결과

### Latency (로컬 Docker, 109건)

| 구분 | p50 | p95 | p99 |
|------|-----|-----|-----|
| 전체 | 300ms | 29,115ms | 29,118ms |
| INSERT | 335ms | — | — |
| UPDATE | 300ms | — | — |

> p95/p99 이상치는 Spark micro-batch 처리 간격(30s trigger)에 기인. [ADR-010](docs/DECISIONS.md)

### Data Quality

```
Layer 1 (MySQL):    PASS 14 / FAIL 0
Layer 2 (Parquet):  PASS  8 / FAIL 0
Layer 3 (Consist):  WARN  2 / FAIL 0  (bulk load CDC 미경유 — 설계상 expected)
```

---

## 설계 결정

주요 기술 선택 근거는 [docs/DECISIONS.md](docs/DECISIONS.md)에 기록.

| ADR | 결정 |
|-----|------|
| ADR-001 | 배치는 pandas, 스트리밍만 Spark |
| ADR-002 | 원본 CSV 유지, CDC sink만 Parquet |
| ADR-003 | Event Replayer로 시간 압축 재생 |
| ADR-004 | Throughput 대신 Correctness/Latency 측정 |
| ADR-005 | Checkpoint 영구화 |
| ADR-006 | Debezium 커넥터 옵션 튜닝 |
| ADR-007 | Debezium 전용 유저 분리 (least privilege) |
| ADR-008 | Event Replayer 세부 설계 |
| ADR-009 | Spark Structured Streaming 선택 재검토 |
| ADR-010 | End-to-end Latency 측정 설계 |
| ADR-011 | Schema Evolution 검증 설계 |
| ADR-012 | Data Quality 3계층 검증 설계 |

---

## 진행 현황

- [x] Phase 0: 프로젝트 초기 설정, ADR 문서화
- [x] Phase 1a: Docker 인프라 + Debezium 커넥터
- [x] Phase 1b: 70% 초기 데이터 적재
- [x] Phase 2: Event Replayer 구현 및 CDC 이벤트 검증 (op=c, op=u)
- [x] Phase 3: Parquet sink + checkpoint 볼륨 + 전체 테이블 구독
- [x] Phase 4: End-to-end latency 측정 (p50/p95/p99)
- [x] Phase 5-1: Spark 재시작 복구 검증 (checkpoint resume)
- [x] Phase 5-2: Schema Evolution 검증 (ALTER TABLE ADD COLUMN)
- [x] Phase 6: 코드 정리 (dead code 제거, 보안 강화, 버그 수정)
- [x] Phase 7: Data Quality 3계층 검증 + timestamp 버그 수정
