# Real-time CDC Streaming Pipeline

MySQL 변경 데이터를 실시간으로 캡처해 Kafka로 흘리고, Spark Structured Streaming으로 처리하는 end-to-end 데이터 파이프라인.

```
MySQL ──► Debezium ──► Kafka ──► Spark Structured Streaming ──► Parquet
          (CDC)       (broker)   (stream processing)            (sink)
```

---

## 데이터셋

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 주문/고객/상품 데이터 (~35만 행, ~50MB)

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
│  Event Replayer                                             │
│  (scripts/replay_orders.py)                                 │
└─────────────────────────────────────────────────────────────┘
```

- **Debezium**: MySQL binlog를 읽어 INSERT/UPDATE/DELETE를 JSON 이벤트로 발행
- **Event Replayer**: orders_future_30을 시간 압축해서 MySQL에 순차 주입 → 실시간 CDC 스트림 생성
- **Spark**: Kafka 구독 → 이벤트 처리 → Parquet 저장 (Phase 3)

---

## 스택

| 컴포넌트 | 기술 |
|---------|------|
| Source DB | MySQL 8.0 |
| CDC | Debezium 2.7.3 (Kafka Connect) |
| Message Broker | Apache Kafka (Confluent 7.6.0) |
| Stream Processing | Spark Structured Streaming 3.5.7 |
| Batch Loading | pandas + SQLAlchemy |
| Orchestration | Docker Compose |

---

## 실행 방법

### 1. 환경 변수 설정

```bash
cp .env.example .env
# .env 수정: MYSQL_ROOT_PASSWORD, MYSQL_USER, MYSQL_PASSWORD
```

### 2. 인프라 실행

```bash
cd docker
docker compose up -d
```

> MySQL 호스트 포트: **3307** (로컬 MariaDB와 충돌 방지)

### 3. Debezium 커넥터 등록

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @docker/register-mysql-debezium.json
```

### 4. 초기 데이터 적재 (70%)

```bash
python spark/load_data.py
```

### 5. Event Replayer 실행 (30% CDC 스트림)

```bash
# dry-run으로 계획 확인
python scripts/replay_orders.py --dry-run

# 전체 실행 (약 3만건, 10분)
python scripts/replay_orders.py --duration-minutes 10
```

→ 자세한 옵션: [docs/REPLAYER.md](docs/REPLAYER.md)

### 6. Spark Streaming 실행

```bash
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /workspace/spark/stream_cdc.py
```

---

## CDC 이벤트 검증

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

---

## 진행 현황

- [x] Phase 0: 프로젝트 초기 설정, ADR 문서화
- [x] Phase 1a: Docker 인프라 + Debezium 커넥터
- [x] Phase 1b: 70% 초기 데이터 적재
- [x] Phase 2: Event Replayer 구현 및 CDC 이벤트 검증 (op=c, op=u)
- [ ] Phase 3: Parquet sink + checkpoint 볼륨 + 전체 테이블 구독
- [ ] Phase 4: End-to-end latency 측정
- [ ] Phase 5: Resiliency 검증 (재시작 복구, schema evolution)
