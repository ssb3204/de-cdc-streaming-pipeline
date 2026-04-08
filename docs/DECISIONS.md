# Architecture Decisions & Learning Log

> 이 문서는 de-cdc-streaming-pipeline 프로젝트의 **기술 결정 과정**을 기록한다.
> 단순히 "무엇을 썼는가"가 아니라 **"왜 그 선택을 했는가, 대안은 무엇이었는가, 이 프로젝트에 왜 적합한가"** 를 남긴다.
> 학습용 자료로도 활용 가능하도록 잘못된 초기 판단과 그 수정 과정까지 포함한다.

---

## Table of Contents

1. [프로젝트 목적과 스케일 재정의](#1-프로젝트-목적과-스케일-재정의)
2. [코드 리뷰 결과 요약](#2-코드-리뷰-결과-요약)
3. [잘못된 첫 개선 계획 (반면교사)](#3-잘못된-첫-개선-계획-반면교사)
4. [비판적 재검토 — 던진 질문들](#4-비판적-재검토--던진-질문들)
5. [기술 선택 재검토 (ADR)](#5-기술-선택-재검토-adr)
6. [수정된 개선 우선순위](#6-수정된-개선-우선순위)
7. [이력서 문장 Before/After](#7-이력서-문장-beforeafter)
8. [학습 노트](#8-학습-노트)

---

## 1. 프로젝트 목적과 스케일 재정의

### 원래 프로젝트
- **Stack**: MySQL → Debezium → Kafka Connect → Kafka → Spark Structured Streaming
- **Data**: Olist e-commerce dataset (Brazilian marketplace)
- **Infra**: 단일 노드 Docker Compose (로컬)
- **최초 목표**: Kafka/CDC 아키텍처 학습

### 실제 데이터 스케일

| 테이블 | 행 수 | CSV 크기 |
|--------|-------|----------|
| orders | ~100k | ~17MB |
| order_items | ~113k | ~15MB |
| customers | ~100k | ~9MB |
| products | ~33k | ~2MB |
| **합계** | **~350k** | **~50MB** |

**중요한 깨달음**: 이건 **빅데이터가 아니다**. pandas로 10초 안에 처리 가능한 규모.

### 재정의된 프로젝트 목적
> **"소규모 로컬 환경에서 CDC 파이프라인의 Correctness · Resiliency · Operability를 증명하는 프로젝트"**

→ 성능(Throughput) 벤치마크 목적이 아니라, **아키텍처 이해와 설계 결정의 근거**를 증명하는 것이 목표.

### 왜 이 재정의가 중요한가
주니어 데이터 엔지니어 이력서/면접 관점:
- ❌ "10만 건을 1초에 처리" → 면접관: "그 규모는 Spark 안 써도 됩니다"
- ✅ "왜 이 도구를 썼는지 근거 있고, 대안을 검토했고, trade-off를 이해함" → 면접관: "엔지니어링 감각 있네"

**Scale이 작을수록 '성능'이 아니라 '판단력'으로 증명해야 한다.**

---

## 2. 코드 리뷰 결과 요약

### CRITICAL (기능 블로커)
- **하드코딩된 비밀번호** 다수 (`docker-compose.yml`, `load_data.py`, 커넥터 JSON)
- **Debezium 커넥터 JSON 3개 충돌**:
  - `mysql-cdc-config.json` (불완전, connector name 없음)
  - `register-mysql-debezium.json` (appuser 사용)
  - `mysql_ecommerce_connector.json` (`debezium` 유저를 쓰는데 **DB에 그 유저가 없음**)
  - 두 파일이 같은 `database.server.id: 184054` → 동시 등록 시 충돌
- **init.sql 2개**가 **완전히 다른 스키마** (`sql/init.sql` vs `docker/sql/init.sql`)

### HIGH
- `load_data.py` port 3307 참조 — docker-compose는 3306만 매핑
- `stream_cdc.py` TOPIC = `"ecommerce.ecommerce.cdc_test"` — **존재하지 않는 테이블**
- `checking_data.py:4` Windows 절대 경로 하드코딩 (이 PC에서만 동작)
- `docker/docker` 정체불명 파일

### MEDIUM
- `load_mysql_spark.py:62` — `print(f"JDBC_URL = {JDBC_URL}")` → 비밀번호 로그 노출
- `spark/` vs `spark-submit/spark/` — 동일 파일 중복 (어느 게 정본인지 불명확)
- `load_data.py` products 컬럼 오타 처리 — `load_mysql_spark.py`와 불일치
- `split_data.py` — 예외 silent swallow (`raise` 주석 처리)

### LOW
- 대용량 CSV가 git에 커밋됨 (`.gitignore` 없음)
- `requirements.txt` / `pyproject.toml` 없음

**→ 전체 수정 없이는 파이프라인 자체가 구동 안 될 수 있음. 개선 작업 전에 이 CRITICAL 이슈들부터 해결 필요.**

---

## 3. 잘못된 첫 개선 계획 (반면교사)

> ⚠️ 이 섹션은 **초기 추천안**이다. 나중에 비판적 재검토를 거쳐 **상당수가 기각**되었다.
> 학습 목적으로 기록해둔다 — "DE 면접에서 이렇게 답하면 안 되는 예시"로.

### 초기 추천 7가지 (기각/수정 포함)
1. ~~CSV → Parquet + Snappy/ZSTD 압축~~ (부분 수정)
2. ~~Pandas → Spark JDBC 병렬 적재~~ (**기각** — 이 스케일에 Spark 불필요)
3. Debezium snapshot 모드 / 락 최적화 (유지)
4. Spark Streaming Sink: Console → Parquet (유지, 이유 변경)
5. ~~Kafka Topic 사전 생성 + 파티셔닝~~ (**기각** — 로컬 단일 브로커에서 의미 없음)
6. DQ 체크 (유지)
7. Checkpoint 영구화 + 모니터링 (유지)

### 왜 잘못됐나
- **"Best practice 나열"**이지, 이 프로젝트의 맥락을 고려하지 않음
- **100MB 데이터에 Spark 파티셔닝**을 추천한 건 도구 오남용
- **"파티션 4배로 throughput 3배 향상"** 같은 수치는 단일 브로커 로컬에선 **측정 자체가 불가능**
- **CSV→Parquet**를 "스토리지 82% 절감"이라고 했지만, 50MB → 10MB는 **절약이 아니다**
- 가장 중요한 질문 — **"배치 데이터로 실제 스트리밍 흐름을 어떻게 재현하는가?"** — 에 답하지 못했음

---

## 4. 비판적 재검토 — 던진 질문들

사용자가 던진 핵심 질문들. **이 질문들이 프로젝트 방향을 완전히 바꿨다.**

### Q1. 왜 Parquet인가? 사람이 못 읽는 단점은?
- CSV는 디버깅할 때 `cat`, `head`, git diff로 바로 볼 수 있음
- Parquet는 전용 도구(`parquet-tools`, DuckDB 등) 필요
- 50MB 데이터에 대해 압축률은 사실상 의미 없음
- **결론**: Parquet 자체가 목적이 되면 안 됨. "왜 이 포맷인가"가 설명되어야 함.

### Q2. Spark를 쓸 만한 데이터 크기인가?
- 50MB → pandas가 Spark보다 빠르고 가벼움 (JVM 부팅 비용 없음)
- Spark를 batch load에 쓰는 건 **포트폴리오용 과시**에 불과
- **결론**: 도구는 데이터 스케일에 맞춰야 한다. "Spark 쓸 줄 안다"를 증명하려고 Spark를 쓰면 오히려 감점.

### Q3. 로컬 Kafka에서 성능 측정이 의미 있나?
- 단일 브로커, 단일 컨슈머, 50MB에서 partition 수 늘려도 측정 불가
- Throughput 벤치마크는 **로컬에서 거짓말에 가깝다**
- **결론**: 로컬에서 증명 가능한 것은 성능이 아니라 **정확성(correctness)**. exactly-once, 재시작 복구, 스키마 진화 등.

### Q4. 배치 데이터로 어떻게 실시간 흐름을 재현했나?
- **이것이 가장 중요한 질문이었다.**
- 현재 70/30 split은 "초기 70% 벌크 + 30% 벌크"라서 **연속 이벤트가 아님**
- Debezium은 한 번 덤프하고 끝 → streaming 파이프라인의 의미가 사라짐
- **결론**: 시간 기반 Replayer가 필요하다. 이게 프로젝트의 정당성을 결정한다.

### Q5. 이게 실사용자의 흐름과 유사하다고 어떻게 판단했나?
- Olist는 18개월치 과거 데이터 → 정적 덤프
- 실제 이커머스는 **연속된 작은 트랜잭션**
- **결론**: 원본 timestamp 순서대로 재생하되, 시간 축을 압축(예: 1일→1분)해서 실 트래픽 패턴을 유지해야 함.

### 교훈
> **DE 역량 = 도구 선택 근거 + 대안 검토 + trade-off 이해.**
>
> "무엇을 썼나"보다 "왜 그것을 썼고 왜 다른 것을 기각했나"가 훨씬 중요하다.

---

## 5. 기술 선택 재검토 (ADR)

### ADR-001: 배치 적재는 pandas, 스트리밍은 Spark

**Context**: 초기 70% 데이터 적재 도구 선택.

**Decision**: pandas 사용. Spark는 streaming 단계에만.

**Alternatives**:
| 옵션 | 평가 |
|------|------|
| Spark JDBC | JVM 워밍업 ~10초, 50MB에 오버킬, 코드 복잡 |
| **pandas + SQLAlchemy** | 즉시 실행, 코드 간결, 이 스케일에 최적 |
| polars | 더 빠르지만 이 규모에선 pandas와 차이 미미, 학습 곡선 |
| Dask | 중간 지점이지만 여기선 필요 없음 |

**Rationale**:
- 50MB는 메모리에 편하게 로드됨 → 단일 프로세스로 충분
- Spark는 checkpoint/상태관리가 필요한 streaming 단계에만 사용
- "도구별 적합성 분리"는 이력서 포인트 (면접관이 좋아함)

**Trade-offs**:
- (-) Spark로 일원화하지 못해 코드 베이스에 2개 도구 공존
- (+) 각 단계마다 "왜 이 도구인가"를 정당화할 수 있음

---

### ADR-002: 저장 포맷 — 원본 CSV 유지, CDC sink만 Parquet

**Context**: 저장 포맷 선택.

**Decision**:
- Raw Olist 원본 → **CSV 유지**
- 70/30 split 결과 → **CSV 유지**
- CDC Streaming sink → **Parquet (partitioned by op, date)**

**Alternatives**:
| 포맷 | 장점 | 단점 | 적용 |
|------|------|------|------|
| CSV | 사람이 읽음, 디버깅 쉬움 | 스키마 없음, 크기 큼 | 원본/중간 |
| Parquet | 컬럼 기반, 쿼리 효율, 스키마 보존 | 툴 필요, 작은 파일에 메타 오버헤드 | 분석용 sink |
| Avro | 스키마 진화, row-based (스트리밍 친화) | 읽기엔 툴 필요 | Schema Registry 없어서 기각 |
| JSON Lines | Debezium 원본 포맷과 동일 | 크기 큼 | 불필요한 중간 포맷 |
| Delta Lake | ACID + time travel + upsert | 로컬 학습 프로젝트엔 과잉 | 향후 확장 후보 |

**Rationale**:
- 원본은 source of truth → human-readable 유지해야 검증 가능
- Streaming sink는 "쿼리용" 목적이 분명 → Parquet이 타당
- Delta Lake는 멋지지만 이 스케일에선 over-engineering

**Trade-offs**:
- (-) 두 가지 포맷 혼재 → 통일성 떨어짐
- (+) 각 단계의 목적이 포맷에 반영됨 — 설명 가능

---

### ADR-003: 실시간 흐름 재현 — 시간 기반 Event Replayer

**Context**: Olist는 정적 데이터셋 — 그 자체론 streaming 파이프라인을 증명할 수 없음.

**Decision**: `replay_orders.py` 구현.
- `orders_future_30.csv` 를 `order_purchase_timestamp` 기준 정렬
- 원본 시간차를 N배 압축(예: 1일→1분)해서 MySQL에 연속 INSERT/UPDATE
- 일부 UPDATE 이벤트도 주입 (주문 상태 변경 시뮬)

**Alternatives**:
| 방식 | 평가 |
|------|------|
| Faker로 랜덤 데이터 생성 | 현실성 X, Olist 실제 패턴 상실 |
| 고정 간격 insert (1초마다 1건) | 단순하지만 실 트래픽 burst 패턴 없음 |
| **timestamp 비례 리플레이** | 원본 트래픽 패턴(피크/밸리) 보존, 시간만 압축 |
| Kafka Producer 직접 주입 | Debezium 우회 → CDC 실습 목적 상실 |

**Rationale**:
- 이게 없으면 Debezium은 한 번 덤프만 하고 끝 → streaming 증명 불가
- 시간 비례 압축은 실 트래픽의 burst/lull 패턴을 유지
- MySQL → Debezium 경로를 그대로 살려서 CDC 학습 목적 달성

**Trade-offs**:
- (-) 진짜 실시간은 아님 (시간 압축)
- (+) end-to-end latency, consumer lag 등 의미 있는 측정 가능
- (+) 재현 가능한 테스트 시나리오로 정착 가능

**이게 이 프로젝트의 핵심 차별점이 될 것이다.**

---

### ADR-004: 측정 지표 — Throughput 대신 Correctness/Latency

**Context**: 로컬 단일 브로커에서 throughput 측정은 신뢰할 수 없음.

**Decision**: 아래 지표만 측정하고 보고.

| 지표 | 방법 | 이유 |
|------|------|------|
| End-to-end latency (p50/p95/p99) | Debezium `ts_ms` vs Spark 처리 시점 | 의미 있는 숫자 |
| Consumer lag | `kafka-consumer-groups --describe` | 시각화 가능 |
| Exactly-once 증명 | MySQL row count == Parquet sink count (재시작 후) | 정확성 증명 |
| Schema evolution 대응 | ALTER TABLE 후 파이프라인 복구 여부 | 운영 대비 |
| 장애 복구 | Kafka Connect / Spark kill 후 재기동 → 데이터 일관성 | Resiliency |

**Alternatives**:
- Throughput (msg/sec) 벤치마크 → **기각** (로컬에선 거짓말)
- Kafka partition 튜닝 비교 → **기각** (단일 브로커에서 의미 없음)

**Rationale**:
- 로컬에서 측정 가능한 것만 측정 → 신뢰도 확보
- "정확성/복구성"은 production에서 **더 중요한 속성**임

---

### ADR-005: Checkpoint 영구 저장

**Context**: 현재 `stream_cdc.py` 가 `/tmp/spark_chk/` 사용 → 컨테이너 재시작 시 유실.

**Decision**: `/workspace/checkpoints/` (볼륨 마운트 경로)로 이동.

**Rationale**:
- checkpoint 유실 = exactly-once 보장 깨짐
- 재시작 복구 시나리오를 증명하려면 필수
- 볼륨에 두면 host 파일 시스템에 영구 저장

---

### ADR-006: Debezium 옵션 — 학습 목적 튜닝

> ⚠️ **ADR-006 개정 (2026-04-08)**: 아래 옵션 중 일부는 **이 프로젝트 환경에서 검증 불가능**하여 기각/연기한다.
>
> - **기각**: `tombstones.on.delete=true` — 이 프로젝트는 Parquet sink를 쓰고 log compaction을 사용하지 않는다. tombstone 동작을 검증할 코드 경로가 없으므로 "버즈워드 나열"이 된다.
> - **Phase 5로 연기**: `schema.history.internal.skip.unparseable.ddl=true` — schema evolution 테스트에서 필요할 때 추가한다. 그 전엔 의미 없는 안전장치.
> - **유지**: `snapshot.mode`, `snapshot.locking.mode`, `include.schema.changes`, `table.include.list`, `time.precision.mode`, `decimal.handling.mode` — 각각 이 프로젝트 환경에서 실제 효과가 있거나 학습 가치가 있음.
>
> 원칙: "문서에 적혔다고 무조건 도입하지 말고, 실제 검증 가능한 것만 도입한다."

**Context**: 현재 `register-mysql-debezium.json` 에 기본값만 사용. 기본값으로도 동작은 하지만, 각 옵션의 의미를 이해하고 있음을 증명해야 함.

**Decision**: 아래 옵션을 명시적으로 설정하고 각각의 이유를 주석으로 기록.

```json
{
  "snapshot.mode": "initial",              // 최초 기동 시 전체 스냅샷
  "snapshot.locking.mode": "none",         // 락 없음 (운영 영향 최소화)
  "tombstones.on.delete": "true",          // DELETE 시 null 이벤트 발행 (log compaction 대비)
  "include.schema.changes": "true",         // DDL 변경 Kafka 토픽에 기록
  "schema.history.internal.skip.unparseable.ddl": "true"
}
```

**Rationale**:
- 로컬에선 성능 차이 없지만, **각 옵션의 의미를 문서화**하는 게 학습 성과
- 면접에서 "왜 `snapshot.locking.mode=none`으로 했어요?" 질문에 답할 수 있어야 함

---

### ADR-007: Debezium replication 유저 분리 (2026-04-08)

**Context**: Debezium이 MySQL binlog를 읽으려면 `REPLICATION SLAVE, REPLICATION CLIENT` 권한이 필요하다. 현재 `appuser`는 `ecommerce` DB에 대한 기본 권한만 있어 Debezium 구동 자체가 불가능. P0 작업 중 이 문제가 드러남.

**Decision**: `debezium` 전용 유저를 `init.sql`에서 생성. `appuser`는 애플리케이션 CRUD 권한만, `debezium`은 replication 권한만 갖도록 분리.

**Alternatives**:
| 옵션 | 평가 |
|------|------|
| `appuser`에 REPLICATION 권한 추가 | 단순하지만 앱 유저와 복제 유저 권한이 섞임 → 권한 최소화 원칙 위반 |
| **`debezium` 전용 유저 신규 생성** | 권한 분리, 운영 관행, 면접에서 설명 가능 |
| 외부 Secrets Manager로 자격증명 관리 | 로컬 단일 브로커 학습 환경에 과잉 |

**Rationale**:
- **권한 최소화(least privilege)** 는 DB 운영의 기본 원칙. `appuser`가 탈취됐을 때 binlog 전체가 유출되면 안 됨.
- 비용은 `init.sql`에 3줄 추가뿐 — 기술 도입이 아니라 **SQL 설정 변경**이므로 "프로젝트 사이즈에 안 맞는 기술" 범주에 해당하지 않음.
- 면접 포인트: "왜 전용 유저?" → "binlog 읽기 권한은 강력해서 앱 유저와 분리했습니다."

**Trade-offs**:
- (-) 비밀번호를 하나 더 관리해야 함
- (-) `debezium` 비밀번호가 `init.sql`에 하드코딩됨. MySQL `initdb`가 환경변수 치환을 지원하지 않기 때문. **운영 환경에선 Secrets Manager/Vault가 필요**하다는 한계를 문서화.
- (+) 권한 분리 구조를 git/코드로 증명
- (+) 운영 관행 학습

**How to verify**:
- `SHOW GRANTS FOR 'appuser'@'%'` → `ecommerce` DB 권한만
- `SHOW GRANTS FOR 'debezium'@'%'` → `REPLICATION SLAVE`, `REPLICATION CLIENT` 포함
- Debezium 커넥터가 `debezium` 유저로 접속하여 binlog tailing 성공

---

### ADR-008: Event Replayer 구현 세부 설계 (2026-04-08)

**Context**: ADR-003에서 "시간 기반 Event Replayer"를 도입하기로 결정했지만, 실제 구현에는 **압축 비율 선택, CLI 스펙, MySQL write 전략, UPDATE 이벤트 주입 방법, 재시작 정책** 등 구체 결정이 필요하다. 이 ADR은 `scripts/replay_orders.py` 의 구현 계약을 확정한다.

**데이터 계측 결과** (`orders_future_30/part-00000-*.csv`):

| 항목 | 값 |
|------|----|
| 행 수 | 29,833 |
| 원본 시간 범위 | 2018-04-13 21:53 UTC → 2018-10-17 17:30 UTC |
| 원본 span | **186.8 일 (≈ 6개월)** |
| `order_status` 분포 | delivered 29,254 / shipped 252 / canceled 196 / invoiced 72 / unavailable 48 / processing 11 |
| `order_purchase_timestamp` null | 0 |

---

#### 결정 1: 시간 압축 단위 — "목표 재생 시간" 방식

**Decision**: 사용자가 **총 재생 시간(분)** 을 지정하면 스크립트가 compression ratio를 자동 계산. 기본값 **10분**.

**Alternatives**:
| 옵션 | 평가 |
|------|------|
| `--compression-ratio 27000` (고정 배수) | 데이터 span이 바뀌면 재계산 필요, 직관적이지 않음 |
| **`--duration-minutes 10` (목표 시간)** | 데이터 기간과 무관하게 "10분짜리 시나리오"를 만든다는 의도가 명확 |
| `--events-per-second 50` (고정 속도) | 원본 burst/lull 패턴 소실 → ADR-003 정신 위반 |

**Rationale**:
- **10분이 기본**인 이유: 29,833 events / 600s = **평균 ~50 events/s**. 로컬 Debezium + 단일 Kafka 브로커에서 여유 있게 처리 가능한 수준. 더 짧게 하면 burst 구간에서 binlog tailing이 밀릴 수 있고, 더 길게 하면 검증 사이클이 느려진다.
- 압축 비율로 환산하면 `186.8일 / 10분 ≈ 26,900x` — 내부 구현은 이 ratio로 환산해 `time.sleep()` 계산에 사용.
- `--compression-ratio` 플래그도 남겨두되, `--duration-minutes`와 상호 배타(`argparse` mutually exclusive group).

**Trade-offs**:
- (-) "실시간"이 아니라는 점을 README에서 명시해야 함
- (+) 원본 burst 패턴은 보존됨 (이벤트 간 상대 시간차는 동일 비율로 유지)
- (+) 10분짜리 짧은 실행으로 CI/검증 루프 구축 가능

---

#### 결정 2: MySQL write 전략 — 건당 INSERT + autocommit

**Decision**: 한 건씩 `INSERT` 후 **즉시 commit**. 배치 insert 사용하지 않음.

**Alternatives**:
| 옵션 | 평가 |
|------|------|
| `executemany` 배치 insert (100건 단위) | 한 트랜잭션 = 한 commit → binlog에 이벤트 덩어리로 찍힘 → **ADR-003의 "연속 CDC 이벤트" 정신 위반** |
| **건당 INSERT + autocommit** | 각 INSERT가 별도 binlog 이벤트 → Debezium이 초당 50건의 개별 CDC 이벤트로 포착 |
| LOAD DATA INFILE | 벌크 로드 용도, 연속 이벤트 생성 의도와 정반대 |

**Rationale**:
- 이 Replayer의 목적은 "벌크 적재"가 아니라 "**실시간처럼 보이는 CDC 이벤트 스트림 생성**"이다. 건당 커밋이 유일하게 이 목적에 부합한다.
- pandas/SQLAlchemy 대신 `pymysql` 직접 사용 — SQLAlchemy ORM 레이어는 이 단순 INSERT에 오버헤드이고, connection 재사용 제어가 직관적이지 않음.

**Trade-offs**:
- (-) 29,833 commit → 단일 프로세스 기준 ~10분간 지속 부하. 로컬 개발 머신에는 무시 가능.
- (+) Debezium이 각 INSERT를 개별 `op=c` 이벤트로 발행 → Kafka 토픽에서 연속 이벤트 확인 가능.

---

#### 결정 3: 중복/FK 위반 처리 — `INSERT IGNORE` + 로그

**Decision**: SQL은 `INSERT IGNORE INTO orders ...` 사용. skip된 건은 WARN 로그.

**이유**:
- **재시작 안전성(idempotency)**: 중간에 스크립트가 죽거나 사용자가 Ctrl+C로 끊었을 때, 동일 CSV를 다시 돌려도 이미 들어간 `order_id`는 PK 충돌로 조용히 skip. offset 기반 재시작 로직을 구현하지 않아도 됨 → **학습 프로젝트에 적합한 단순성**.
- **FK 위반 대응**: `customer_id`가 `customers` 테이블에 없을 때 `INSERT IGNORE`는 FK 위반도 조용히 skip. 단, 이 경우엔 count를 따로 집계해 경고.

**기각된 대안**:
- Offset 파일(`.replay_state.json`) 기반 재시작 — 명시적이지만 구현 복잡도 ↑, 학습 프로젝트엔 과잉.
- `ON DUPLICATE KEY UPDATE` — "이전 값을 덮어쓴다"는 의미 추가돼 의도 불명확.

**Trade-offs**:
- (-) `INSERT IGNORE` 는 FK 위반과 중복 PK를 구분 없이 삼킴 → 스크립트가 `SELECT COUNT(*)` 로 before/after 비교해 skip 원인을 사후 분리 가능하도록 함.
- (+) "CTRL+C 후 재실행해도 망가지지 않음" — 데모/면접 시연 친화적.

---

#### 결정 4: UPDATE 이벤트 주입 — 옵트인, 상태 전이 1회

**Decision**: `--include-updates` 플래그 활성화 시, INSERT 후 **1~3초 내에 동일 order_id에 대해 `order_status` UPDATE 1건 발행**. 기본 비율 `--update-ratio 0.1` (10%).

**상태 전이 규칙**: 원본 CSV의 최종 상태가 `delivered`인 행을 대상으로:
1. 최초 INSERT 시 `order_status='invoiced'` 로 덮어씀 (나머지 컬럼은 원본 유지)
2. 1~3초 후 `UPDATE orders SET order_status='delivered', order_delivered_customer_date=... WHERE order_id=?`

**Alternatives**:
| 옵션 | 평가 |
|------|------|
| **opt-in UPDATE 주입 (기본 OFF)** | insert-only 모드로 빠른 검증 가능, updates는 별도 테스트 |
| 항상 UPDATE 주입 | 단순하지만 빠른 디버깅 루프 방해 |
| 2~3단계 상태 전이 (invoiced→shipped→delivered) | 더 현실적이지만 타이밍 복잡, 이 프로젝트 목적엔 과잉 — UPDATE 이벤트가 발생하는지만 증명하면 충분 |
| DELETE 이벤트 주입 | ADR-006 개정에서 tombstones 기각했고 log compaction 미사용 → 의미 없음 |

**Rationale**:
- Debezium CDC 파이프라인이 **`op=c`만이 아니라 `op=u`도 정상 처리하는지** 증명해야 한다. 10% 비율이면 ~2,980건의 UPDATE 이벤트 → 충분히 유의미한 샘플.
- 단계 하나(invoiced→delivered)만 구현하는 이유: 학습 목적상 "UPDATE 이벤트 `before`/`after` 페이로드 관찰" 이면 충분. 다단계 전이는 over-engineering.

**Trade-offs**:
- (-) 인위적 상태 전이 — 원본 데이터 분포를 왜곡
- (-) UPDATE 시점이 실제 Olist의 배송 주기와 무관한 랜덤 지연
- (+) `op=u` 이벤트 생성 증명 가능, ADR-004의 "exactly-once 증명" 에 UPDATE도 포함 가능

---

#### 결정 5: CLI 스펙

```
python scripts/replay_orders.py \
  [--csv PATH]                        # 기본: spark-submit/data/orders_future_30/part-00000-*.csv
  [--duration-minutes INT]            # 총 재생 시간 (기본 10)
  [--compression-ratio FLOAT]         # 대안: 고정 비율 (duration-minutes와 상호 배타)
  [--limit INT]                       # 처음 N건만 (테스트용, 기본 전체)
  [--include-updates]                 # UPDATE 주입 활성화 (기본 OFF)
  [--update-ratio FLOAT]              # UPDATE 비율 (기본 0.1)
  [--update-delay-range MIN,MAX]      # UPDATE 지연 범위 초 (기본 1,3)
  [--dry-run]                         # DB 안 건드리고 plan만 출력
  [--log-level LEVEL]                 # 기본 INFO
```

**핵심 동작**:
1. `.env` 로드 → `MYSQL_USER`, `MYSQL_PASSWORD`, `DB_HOST`, `DB_PORT`, `MYSQL_DATABASE`
2. CSV 읽고 `order_purchase_timestamp` 오름차순 정렬
3. `--dry-run` 시: 이벤트 수, span, 계산된 ratio, 예상 총 소요시간, 첫/마지막 timestamp만 출력하고 종료
4. 실제 실행 시: `pymysql` autocommit 연결, 루프 돌며 `time.sleep((next_ts - prev_ts) / ratio)` 후 INSERT
5. 종료 시 요약 출력: inserted / skipped / updates_pending / elapsed / planned / drift

---

#### 결정 6: 관측 지표 (ADR-004 연장)

Replayer가 stdout/로그로 출력할 지표:

| 지표 | 의미 | 왜 중요 |
|------|------|---------|
| `inserted` | 성공 INSERT 수 | 완주 검증 |
| `skipped_pk` | PK 중복 skip 수 | idempotency 확인 (재실행 시 > 0) |
| `skipped_fk` | FK 위반 skip 수 | customers 테이블 무결성 검증 |
| `updates_sent` | 발행된 UPDATE 수 | `op=u` 이벤트 생성 증명 |
| `planned_seconds` | 계산된 총 재생 시간 | 설정 투명성 |
| `actual_seconds` | 실제 소요 시간 | 드리프트 측정 |
| `drift_pct` | `(actual - planned) / planned` | 50 events/s를 로컬이 소화했는가 |

`drift_pct`가 일관되게 10% 이상이면 → `--duration-minutes`를 늘려야 한다는 신호 (ADR-008 후속 개정 트리거).

---

**Trade-offs 총정리**:
- (-) 진짜 실시간 아님, 단일 프로세스, 인위적 UPDATE
- (+) 원본 burst/lull 패턴 보존
- (+) idempotent 재실행
- (+) 10분 실행으로 빠른 검증 루프
- (+) MySQL → Debezium → Kafka 경로를 **그대로** 사용 → CDC 학습 목적 달성

**How to verify** (ADR-008 구현 완료 기준):
1. `--dry-run` 으로 29,833 rows / 186.8일 span / ratio ~26,900x 출력 확인
2. `--limit 10 --dry-run` 으로 소규모 계획 확인
3. `docker compose up` 상태에서 `--limit 10` 실제 실행 → MySQL `orders` 테이블 10건 증가 확인
4. 같은 명령 재실행 → `skipped_pk=10, inserted=0` 확인 (idempotency)
5. `kafka-console-consumer --topic ecommerce.ecommerce.orders` 에서 10건의 `op=c` 이벤트 관찰
6. `--limit 100 --include-updates --update-ratio 0.5` 실행 → `op=u` 이벤트 ~50건 관찰

---

## 6. 수정된 개선 우선순위

| 우선순위 | 항목 | Why | 완료 증명 |
|---------|------|-----|-----------|
| **P0** | CRITICAL 이슈 정리 (커넥터 JSON, init.sql, 하드코딩 비밀번호) | 없으면 파이프라인 자체가 안 돎 | `docker compose up` 한 번에 성공 |
| **P0** | `.env` 도입 → 자격증명 외부화 | 기본 보안 | 하드코딩 0건 |
| **P1** | **Event Replayer 구현** (`replay_orders.py`) | **이 프로젝트의 정당성** | 30% 데이터 시간 압축 재생, CDC 이벤트 연속 발생 확인 |
| **P1** | Spark Streaming sink → Parquet + volume checkpoint | exactly-once 증명 기반 | `/workspace/data/cdc_output/` 에 partitioned Parquet 저장 |
| **P1** | End-to-end latency 측정 | 의미 있는 지표 | README에 p50/p95/p99 표 |
| **P2** | 재시작 복구 테스트 (Kafka Connect, Spark 각각 kill) | Correctness 증명 | 10회 반복, 유실/중복 0건 |
| **P2** | Schema evolution 테스트 (`ALTER TABLE ADD COLUMN`) | 운영 대비 증명 | ADD 후 파이프라인 자동 복구 |
| **P2** | batch load 정리: `load_data.py` 유지, `load_mysql_spark.py` 제거 | 도구 선택 근거 명확화 | 단일 batch 스크립트 |
| **P3** | DQ 체크 (PK null, 중복, row count 일치) | 운영 마인드셋 | 자동화 스크립트 + 검증 로그 |
| **P3** | Debezium 옵션 주석 문서화 | 학습 증명 | 커넥터 JSON + 코멘트 |
| **~~기각~~** | ~~Kafka partition 튜닝~~ | 로컬에선 의미 없음 | - |
| **~~기각~~** | ~~Spark JDBC 파티셔닝~~ | 데이터 스케일 부적합 | - |
| **~~기각~~** | ~~Throughput 벤치마크~~ | 단일 브로커에서 신뢰 불가 | - |

---

## 7. 이력서 문장 Before/After

### ❌ Before (허세, 검증 불가)
> "CSV → Parquet 전환으로 스토리지 82% 절감, Spark JDBC 병렬 적재로 **적재 시간 78% 단축**, Kafka partition 튜닝으로 consumer 처리량 **3.2배 향상**"

**면접관 예상 질문**:
- "50MB 데이터에 Parquet가 왜 필요하죠?"
- "단일 브로커에서 partition을 4로 늘리면 뭐가 달라지죠?"
- "Spark JDBC 쓰면서 pandas 대비 JVM 오버헤드는 고려했나요?"

→ **대답할 수 없음. 감점.**

### ✅ After (근거 있음, 검증 가능)
> **Olist 기반 CDC 파이프라인 Correctness 검증 프로젝트**
>
> - 정적 데이터셋의 한계를 해결하기 위해 `order_purchase_timestamp` 기반 **Event Replayer** 를 구현. 18개월치 주문 데이터를 1시간으로 시간 압축 재생하여 Debezium → Kafka → Spark Structured Streaming 파이프라인에 **연속 CDC 이벤트 ~30,000건** 주입.
> - End-to-end latency (MySQL 커밋 → Parquet sink 도달) **p95 XXXms, p99 XXXms** 측정 체계 구축.
> - Kafka Connect 및 Spark Driver 강제 종료 시나리오 10회 반복 테스트에서 **exactly-once 보장, 이벤트 유실/중복 0건** 검증 (persistent checkpoint + idempotent Parquet sink 조합).
> - `ALTER TABLE ADD COLUMN` 시나리오에서 schema history 자동 복구 및 후속 이벤트 정상 처리 확인.
> - 배치 적재 단계는 데이터 규모(~50MB) 고려해 **pandas로 구현** (Spark 미사용, JVM 오버헤드 회피). Spark Structured Streaming은 checkpoint/상태관리가 필요한 스트리밍 단계에만 사용하여 **도구별 적합성 분리**.

**면접관 예상 질문**:
- "Replayer에서 시간 압축 비율은 어떻게 정했어요?" → 답할 수 있음
- "왜 batch에 Spark 안 썼어요?" → 답할 수 있음
- "Parquet 파티션 기준은 뭘로 잡았어요?" → 답할 수 있음

→ **모든 결정에 근거가 있음. 가점.**

---

## 8. 학습 노트

### 학습 포인트 1 — CDC의 본질
- **CDC = Change Data Capture**. DB 변경 이벤트를 실시간으로 포착하는 패턴.
- MySQL에선 binlog (row-based)가 단일 진실 원천.
- Debezium이 **slave replication protocol**을 흉내내서 binlog를 읽음.
- 따라서 MySQL 설정에 `log-bin`, `binlog-format=ROW`, `binlog-row-image=FULL` 필수.
- Debezium은 DB에 부담 없이 "또 다른 replica"처럼 보임 → 운영 DB에 붙여도 영향 최소.

### 학습 포인트 2 — Kafka Connect vs Kafka 자체
- Kafka: 메시지 브로커 (pub/sub)
- Kafka Connect: **Source/Sink 커넥터 프레임워크** — "DB에서 Kafka로", "Kafka에서 S3로" 등 integration을 표준화
- Debezium은 Kafka Connect의 **Source connector**
- 즉 파이프라인은: `MySQL → [Kafka Connect에서 Debezium plugin이 돌면서] → Kafka 토픽`

### 학습 포인트 3 — Debezium 이벤트 구조
각 이벤트는 JSON 형태로 발행된다:
```json
{
  "payload": {
    "op": "u",                    // c=create, u=update, d=delete, r=read(snapshot)
    "ts_ms": 1712345678000,       // Debezium 처리 시각
    "source": {
      "ts_ms": 1712345677500,     // 실제 DB 커밋 시각
      "table": "orders",
      "db": "ecommerce"
    },
    "before": { ... },            // 변경 전 row (UPDATE/DELETE만)
    "after":  { ... }             // 변경 후 row (CREATE/UPDATE만)
  }
}
```
- `source.ts_ms` vs `ts_ms` 차이가 **DB → Kafka 지연**
- Spark 처리 시점 - `ts_ms` = **end-to-end latency**

### 학습 포인트 4 — Exactly-once 의 비밀
Kafka는 원래 at-least-once. exactly-once 보장은:
1. **Consumer**: Kafka offset + sink 결과를 같은 트랜잭션에 커밋 (transactional write)
2. **Spark Structured Streaming**: checkpoint에 offset 기록 + idempotent sink (Parquet은 `overwrite` 기반이라 idempotent)
3. **전제**: sink가 멱등적이어야 함. 아무데나 "저장"만 한다고 exactly-once 되는 것 아님.

### 학습 포인트 5 — "이 프로젝트에 왜 이 도구?" 를 항상 답할 수 있어야
한 줄 요약:
> 도구는 스케일과 목적에 맞춰야 한다. 포트폴리오용이라고 큰 도구를 쓰면 오히려 감점이다.

### 학습 포인트 6 — 로컬 학습에서 증명 가능한 것 vs 불가능한 것

| 증명 가능 | 증명 불가능 |
|-----------|-------------|
| Correctness (exactly-once, 유실 0) | Throughput (msg/sec) |
| Resiliency (재시작 복구) | Scalability (수평 확장 효과) |
| Schema evolution 대응 | Real production 부하 |
| Latency (단일 경로) | Latency under load |
| Operability (모니터링, 로그) | SLA 달성률 |

로컬에선 **왼쪽 컬럼만** 측정하고 보고해야 한다. 오른쪽을 측정한다고 하면 거짓말이다.

---

## 다음 작업

- [ ] P0 CRITICAL 이슈 정리 (커넥터 JSON 통합, init.sql 단일화, `.env` 도입)
- [ ] `replay_orders.py` 설계 문서 작성
- [ ] Spark Streaming sink를 Parquet로 전환, checkpoint 볼륨화
- [ ] End-to-end latency 측정 코드 추가 (`ts_ms` 기반)
- [ ] 재시작 복구 테스트 시나리오 문서화

---

**이 문서는 살아있는 문서다.** 새 결정이 추가될 때마다 ADR-007, ADR-008... 로 append한다.
기각된 옵션도 지우지 않고 "왜 기각했는지" 남긴다 — 그게 학습 자료다.
