# de-cdc-streaming-pipeline — 이력서용 PAAR 정리

> **P**roblem → **A**nalysis → **A**ction → **R**esult 흐름으로 정리한 주니어 데이터 엔지니어 포트폴리오 성과 요약.
> 단순 "기능 구현 나열"이 아니라 **"어떤 문제가 있었고, 어떻게 분석했고, 왜 그 선택을 했고, 결과가 어떻게 달라졌는지"** 를 남긴다.

---

## 프로젝트 한 줄 요약

**MySQL → Debezium → Kafka → Spark Structured Streaming → Parquet** 로컬 CDC 파이프라인을 **Correctness · Resiliency · Operability** 관점으로 검증한 프로젝트.

- **Dataset**: Olist Brazilian E-Commerce (~35만 행, ~50MB)
- **Stack**: MySQL 8.0, Debezium 2.7.3, Kafka (Confluent 7.6.0), Spark 3.5.7, Docker Compose
- **차별점**: ADR 14개로 **"왜 이 결정을 했고 대안을 왜 기각했는지"** 를 모두 문서화

---

# 1. 정적 데이터셋으로 실시간 CDC 스트림 재현 — Event Replayer

## Problem

- Olist는 18개월치 **정적 CSV 덤프** → 그대로 Debezium에 붙이면 최초 스냅샷(`op=r`) 1회 이후 이벤트가 발생하지 않음
- "Streaming 파이프라인"이라 부르는데 실제로는 batch dump만 흐르는 상태 → **프로젝트 정당성 자체가 무너짐**
- Debezium의 `op=c`/`op=u` 연속 처리, consumer lag, end-to-end latency 같은 **streaming 특유의 지표 측정 불가**

## Analysis

| 대안 | 평가 |
|------|------|
| A. Faker 랜덤 생성 | 현실성 X, Olist 실제 트래픽 패턴 상실 |
| B. 고정 간격 주입 (1초/건) | 단순하나 원본 burst/lull 패턴 소실 |
| **C. timestamp 비례 시간 압축 재생** | 원본 burst/lull 패턴 보존, 시간축만 N배 압축 |
| D. Kafka Producer 직접 주입 | Debezium 경로 우회 → CDC 학습 목적 상실 |

## Action

C안 채택 근거: **원본 burst 패턴 보존 + MySQL→Debezium 경로 유지** → 이후의 모든 latency/복구/스키마 검증의 전제가 됨.

- `scripts/replay_orders.py` 구현
  - `order_purchase_timestamp` 정렬 후 이벤트 간 시간차를 압축 비율로 나눠 `time.sleep`
  - **`INSERT IGNORE` + autocommit** → Ctrl+C 후 재실행 idempotent, 건당 binlog 이벤트 생성 (배치 commit 금지)
  - `--include-updates` 옵션으로 `invoiced → delivered` + `shipped → canceled` 상태 전이 UPDATE 주입 → `op=u` 검증 경로 확보 (B1 취소율 지표의 선행 조건)
  - `--dry-run`, `--limit`, `--duration-minutes` CLI로 CI/검증 루프 지원

## Result

- **186.8일치 29,833건을 10분으로 압축 재생** (압축비 ~26,900x, 평균 ~50 events/s)
- Kafka 토픽에서 **연속 `op=c`/`op=u` 이벤트 발행 확인** → 이후 모든 검증의 선행 조건 확보
- 기본 10분 재생 기준 drift 10% 이내 → 로컬 단일 브로커 소화 상한 식별

---

# 2. 도구-스케일 불일치 해소 — pandas vs Spark 분리

## Problem

- 원본 코드는 50MB/35만 행 배치 적재에 Spark JDBC 사용 → **JVM 부팅 ~10초 + 코드 복잡 + pandas가 오히려 빠름**
- "Spark로 적재 시간 78% 단축" 류 허세 문장은 **검증 불가능** → 면접에서 역공받음

## Analysis

- 실측: 4개 테이블 합계 **~350k 행 / ~50MB** → 메모리에 편하게 로드되는 규모
- pandas / Spark JDBC / polars / Dask 비교
- Spark는 **checkpoint/상태관리가 필요한 streaming 단계**에만 적합

## Action

- **batch = pandas + SQLAlchemy, streaming = Spark Structured Streaming** 로 분리 (ADR-001)
- `load_mysql_spark.py` 제거, 중복 파일(`spark/` vs `spark-submit/spark/`) 정리
- DECISIONS.md에 **기각된 대안까지 남김** → "왜 Spark를 안 썼는지" 답변 가능
- ADR-009에서 "Spark Streaming도 사실 과하지만 exactly-once 학습 목적으로 유지" 자기 비판까지 문서화

## Result

- 배치 적재 JVM 오버헤드 제거, 코드 베이스 2파일 → 1파일
- 면접 질문 "이 스케일에 Spark 필요했나요?"에 **정량적 근거로 답변 가능**
- "도구별 적합성 분리"가 이력서 서술 포인트로 정착

---

# 3. End-to-end Latency 측정 체계 구축

## Problem

- ADR-004에서 **로컬 단일 브로커 throughput 측정은 거짓말**로 기각 → 그럼 무엇을 측정할 것인가
- 측정 방법을 잘못 고르면 "Debezium 내부 latency"만 보거나 "DB 커밋 시점"이 빠진 반쪽 수치가 됨

## Analysis

| 방법 | 커버 구간 | 문제 |
|------|-----------|------|
| `payload.ts_ms` vs `source.ts_ms` | Debezium 내부만 | Spark 처리 제외 |
| Kafka timestamp vs `processed_at` | Broker→Spark | DB 커밋 제외 |
| **`payload.ts_ms` vs Spark `processed_at`** | MySQL 커밋 → Parquet 저장 전체 | end-to-end, 구현 단순 |
| 별도 latency 토픽 | 정확하나 과한 설계 |

## Action

- `stream_cdc.py` 에 `.withColumn("processed_at", current_timestamp())` 추가 (**코드 변경 1줄**)
- `scripts/measure_latency.py` 로 Parquet 읽어 **p50/p95/p99** 집계
- INSERT/UPDATE 분리 집계로 이벤트 유형별 분포 차이까지 분석

## Result

```
전체 70건:   p50= 298ms  p95= 511ms  p99=1365ms
INSERT 40건: p50= 268ms  p95= 941ms  p99=1406ms
UPDATE 30건: p50= 298ms  p95= 301ms  p99= 301ms
```

- **p50 300ms, p95 500ms** 수치 확보 → 이력서 기재 가능
- p95 이상치 원인을 **Spark micro-batch 30s trigger** 로 특정 → "튜닝 여지" vs "설계 trade-off" 로 문서화
- INSERT vs UPDATE 분포 차이(replay 간격 편차 영향) 관찰

---

# 4. Exactly-once 증명 — Checkpoint 영구화 + 재시작 복구

## Problem

- 원본 코드: checkpoint를 `/tmp/spark_chk/` 에 저장 → 컨테이너 재시작 시 **유실** → earliest부터 재처리 → **중복 이벤트로 exactly-once 깨짐**
- "Spark Streaming은 exactly-once 입니다" 라는 말만으로는 면접에서 검증 불가

## Analysis

Exactly-once 보장 조건 재확인:
1. offset을 **persistent storage**에 저장
2. sink가 **idempotent** (Parquet은 committed offset 기반이라 동일 파일 재기록 방지 → 조건 충족)

## Action

- Checkpoint를 Docker 볼륨 `/workspace/checkpoints/` 로 이동 (ADR-005)
- `scripts/verify_restart_recovery.py` 구현: Spark kill → 재기동 → offset/row count 비교 자동화
- `.ivy2` 캐시 의존성 이슈 영구 해결 (jars/ 디렉토리 + `--jars` 플래그)

## Result

- 재시작 후 **offset 139325 → 139343** (earliest 재처리 0건)
- Parquet **91 → 109행** (+18건 정확)
- **이벤트 유실/중복 0건** 을 재현 가능한 스크립트로 증명 → 면접 시연 가능

---

# 5. Schema Evolution 무중단 대응

## Problem

- 운영 파이프라인에서 `ALTER TABLE ADD COLUMN` 은 불가피 → Spark를 재배포해야 하면 **downtime 발생**
- 구조화된 schema 방식으로 `stream_cdc.py` 를 짰다면 컬럼 추가 시마다 코드 수정 필요

## Analysis

- Debezium은 `schema-changes` 토픽에 DDL을 기록하고 내부 스키마를 자동 갱신 → **업스트림은 자동 처리**
- 다운스트림(Spark) 수용 방식
  - A. 구조화된 StructType 재정의 → 재배포 필요
  - **B. `payload.after` 를 JSON string 으로 저장 → 재배포 불필요**

## Action

- `payload.after` 를 `get_json_object` 로 **JSON string 컬럼**으로 저장 → Parquet 스키마 고정, 내용은 JSON 안에 보존
- `scripts/verify_schema_evolution.py` 구현
  - `ALTER TABLE ADD COLUMN is_late_delivery` + UPDATE 20건 주입
  - Kafka / Parquet 양쪽에서 신규 컬럼 반영 자동 검증

## Result

- ALTER 이후 20건 이벤트 모두 `is_late_delivery` 포함 확인 (**PASS**), Spark **재배포 0회**
- Trade-off 명시 기록
  - (-) 쿼리 시 `get_json_object` 필요, Parquet 압축률 감소
  - (+) **zero-downtime schema evolution**, 업/다운스트림 **결합도 최소화**

---

# 6. Data Quality 3계층 검증 + 숨은 timestamp 버그 발견

## Problem

- "파이프라인이 돈다" ≠ "데이터를 신뢰할 수 있다"
- 실제로 `orders.order_purchase_timestamp` **99.9%가 NULL** 이었으나 Spark는 정상 실행 → CDC 이벤트 타임스탬프 측정·정렬 자체가 무의미해지는 상황

## Analysis

**대안 비교**

| 방법 | 기각 이유 |
|------|-----------|
| Great Expectations | 이 규모에 과한 의존성 |
| dbt test | dbt 스택 없음 |
| **커스텀 3계층 스크립트** | 재현 가능, git 기록, 이 규모에 적합 |

**원인 분석**

```
CSV "2016-09-04T21:15:19.000Z" (UTC)
  → pandas (tz-aware Timestamp)
  → pymysql
  → MySQL DATETIME (tz 미지원)
  → 조용히 NULL 저장
```

## Action

- `scripts/check_dq.py` 구현
  - **Layer 1 (MySQL)**: PK null/중복, FK 고아, row count, 핵심 컬럼 null 비율
  - **Layer 2 (Parquet)**: op 분포, `after` JSON 파싱 실패율, schema evolution 컬럼 존재
  - **Layer 3 (Consistency)**: MySQL vs Parquet 커버리지 교차 검증
- `load_data.py` 에 `.dt.tz_convert(None)` 추가로 **근본 원인 수정**
- `scripts/fix_timestamps.py` 로 기존 데이터 UPDATE 복구 (`--dry-run` 포함)

## Result

```
Layer 1 (MySQL):   PASS 14 / FAIL 0
Layer 2 (Parquet): PASS  8 / FAIL 0
Layer 3 (Consist): WARN  2 / FAIL 0   ← 설계상 expected
최종:              PASS 22 / WARN 2 / FAIL 0
```

- `order_purchase_timestamp` NULL **69,608건 → 0건** 복구
- Layer 3 WARN까지 **"설계상 정상인지 버그인지"** 구분해서 문서화 → 운영자 마인드셋 증명

---

# 7. 보안·권한 정리 — Debezium 전용 유저, 자격증명 외부화

## Problem

- 원본 코드: `docker-compose.yml` / `load_data.py` / 커넥터 JSON에 **비밀번호 하드코딩**
- 커넥터 JSON 3개 충돌 (동일 `database.server.id`) → `docker compose up` 자체가 실패
- `appuser` 에 REPLICATION 권한 부여 시 **앱 유저 탈취 = binlog 전체 유출**

## Analysis

| 옵션 | 평가 |
|------|------|
| `appuser` 에 REPLICATION 권한 추가 | 앱/복제 유저 권한 섞임, 권한 최소화 원칙 위반 |
| **`debezium` 전용 유저 분리** | 권한 분리, 운영 관행, 면접 설명 가능 |
| Secrets Manager (Vault 등) | 로컬 학습 환경에 과잉 |

## Action

- `debezium` 유저 신규 생성 (REPLICATION SLAVE/CLIENT만), `appuser` 는 CRUD만 (ADR-007)
- `.env` + `register-mysql-debezium.json.template` + `envsubst` 조합
  → **커넥터 JSON에 비밀번호 주입되지만 git에는 커밋되지 않음**
- SQL table allowlist, 로그 비밀번호 노출 제거, DB 커넥션 try/finally 보장
- 커넥터 JSON 3개 → 1개로 통합, init.sql 2개 → 1개로 통합

## Result

- git 전체 **하드코딩 자격증명 0건**
- **권한 최소화 원칙을 SQL·코드로 증명** (`SHOW GRANTS` 로 검증 가능)
- `docker compose up` 한 번에 파이프라인 부팅 성공 (이전엔 커넥터 충돌로 실행 불가)

---

# 8. CDC 파이프라인을 "지표 제공" 서사로 재정의 — A1 운영 / B1 리스크 지표

## Problem

- v1 완료 시점의 Spark sink는 **Parquet raw dump** (`stream_cdc.py`) — 소비처 없음
- 무신사페이먼츠 공고 원문 "배치 파이프라인(CDC, 이벤트 스트리밍)을 통해 **핵심 지표를 제공**한 경험" — 주 동사는 "구축"이 아니라 **"제공·지원"**
- 제공·지원은 SELECT 주체(대시보드·서비스)가 없으면 성립 불가 → "Parquet에 뭘 했느냐"는 질문에 답이 약함

## Analysis

- 기존 자산 재활용 관점에서 옵션 비교 (ADR-013)
  1. Parquet 유지 + 별도 BI 레이어 — 새 컴포넌트 비용 큼
  2. Spark sink만 교체, 집계 테이블을 MySQL에 직접 — **채택**
  3. Kafka Streams 재작성 — 동작하는 Spark 자산 폐기, 손실 큼
- 지표 선택 기준: **"CDC가 없으면 이 지표가 성립하지 않는가"**
  - A1 (분당 주문 건수) — INSERT만 필요하므로 CDC 없어도 가능. 운영 지표 기본기
  - B1 (10분 취소율) — `order_status` 의 **변경**(created→canceled)을 포착해야 성립. INSERT-only 스트림으로 불가능 → **CDC 정당성이 지표 정의 자체에 내장**
- 기준선 실측 (n=69,674): delivered 96.634%, canceled 0.616% (429건), unavailable 0.805%
  → B1 알람 임계치 1.5~2% (기준선 2~3배) 설정 근거 확보

## Action

- Spark Structured Streaming job 2개 신설
  - `spark/stream_a1_orders.py` — `op='c'` 필터, **10초 tumbling** (ADR-014), `count(order_id)`
  - `spark/stream_b1_cancel_rate.py` — `op='c'` (분모) + `op='u' AND before.order_status != 'canceled' AND after.order_status = 'canceled'` (분자), **1분 sliding / 30초 slide** (ADR-014)
- MySQL sink 테이블 `minute_order_summary`, `ten_minute_cancel_rate` 신설
  - PK=`window_start`, `mysql.connector` REPLACE INTO를 `foreachBatch`에서 호출 (Spark JDBC는 upsert 미지원)
  - outputMode=`update`로 윈도우 진행 중 증가분을 대시보드에 즉시 반영
- Debezium envelope만 `from_json` 스키마로 정의 (before/after/op/ts_ms), 이외 필드는 무시
- S1~S5 단계별 구현 + 검증 체크리스트로 진행 (`docs/DIRECTION_v2.md` §5)

## Result

- A1: **35+ 윈도우** upsert (10초 tumbling, 3,000건 replay), `minute_order_summary` 정상 적재
- B1: `--include-updates` 3,000건 replay → **9개 윈도우에서 cancel_rate 0.18%~1.2%** 확인
  - `replay_orders.py`에 canceled UPDATE 주입 추가 (shipped→canceled 전이, 100% 비율)
  - Kafka 토픽 72,925건 (69,608 snapshot + 3,000 INSERT + 317 UPDATE)
- op='c' 필터로 **snapshot(op='r') 69,674건 집계 미반영** 확인 → 서비스 오픈 이후 실시간 이벤트만 계산
- ADR-014로 윈도우 크기를 압축 시간 기준으로 재조정 (A1: 1분→10초, B1: 10분→1분) — 원본 시간 기준 윈도우가 압축 환경에서 무의미해지는 문제 해결
- 면접 동사 전환: "CDC 파이프라인을 **구축**했다" → "CDC로 **운영·리스크 지표를 실시간 제공**한다"
- ADR-013·014 / DIRECTION_v2.md §3·§6·§7에 결정 근거·실측 수치 전수 기록

---

# 포트폴리오 차별 포인트 (면접 대응)

| # | 포인트 | 근거 |
|---|--------|------|
| 1 | **"왜"에 답하는 ADR 14개** | 채택/기각 이유 + 대안 비교 + trade-off 모두 기록 |
| 2 | **자기 비판 문서화** | "Spark도 사실 과했다"(ADR-009), "초기 계획 7개 중 3개 기각" 를 **지우지 않고 남김** |
| 3 | **로컬에서 측정 가능한 것만 측정** | throughput 대신 latency/correctness/resiliency (ADR-004) |
| 4 | **문제→분석→결정→검증 사이클** | 모든 Phase가 동일 구조로 정리 가능 |

---

# 이력서 문장 Before / After

### ❌ Before (검증 불가, 감점)
> "CSV → Parquet 전환으로 스토리지 82% 절감, Spark JDBC 병렬 적재로 적재 시간 78% 단축, Kafka partition 튜닝으로 consumer 처리량 3.2배 향상"

### ✅ After (근거 있음, 가점)
> Olist 기반 CDC 파이프라인 **Correctness 검증** 프로젝트.
> - `order_purchase_timestamp` 기반 **Event Replayer** 구현, 18개월치 주문 데이터를 10분으로 시간 압축 재생하여 **연속 CDC 이벤트 ~30,000건** 주입 (Debezium → Kafka → Spark)
> - End-to-end latency **p50 300ms, p95 500ms** 측정 체계 구축. p95 이상치는 Spark micro-batch 30s trigger 기인 — 설계 trade-off로 문서화 (ADR-010)
> - Kafka Connect / Spark Driver 강제 종료 재기동 시나리오에서 **이벤트 유실·중복 0건** 검증 (persistent checkpoint + idempotent Parquet sink)
> - `ALTER TABLE ADD COLUMN` 시나리오에서 Spark 재배포 없이 **zero-downtime schema evolution** 검증 (JSON string sink 설계)
> - MySQL → Parquet → Consistency **3계층 DQ 스크립트** 직접 구현 중 `tz-aware datetime → MySQL DATETIME` 호환 이슈로 인한 **99.9% NULL 버그를 식별·수정하고 69,608건 복구**
> - CDC 이벤트 기반 **운영 지표(10초 주문 건수) + 리스크 지표(1분 취소율)**를 MySQL 집계 테이블로 실시간 제공. B1 취소율은 `order_status` UPDATE 전이를 CDC로 포착 — **INSERT-only 스트림으로 불가능한 지표를 선정해 CDC 정당성을 내장** (ADR-013·014)
> - 데이터 규모(~50MB) 고려하여 배치 적재는 **pandas**, 스트리밍만 Spark 로 **도구별 적합성 분리** (ADR-001)
