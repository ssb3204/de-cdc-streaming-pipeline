# 프로젝트 방향 v2 — CDC 기반 실시간 운영·리스크 지표 제공

> 작성일: 2026-04-15
> 상태: 완료 (A1 + B1 지표 구현·검증 완료)

---

## 1. 방향 전환 요약

| 항목 | v1 (기존) | v2 (신규) |
|------|----------|----------|
| 목적 문장 | "CSV 70:30 분할로 Kafka CDC 작동을 확인" | "주문 DB 변경을 CDC로 포착해 운영·리스크 지표를 실시간 제공" |
| Spark sink | Parquet raw dump | 집계 테이블 (A1, B1) |
| Replayer 의미 | 스트리밍 테스트용 주입기 | 실시간 신규 주문·환불 이벤트 시뮬레이터 |
| 70% 초기 데이터 의미 | Debezium snapshot용 비어있지 않은 테이블 | 서비스 오픈 시점 기존 주문 이력 |
| 면접 동사 | "CDC 파이프라인을 구축했다" | "CDC로 운영 지표를 제공·지원한다" |

**전환 범위**: Spark Streaming job의 출력 로직만 교체. 새 서비스·새 컨테이너 없음.

---

## 2. 근거

### 2.1 공고 인용 (출처: `docs/kafka_requirements_from_job_postings.md`)

**무신사페이먼츠 (Data Engineer) 우대사항 원문**:
> "Kafka Connect, Kafka Streams 등 Kafka 에코시스템에 대한 깊은 이해와 활용 경험"
> "배치 파이프라인(CDC, 이벤트 스트리밍)을 통해 핵심 지표를 제공한 경험"

**핵심 동사**: "**구축**" (일반 공고) vs "**제공·지원**" (무신사페이먼츠) — 제공·지원은 소비처(지표)가 있어야 성립.

### 2.2 희귀성 근거 (출처: 동 문서 섹션 4 "핵심 인사이트")

> "CDC 연동 경험은 희귀 포인트 — Kafka Connect + Debezium + binlog 기반 CDC를 end-to-end로 다룬 신입 포트폴리오는 거의 없음"

→ 현재 프로젝트가 가진 기존 자산(Debezium+Kafka Connect+binlog) 위에 지표 소비 레이어를 얹으면 차별점이 상승.

### 2.3 매칭 공고 도메인 (동 문서 섹션 3)

- 무신사페이먼츠: 핀테크·커머스 결제
- 배달의민족(우아한형제들): 커머스 (CDC 우대 명시)
- 마이리얼트립, 펫프렌즈: 커머스

→ Olist(브라질 이커머스 주문 데이터)와 도메인 일치.

---

## 3. 지표 정의

### A1 — 분당 주문 건수·금액 (운영 지표)

| 항목 | 값 | 근거 |
|------|----|----|
| 이벤트 소스 | `orders` 테이블 INSERT | Replayer가 `orders`에 신규 row INSERT |
| 집계 윈도우 | **확인 필요** (후보: 1분 텀블링) | 운영 대시보드 일반 관례 — 확정 전 검토 필요 |
| 집계 필드 | `count(order_id)`, `sum(oi.price)` | order_items와 JOIN 필요 |
| JOIN 전략 | **확인 필요** | order_items도 CDC로 들어오는지 현재 설정 점검 필요 |
| 출력 스키마 (초안) | `window_start, window_end, order_count, total_amount` | |

### B1 — 실시간 취소율 (리스크 지표)

| 항목 | 값 | 근거 |
|------|----|----|
| 이벤트 소스 | `orders.order_status` UPDATE 이벤트 | Replayer `--include-updates` 옵션과 1:1 매칭 |
| 집계 윈도우 | **확인 필요** (후보: 10분 슬라이딩) | 이상 탐지 일반 관례 — 확정 전 검토 필요 |
| 집계 공식 | `count(status='canceled') / count(all orders)` | Olist `order_status` 값 범위: created/approved/invoiced/processing/shipped/delivered/canceled/unavailable (init.sql 참조 — 실제 데이터 분포는 **확인 필요**) |
| 출력 스키마 (초안) | `window_start, window_end, total, canceled, cancel_rate` | |
| 알람 임계치 | **확인 필요** | 올리스트 데이터의 기준선 취소율을 먼저 측정 후 설정 |

### B1이 CDC를 정당화하는 이유

- B1은 `order_status`의 **변경**(created→canceled)을 포착해야 함
- INSERT-only 스트림이면 초기 status(보통 created)만 받고 canceled 전환을 놓침
- CDC(binlog UPDATE 이벤트)가 없으면 B1은 배치로 MySQL을 주기적으로 재조회해야 함 → 실시간성 상실
- 즉, **B1은 CDC가 존재해야만 실시간으로 성립하는 지표** → "왜 CDC를 썼는가"에 대한 답이 지표 자체에 내장됨

---

## 4. 확인 필요 목록 (추측 금지)

작업 진행 중 다음 항목은 실측 또는 사용자 확인으로 채울 것:

1. ~~**Olist `orders` 데이터의 실제 취소율 기준선**~~ — ✅ §6.2에서 해소 (0.616%)
2. ~~**order_status 값 분포**~~ — ✅ §6.1에서 해소 (canceled만 사용, §7 결정)
3. ~~**order_items가 Debezium CDC에 포함되는지**~~ — ✅ §6.3에서 해소 (157,436건 확인)
4. ~~**윈도우 크기(1분/10분) 확정**~~ — ✅ ADR-014에서 해소 (A1: 10초 tumbling, B1: 1분 sliding/30초 slide)
5. ~~**sink 저장소 선택**~~ — ✅ §7에서 해소 (MySQL 집계 테이블)
6. ~~**Replayer 압축율과 윈도우 크기의 상호작용**~~ — ✅ ADR-014에서 해소 (10분 replay 기준 세분화)

---

## 5. 구현 소단위 (CLAUDE.md "최하단부터, 소단위" 규칙)

> 각 단계 완료 시 검증 후 다음으로 진행. 각 단계별 근거·수치는 이 문서 또는 별도 섹션에 추가 기록.

- [x] **S0. 현재 Spark job 구조 파악** — 입력 토픽, 현재 sink, 스키마 (2026-04-16)
- [x] **S1. Olist 데이터 기준선 측정** — orders 취소율, status 분포, order_items CDC 포함 여부 → §6 참조 (2026-04-16)
- [x] **S2. 윈도우·임계치·sink 확정** — §7 참조 + ADR-014 윈도우 재조정 (2026-04-16)
- [x] **S3. A1 집계 로직 구현** — `spark/stream_a1_orders.py`, 10초 tumbling count, MySQL `minute_order_summary` sink (2026-04-16)
- [x] **S3 검증** — batch 0 16→35+ 윈도우 upsert 확인 (ADR-014 적용 후 10초 윈도우 세분화)
- [x] **S4. B1 집계 로직 구현** — `spark/stream_b1_cancel_rate.py`, 1분 sliding(30초 slide), MySQL `ten_minute_cancel_rate` sink (2026-04-16)
- [x] **S4 검증 (1차)** — batch 0 73 윈도우 upsert 성공, canceled 0 (Kafka에 UPDATE 이벤트 없음 — `--include-updates` 주입 후 재검증 대기)
- [x] **S4 검증 (2차)** — `--include-updates` 3,000건 replay 후 B1 cancel_rate > 0 확인: 9개 윈도우에서 0.18%~1.2% 범위 (2026-04-16)
- [x] **S5. 문서 반영** — DECISIONS.md ADR-013·ADR-014 추가, DIRECTION_v2.md 전면 갱신 (2026-04-16)

---

## 6. S1 기준선 실측 결과 (2026-04-16)

MySQL `ecommerce.orders` 전수 조사 (n=69,674).

### 6.1 order_status 분포

| status | count | 비율 |
|--------|------:|-----:|
| delivered | 67,328 | 96.634% |
| shipped | 815 | 1.170% |
| unavailable | 561 | 0.805% |
| canceled | 429 | 0.616% |
| processing | 290 | 0.416% |
| invoiced | 244 | 0.350% |
| created | 5 | 0.007% |
| approved | 2 | 0.003% |

### 6.2 B1 기준선 취소율

- **canceled / total = 429 / 69,674 = 0.616%**
- 유사 실패 상태 `unavailable` 0.805% (B1 정의 시 별도 고려 대상)

### 6.3 order_items CDC 포함 검증

- Kafka 토픽 `ecommerce.ecommerce.order_items` offset: **157,436건**
- Debezium이 order_items 변경을 정상 emit → **A1의 `sum(price)` 집계 성립**

---

## 7. S2 결정 사항 (2026-04-16)

| 항목 | 결정 | 근거 |
|------|------|------|
| Sink 저장소 | **MySQL 집계 테이블** (`metrics_a1_orders_per_minute`, `metrics_b1_cancel_rate`) | "제공·지원" 동사 정당화 — SQL 대시보드가 직접 SELECT하는 소비 스토리 성립. Parquet은 소비처 답변 약함 |
| A1 윈도우 | ~~1분 tumbling~~ → **10초 tumbling** (ADR-014) | 압축 시간 기준(10분 replay, ~30건/10초) 세분화. 원본 시간 기준 1분은 압축 시 빈 윈도우 多 |
| B1 윈도우 | ~~10분 sliding, slide 1분~~ → **1분 sliding, slide 30초** (ADR-014) | 비율 지표 분모 안정 + 압축 시간에 맞춘 세분화. 원본 10분 윈도우는 압축 시 전체 데이터를 한 윈도우에 담음 |
| B1 정의 | `canceled`만 | "취소율" 직관성. unavailable은 INSERT 시점에 결정되는 경우 多 → UPDATE 포착 명분 약함. 확장은 추후 컬럼 추가로 간단 |
| B1 알람 임계치 | 1.5~2% (기준선 0.616%의 2~3배) | S3/S4 검증 시 재조정 |
| Replayer 압축율 상호작용 | S3/S4 검증 중 재평가 | 현 단계에서 조정 비용 낮음 |

---

## 8. 변경 이력

- 2026-04-15: v2 방향 확정, A1+B1 조합 선택 (사용자 합의)
- 2026-04-16: S1 실측 완료, S2 결정 확정 (Sink=MySQL, A1=1분 tumbling, B1=10분 sliding, B1=canceled만)
- 2026-04-16: S3 A1 구현·검증 완료 (16 윈도우/740 orders), S4 B1 구현·1차 검증 완료 (73 윈도우, canceled 0), S5 문서 반영 완료 (ADR-013 추가)
- 2026-04-16: ADR-014 윈도우 크기 재조정 (A1: 10초 tumbling, B1: 1분 sliding/30초 slide) — 압축 시간 기준 세분화
- 2026-04-16: `replay_orders.py`에 canceled UPDATE 주입 로직 추가 (shipped→canceled 전이). B1 2차 검증 완료: `--include-updates` 3,000건 replay, 9개 윈도우에서 cancel_rate 0.18%~1.2% 확인. 확인 필요 목록 전항목 해소, S0~S5 전체 완료
