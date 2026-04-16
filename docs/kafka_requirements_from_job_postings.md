# 서울 데이터 엔지니어 채용공고 Kafka 요구사항 분석
> 2025~2026년 서울 기준 신입/인턴/전환형 인턴 공고 분석 결과

---

## 1. Kafka 언급 위치 패턴

| 공고 유형 | Kafka 위치 | 특징 |
|-----------|-----------|------|
| 순수 신입 (그립컴퍼니, 아이디어스/백패커 등) | 거의 미언급 | Python, SQL, CS 기초, 클라우드 기초 중심 |
| 주니어/전환형 인턴 (카카오스타일, 레브잇, 펫프렌즈 등) | 우대사항 | "Hadoop, Kafka, Spark, Flink 등 빅데이터 프레임워크" 묶음 형태 |
| 0~3년 수용 공고 (무신사페이먼츠, 데브시스터즈, 다우기술 등) | 자격요건 또는 핵심 우대 | Kafka 단독 명시, 세부 에코시스템까지 요구 |

---

## 2. Kafka 세부 기능별 요구 빈도

| 기능 | 자격요건 | 우대사항 | 비고 |
|------|---------|---------|------|
| Kafka 기반 스트리밍 파이프라인 구축/운영 | ✅ | ✅ | 주니어 이상 공고 대부분 등장 |
| Kafka + Spark Structured Streaming 연동 | — | ✅ | 주니어/전환 인턴 공고 다수 |
| Kafka Connect (Source/Sink Connector) | — | ✅ | 무신사페이먼츠, 하이퍼커넥트 계열 |
| Kafka Streams | — | ✅ | 레브잇, 데브시스터즈 |
| Schema Registry | — | ✅ | 레브잇, 무신사페이먼츠 |
| CDC 연동 (Debezium 포함) | — | ✅ | 무신사페이먼츠, 배달의민족(우아한형제들) |
| AWS MSK 등 관리형 Kafka | — | ✅ | 클라우드 중심 공고 |
| Consumer lag 모니터링/운영 | — | ✅ | 데이터 플랫폼팀 공고 |

---

## 3. 공고별 실제 문구 발췌

**레브잇 (Data Platform Engineer)**
- 자격요건: "빅데이터 프레임워크(Hadoop, Kafka, Spark, Flink 등)의 활용 경험이 있으신 분"
- 기술스택: Kafka, Schema Registry, Kafka Streams

**무신사페이먼츠 (Data Engineer)**
- 자격요건: "Kafka, AWS MSK 등 스트리밍 플랫폼을 활용한 데이터 파이프라인 구축 경험이 있으신 분"
- 우대사항: "Kafka Connect, Kafka Streams 등 Kafka 에코시스템에 대한 깊은 이해와 활용 경험"
- 우대사항: "배치 파이프라인(CDC, 이벤트 스트리밍)을 통해 핵심 지표를 제공한 경험"

**데브시스터즈 (Data Platform Software Engineer)**
- 기술스택: Kafka, Kafka Streams, Spark, Delta Lake

**다우기술 (금융 데이터 엔지니어)**
- 기술스택: Kafka, ElasticSearch, Airflow
- 자격요건에 Kafka 포함된 스택 명시

**마이리얼트립 (데이터 엔지니어)**
- 우대사항: "Hadoop, Spark, Kafka 등의 데이터플랫폼 관련 경험"
- 우대사항: "실시간 데이터 처리 경험(druid, spark streaming 등)"

**펫프렌즈 (데이터 엔지니어)**
- 우대사항: "대용량 분산 시스템(Hadoop, Spark, Kafka, Hive 등) 운영 경험"

---

## 4. 핵심 인사이트

1. **신입 공고에서 Kafka 단독 자격요건은 드물다** — 순수 신입 공고는 Kafka보다 Python/SQL/CS 기초를 봄
2. **우대사항에서 가장 많이 나오는 조합** — Kafka + Spark Streaming + 파이프라인 구축 경험
3. **CDC 연동 경험은 희귀 포인트** — Kafka Connect + Debezium + binlog 기반 CDC를 end-to-end로 다룬 신입 포트폴리오는 거의 없음
4. **Kafka Streams, Schema Registry는 경력 우대** — 신입 포트폴리오에서 없어도 감점 요소 아님
5. **"CDC, 이벤트 스트리밍으로 지표 제공 경험" 우대 공고 증가 추세** — 무신사페이먼츠 등 핀테크/커머스 플랫폼 중심
