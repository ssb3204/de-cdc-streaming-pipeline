# 💾 Real-time CDC Streaming Pipeline (MySQL → Debezium → Kafka)

## 📌 프로젝트 개요
이 프로젝트는 MySQL의 변경 데이터를 실시간으로 캡처(CDC; Change Data Capture)하여  
Debezium → Kafka → Consumer로 전달하는 **스트리밍 데이터 파이프라인**을 구축하는 것을 목표로 합니다.

1일차에는 **실시간 데이터 수집(ingestion) 파트 전체를 구축**했습니다.

---

## 🚀 1일차 진행 내용 요약

### 1. GitHub 저장소 생성 및 로컬 개발 환경 구축
- 리포지토리 생성
- 로컬 PC로 `git clone`
- `work` 브랜치 생성 후 개발 진행

---

### 2. Docker 기반 인프라 구성
`docker-compose.yml`을 활용하여 다음 컨테이너들을 구성:

- **Zookeeper**
- **Kafka Broker**
- **MySQL (init.sql 자동 실행)**
- **Kafka Connect (Debezium)**

문제 해결:
- Debezium 이미지가 dockerhub에서 quay.io로 이동 → 경로 수정
- Kafka PLAINTEXT_HOST 충돌 문제 해결
- Kafka listener 설정 정상화

---

### 3. MySQL 초기 DB 스키마 자동 구성
- MySQL 컨테이너 시작 시 `init.sql` 자동 실행
- `ecommerce` DB 생성
- 주요 테이블(customers, products, orders, order_items) 생성

---

### 4. Debezium MySQL Connector 설정 및 등록
- `mysql-connector.json` 생성
- Debezium 2.7 규칙에 맞게 설정 수정  
  (`database.history.kafka.*` → `schema.history.internal.kafka.*`)
- REST API를 통해 커넥터 정상 등록
- 커넥터 상태 확인 (`RUNNING`)

---

### 5. Kafka CDC 이벤트 검증
- Kafka 컨테이너 내부에서 토픽 생성 확인
- `kafka-console-consumer` 로 orders 토픽 실시간 모니터링
- MySQL에서 INSERT / UPDATE / DELETE 발생 시
  → Debezium이 binlog 감지
  → Kafka로 CDC JSON 이벤트 전송
  → Consumer에서 실시간 확인 성공

예시 이벤트(JSON):
```json
{
  "before": null,
  "after": {
    "id": 2,
    "customer_id": 1,
    "total_amount": 99000.0,
    "status": "CREATED",
    "created_at": "2025-12-04T09:58:40Z",
    "updated_at": "2025-12-04T09:58:40Z"
  },
  "op": "c",
  "source": { ... },
  "ts_ms": 1764842320421
}
