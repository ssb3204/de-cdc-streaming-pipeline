from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, get_json_object

# 1) SparkSession
spark = (
    SparkSession.builder
    .appName("cdc-kafka-parquet")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2) Kafka 설정 (도커 내부 리스너)
# 토픽 네이밍: {topic.prefix}.{database}.{table}
# subscribePattern으로 4개 테이블(orders, order_items, customers, products) 전체 구독
# Phase 3: subscribe(단일 토픽) → subscribePattern(정규식, 전체 테이블)
BOOTSTRAP = "kafka:29092"
TOPIC_PATTERN = "ecommerce\\.ecommerce\\.(orders|order_items|customers|products)"

# Parquet 출력 경로 — /workspace = 프로젝트 루트 (docker-compose.yml 바인드 마운트)
# 컨테이너 재시작 후에도 호스트 파일시스템에 그대로 유지됨 (ADR-005)
OUTPUT_PATH = "/workspace/data/cdc_output"
CHECKPOINT_PATH = "/workspace/checkpoints/cdc_stream"

# 3) Kafka에서 스트리밍 읽기
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribePattern", TOPIC_PATTERN)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# 4) value(JSON)를 문자열로 변환하고 Debezium payload 추출
df = (
    raw.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("value").cast("string").alias("v")
    )
    .withColumn("op", get_json_object(col("v"), "$.payload.op"))
    .withColumn("ts_ms", get_json_object(col("v"), "$.payload.ts_ms"))
    .withColumn("before", get_json_object(col("v"), "$.payload.before"))
    .withColumn("after", get_json_object(col("v"), "$.payload.after"))
    .withColumn("processed_at", current_timestamp())
    .select("topic", "partition", "offset", "timestamp", "op", "ts_ms", "before", "after", "processed_at")
)

# 5) Parquet sink — topic별로 파티셔닝하여 테이블별 디렉토리로 분리
# /workspace/data/cdc_output/topic=ecommerce.ecommerce.orders/...
# /workspace/data/cdc_output/topic=ecommerce.ecommerce.order_items/...
query = (
    df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("topic")
    .start()
)

query.awaitTermination()
