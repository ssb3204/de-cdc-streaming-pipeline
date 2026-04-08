from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, get_json_object

# 1) SparkSession
spark = (
    SparkSession.builder
    .appName("cdc-kafka-console")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2) Kafka 설정 (도커 내부 리스너)
# 토픽 네이밍: {topic.prefix}.{database}.{table}
# topic.prefix는 Debezium 커넥터 설정의 "topic.prefix": "ecommerce"
# Phase 1b(B5): 존재하지 않는 cdc_test -> 실존 테이블 orders로 수정
# Phase 3에서 subscribePattern으로 4개 테이블 전체 구독으로 확장 예정
BOOTSTRAP = "kafka:29092"
TOPIC = "ecommerce.ecommerce.orders"

# 3) Kafka에서 스트리밍 읽기
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# 4) value(JSON)를 문자열로 변환하고 Debezium payload 일부만 추출
df = (
    raw.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        expr("CAST(key AS STRING)").alias("k"),
        expr("CAST(value AS STRING)").alias("v")
    )
    .withColumn("op", get_json_object(col("v"), "$.payload.op"))
    .withColumn("ts_ms", get_json_object(col("v"), "$.payload.ts_ms"))
    .withColumn("before", get_json_object(col("v"), "$.payload.before"))
    .withColumn("after", get_json_object(col("v"), "$.payload.after"))
    .select("topic", "partition", "offset", "timestamp", "op", "ts_ms", "before", "after")
)

# 5) 콘솔로 출력 (디버그용)
query = (
    df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/spark_chk/cdc_console_cdc_test")
    .start()
)

query.awaitTermination()
