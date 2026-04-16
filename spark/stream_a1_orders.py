"""A1 — 10초 텀블링 주문 건수 실시간 집계.

설계 근거: docs/DIRECTION_v2.md §3 A1, §7 S2 결정.
윈도우 크기 근거: ADR-014 (압축 시간 기준 — replay 10분에 ~30건, 10초 윈도우로 세분화)

흐름:
  Kafka(ecommerce.ecommerce.orders)
    → Debezium envelope 파싱 (op, ts_ms, after.order_id)
    → INSERT(op='c')만 필터 (snapshot 'r' 제외 → 초기 70% 데이터 집계 방지)
    → event_time = ts_ms 기준 watermark 20초
    → 10초 tumbling window groupBy → count(order_id)
    → MySQL `minute_order_summary` 테이블에 REPLACE INTO (upsert)

outputMode=update: 윈도우 진행 중에도 증가하는 count를 매 배치 반영.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    window,
    current_timestamp,
    count as spark_count,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

BOOTSTRAP = "kafka:29092"
TOPIC = "ecommerce.ecommerce.orders"

MYSQL_URL = "jdbc:mysql://mysql:3306/ecommerce?useSSL=false&serverTimezone=UTC"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
TARGET_TABLE = "minute_order_summary"

CHECKPOINT_PATH = "/workspace/checkpoints/a1_orders_per_minute"

# Debezium envelope 중 우리가 쓰는 필드만 정의 (나머지는 from_json이 무시)
# ts_ms는 payload 바로 아래, order_id는 payload.after 아래에 있음.
AFTER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
])
PAYLOAD_SCHEMA = StructType([
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("after", AFTER_SCHEMA, True),
])
ENVELOPE_SCHEMA = StructType([
    StructField("payload", PAYLOAD_SCHEMA, True),
])


def write_batch_to_mysql(batch_df, batch_id: int) -> None:
    """foreachBatch 핸들러. 각 마이크로배치 결과를 MySQL에 REPLACE INTO.

    window_start를 PK로 두고 REPLACE INTO 의미(JDBC overwrite가 아닌 per-row merge)
    를 얻기 위해 JDBC append 모드 + (window_start, updated_at) 최신값으로 덮어쓰기.
    Spark JDBC는 REPLACE INTO를 직접 지원하지 않아 collect → executemany 방식 사용.
    """
    rows = batch_df.collect()
    if not rows:
        return

    import mysql.connector

    conn = mysql.connector.connect(
        host="mysql",
        port=3306,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database="ecommerce",
    )
    try:
        cur = conn.cursor()
        sql = (
            "REPLACE INTO minute_order_summary "
            "(window_start, window_end, order_count, updated_at) "
            "VALUES (%s, %s, %s, %s)"
        )
        payload = [
            (r["window_start"], r["window_end"], int(r["order_count"]), r["updated_at"])
            for r in rows
        ]
        cur.executemany(sql, payload)
        conn.commit()
        print(f"[A1 batch {batch_id}] upserted {len(payload)} window rows")
    finally:
        conn.close()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("a1-orders-per-minute")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), ENVELOPE_SCHEMA).alias("env"))
        .select(
            col("env.payload.op").alias("op"),
            col("env.payload.ts_ms").alias("ts_ms"),
            col("env.payload.after.order_id").alias("order_id"),
        )
        .filter(col("op") == "c")  # INSERT만. snapshot 'r' 및 UPDATE 'u' 제외
        .withColumn("event_time", from_unixtime(col("ts_ms") / 1000).cast("timestamp"))
    )

    aggregated = (
        parsed
        .withWatermark("event_time", "20 seconds")
        .groupBy(window(col("event_time"), "10 seconds"))
        .agg(spark_count("order_id").alias("order_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("order_count"),
            current_timestamp().alias("updated_at"),
        )
    )

    query = (
        aggregated.writeStream
        .outputMode("update")
        .foreachBatch(write_batch_to_mysql)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
