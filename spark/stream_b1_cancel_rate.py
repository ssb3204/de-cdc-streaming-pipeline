"""B1 — 1분 슬라이딩 취소율 실시간 집계.

설계 근거: docs/DIRECTION_v2.md §3 B1, §7 S2 결정.
윈도우 크기 근거: ADR-014 (압축 시간 기준 — replay 10분에 ~30건, 1분 윈도우/30초 slide로 세분화)

정의:
  분모(total_orders): 윈도우 내 신규 주문 INSERT 수 (op='c')
  분자(canceled_orders): 윈도우 내 canceled로의 상태 전이 UPDATE 수
                        (op='u' AND before.order_status != 'canceled'
                         AND after.order_status = 'canceled')
  cancel_rate = canceled_orders / total_orders

흐름:
  Kafka(ecommerce.ecommerce.orders)
    → Debezium envelope 파싱 (op, ts_ms, before.order_status, after.order_status, after.order_id)
    → 이벤트를 두 부류로 라벨링 (is_new=1 for INSERT, is_canceled=1 for canceled 전이)
    → event_time = ts_ms 기준 watermark 1분 (비율 지표이므로 INSERT보다 보수적으로)
    → 1분 슬라이딩 윈도우 (slide=30초) groupBy → sum/sum/ratio
    → MySQL `ten_minute_cancel_rate` 테이블에 REPLACE INTO (upsert)

outputMode=update: 윈도우 진행 중에도 비율 변동을 매 배치 반영.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    when,
    lit,
    window,
    current_timestamp,
    sum as spark_sum,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

BOOTSTRAP = "kafka:29092"
TOPIC = "ecommerce.ecommerce.orders"

MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
TARGET_TABLE = "ten_minute_cancel_rate"

CHECKPOINT_PATH = "/workspace/checkpoints/b1_cancel_rate_10min"

# Debezium envelope — before/after 모두에서 order_status 필요
STATE_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_status", StringType(), True),
])
PAYLOAD_SCHEMA = StructType([
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("before", STATE_SCHEMA, True),
    StructField("after", STATE_SCHEMA, True),
])
ENVELOPE_SCHEMA = StructType([
    StructField("payload", PAYLOAD_SCHEMA, True),
])


def write_batch_to_mysql(batch_df, batch_id: int) -> None:
    """foreachBatch 핸들러. REPLACE INTO로 per-window 최신값 upsert."""
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
            "REPLACE INTO ten_minute_cancel_rate "
            "(window_start, window_end, total_orders, canceled_orders, cancel_rate, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )
        payload = [
            (
                r["window_start"],
                r["window_end"],
                int(r["total_orders"]),
                int(r["canceled_orders"]),
                float(r["cancel_rate"]),
                r["updated_at"],
            )
            for r in rows
        ]
        cur.executemany(sql, payload)
        conn.commit()
        print(f"[B1 batch {batch_id}] upserted {len(payload)} window rows")
    finally:
        conn.close()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("b1-cancel-rate-10min")
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
            col("env.payload.before.order_status").alias("before_status"),
            col("env.payload.after.order_status").alias("after_status"),
        )
        .withColumn("event_time", from_unixtime(col("ts_ms") / 1000).cast("timestamp"))
        .withColumn(
            "is_new",
            when(col("op") == "c", lit(1)).otherwise(lit(0)),
        )
        .withColumn(
            "is_canceled",
            when(
                (col("op") == "u")
                & (col("before_status") != "canceled")
                & (col("after_status") == "canceled"),
                lit(1),
            ).otherwise(lit(0)),
        )
        .filter((col("is_new") == 1) | (col("is_canceled") == 1))
    )

    aggregated = (
        parsed
        .withWatermark("event_time", "1 minute")
        .groupBy(window(col("event_time"), "1 minute", "30 seconds"))
        .agg(
            spark_sum("is_new").alias("total_orders"),
            spark_sum("is_canceled").alias("canceled_orders"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_orders"),
            col("canceled_orders"),
            when(col("total_orders") > 0,
                 col("canceled_orders") / col("total_orders"))
                .otherwise(lit(0.0))
                .alias("cancel_rate"),
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
