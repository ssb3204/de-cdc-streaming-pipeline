# /workspace/spark-submit/spark/load_mysql_spark.py
# Spark로 Olist CSV를 MySQL(ecommerce)에 적재 (customers/products 전체 + orders/order_items는 70% split 폴더 적재)

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------- MySQL 접속 정보 ----------
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB", "ecommerce")
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    f"?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&rewriteBatchedStatements=true"
)
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"


# ---------- 데이터 경로 ----------
# (둘 다 있는 경우가 많아서, 존재하는 쪽을 자동 선택)
BASE_DIRS = [
    Path("/workspace/data"),
    Path("/workspace/spark-submit/data"),
]

SPLIT_DIR = Path("/workspace/spark-submit/data")  # split 결과 폴더는 여기 기준


def pick_path(rel: str) -> Path:
    for base in BASE_DIRS:
        p = base / rel
        if p.exists():
            return p
    raise FileNotFoundError(f"파일을 찾을 수 없음: {rel} (검색: {BASE_DIRS})")


def pick_split_dir(rel: str) -> Path:
    p = SPLIT_DIR / rel
    if not p.exists():
        raise FileNotFoundError(f"split 폴더를 찾을 수 없음: {p}")
    return p


def make_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("load_initial_data_spark")
        # 네트워크/타임아웃 조금 여유
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"JDBC_URL = {JDBC_URL}")
    return spark


def read_csv_file(spark: SparkSession, path: Path):
    # 단일 CSV 파일 읽기
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("multiLine", "false")
        .option("escape", "\"")
        .option("quote", "\"")
        .option("mode", "PERMISSIVE")
        .load(str(path))
    )


def read_csv_dir(spark: SparkSession, folder: Path):
    # 폴더 안 part-*.csv 읽기 (※ 여기서 format=csv를 반드시 줘야 함)
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("multiLine", "false")
        .option("escape", "\"")
        .option("quote", "\"")
        .option("mode", "PERMISSIVE")
        .load(str(folder))
    )


def ensure_cols(df, expected_cols: list[str]):
    # 컬럼 누락 시 null 컬럼 생성 + 순서 정렬
    for c in expected_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))
    return df.select(*expected_cols)


def cast_customers(df):
    cols = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ]
    df = ensure_cols(df, cols)
    return (
        df.withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast("int"))
    )


def cast_products(df):
    cols = [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    df = ensure_cols(df, cols)
    int_cols = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    for c in int_cols:
        df = df.withColumn(c, F.col(c).cast("int"))
    return df


def cast_orders(df):
    cols = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    df = ensure_cols(df, cols)

    ts_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    # Olist는 보통 "yyyy-MM-dd HH:mm:ss"
    for c in ts_cols:
        df = df.withColumn(c, F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss"))
    return df


def cast_order_items(df):
    cols = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ]
    df = ensure_cols(df, cols)

    df = df.withColumn("order_item_id", F.col("order_item_id").cast("int"))
    df = df.withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss"))

    # MySQL 스키마가 DECIMAL(10,2)라면 이게 제일 안전
    df = df.withColumn("price", F.col("price").cast("decimal(10,2)"))
    df = df.withColumn("freight_value", F.col("freight_value").cast("decimal(10,2)"))
    return df


def write_jdbc(df, table: str, partitions: int = 4):
    # 너무 많은 파티션으로 동시에 꽂으면 MySQL이 버티기 힘들 수 있어 적당히 제한
    dfw = df.repartition(partitions)

    (
        dfw.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", JDBC_DRIVER)
        .option("batchsize", "10000")
        .mode("append")
        .save()
    )


def show_preview(df, name: str):
    print(f"\n=== [{name}] 샘플 5행 ===")
    df.show(5, truncate=False)
    print(f"[{name}] 총 {df.count()}행")


def main():
    spark = make_spark()

    # 1) customers (전체)
    customers_path = pick_path("olist_customers_dataset.csv")
    customers = cast_customers(read_csv_file(spark, customers_path))
    show_preview(customers, "customers")
    write_jdbc(customers, "customers", partitions=4)

    # 2) products (전체)
    products_path = pick_path("olist_products_dataset.csv")
    products = cast_products(read_csv_file(spark, products_path))
    show_preview(products, "products")
    write_jdbc(products, "products", partitions=4)

    # 3) orders (70% split 폴더)
    orders70_dir = pick_split_dir("orders_initial_70")
    orders70 = cast_orders(read_csv_dir(spark, orders70_dir))
    show_preview(orders70, "orders_initial_70")
    write_jdbc(orders70, "orders", partitions=4)

    # 4) order_items (70% split 폴더)
    items70_dir = pick_split_dir("order_items_initial_70")
    items70 = cast_order_items(read_csv_dir(spark, items70_dir))
    show_preview(items70, "order_items_initial_70")
    write_jdbc(items70, "order_items", partitions=6)

    print("\nDONE")


if __name__ == "__main__":
    main()
