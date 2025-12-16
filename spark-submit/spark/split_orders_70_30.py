import os
import traceback
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------
# 경로 설정
# ---------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

ORDERS_FILE = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
ORDER_ITEMS_FILE = os.path.join(DATA_DIR, "olist_order_items_dataset.csv")

# Spark는 "파일"이 아니라 "폴더"로 쓰는 게 기본
ORDERS_70_DIR = os.path.join(DATA_DIR, "orders_initial_70")
ORDERS_30_DIR = os.path.join(DATA_DIR, "orders_future_30")
ITEMS_70_DIR = os.path.join(DATA_DIR, "order_items_initial_70")
ITEMS_30_DIR = os.path.join(DATA_DIR, "order_items_future_30")

ERROR_LOG_FILE = os.path.join(BASE_DIR, "error_log.txt")


def main():
    spark = (
        SparkSession.builder
        .appName("split_orders_70_30")
        .master("local[*]")  # 로컬 실행. 컨테이너/클러스터면 환경에 맞게 바꿔라.
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== 1. orders 70/30 분리 ===")
    if not os.path.exists(ORDERS_FILE):
        raise FileNotFoundError(f"{ORDERS_FILE} 파일이 없습니다.")

    orders = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(ORDERS_FILE)
    )

    # timestamp 파싱 (Olist 포맷이 보통 "yyyy-MM-dd HH:mm:ss")
    orders = orders.withColumn(
        "order_purchase_ts",
        F.to_timestamp(F.col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    total = orders.count()
    print("orders 총 행 수:", total)
    print("orders 컬럼:", orders.columns)

    cut = int(total * 0.7)
    print("컷(70% row 수):", cut)

    # 시간순 정렬 기준 row_number 부여 (동일 타임스탬프 tie-breaker로 order_id 추가)
    w = Window.orderBy(F.col("order_purchase_ts").asc(), F.col("order_id").asc())
    orders_ranked = orders.withColumn("rn", F.row_number().over(w))

    orders_70 = orders_ranked.filter(F.col("rn") <= F.lit(cut)).drop("rn")
    orders_30 = orders_ranked.filter(F.col("rn") > F.lit(cut)).drop("rn")

    print("70% 주문 수:", orders_70.count())
    print("30% 주문 수:", orders_30.count())

    # 시간 범위 출력
    r70 = orders_70.agg(
        F.min("order_purchase_ts").alias("min_ts"),
        F.max("order_purchase_ts").alias("max_ts")
    ).collect()[0]
    r30 = orders_30.agg(
        F.min("order_purchase_ts").alias("min_ts"),
        F.max("order_purchase_ts").alias("max_ts")
    ).collect()[0]

    print("70% 주문 범위:", r70["min_ts"], "~", r70["max_ts"])
    print("30% 주문 범위:", r30["min_ts"], "~", r30["max_ts"])

    # 저장 (폴더로 저장됨)
    (orders_70
     .drop("order_purchase_ts")  # 필요 없으면 제거(원본 컬럼 유지가 목적이면 drop하지 말 것)
     .coalesce(1)                # 단일 part로 줄이기(데이터 크면 비추)
     .write.mode("overwrite")
     .option("header", "true")
     .csv(ORDERS_70_DIR))

    (orders_30
     .drop("order_purchase_ts")
     .coalesce(1)
     .write.mode("overwrite")
     .option("header", "true")
     .csv(ORDERS_30_DIR))

    print(f"→ {ORDERS_70_DIR}/")
    print(f"→ {ORDERS_30_DIR}/")

    print("\n=== 2. order_items 70/30 분리 ===")
    if not os.path.exists(ORDER_ITEMS_FILE):
        raise FileNotFoundError(f"{ORDER_ITEMS_FILE} 파일이 없습니다.")

    items = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(ORDER_ITEMS_FILE)
    )

    print("order_items 총 행 수:", items.count())
    print("order_items 컬럼:", items.columns)

    initial_order_ids = orders_70.select("order_id").distinct()

    # 70%: initial_order_ids와 매칭되는 것만
    items_70 = items.join(initial_order_ids, on="order_id", how="inner")

    # 30%: initial_order_ids에 없는 것
    items_30 = items.join(initial_order_ids, on="order_id", how="left_anti")

    print("70%에 해당하는 order_items 수:", items_70.count())
    print("30%에 해당하는 order_items 수:", items_30.count())

    (items_70
     .coalesce(1)
     .write.mode("overwrite")
     .option("header", "true")
     .csv(ITEMS_70_DIR))

    (items_30
     .coalesce(1)
     .write.mode("overwrite")
     .option("header", "true")
     .csv(ITEMS_30_DIR))

    print(f"→ {ITEMS_70_DIR}/")
    print(f"→ {ITEMS_30_DIR}/")

    print("\n=== 완료: 70% 초기데이터 / 30% 미래데이터 분리 끝 ===")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        os.makedirs(os.path.dirname(ERROR_LOG_FILE), exist_ok=True)
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "=" * 60 + "\n")
            f.write(f"[{datetime.now().isoformat()}] split_orders_70_30.py 실행 중 오류 발생\n")
            traceback.print_exc(file=f)
        print(f"\n오류가 발생해서 로그를 {ERROR_LOG_FILE} 에 저장했다.")
