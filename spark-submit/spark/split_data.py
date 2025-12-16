import os
import pandas as pd
import traceback
from datetime import datetime

# ---------------------------
# 경로 설정
# ---------------------------
# 이 파일이 spark 폴더에 있다고 가정:
# de-cdc-streaming-pipeline/
#   data/
#   spark/
#     split_orders_70_30.py (현재 파일)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

ORDERS_FILE = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
ORDER_ITEMS_FILE = os.path.join(DATA_DIR, "olist_order_items_dataset.csv")

# 출력 파일 이름
ORDERS_70_FILE = os.path.join(DATA_DIR, "orders_initial_70.csv")
ORDERS_30_FILE = os.path.join(DATA_DIR, "orders_future_30.csv")
ITEMS_70_FILE = os.path.join(DATA_DIR, "order_items_initial_70.csv")
ITEMS_30_FILE = os.path.join(DATA_DIR, "order_items_future_30.csv")

# 에러 로그 파일 (프로젝트 루트에 생성)
ERROR_LOG_FILE = os.path.join(BASE_DIR, "error_log.txt")


def main():
    # ---------------------------
    # 1. orders 70% / 30% 분리
    # ---------------------------
    print("=== 1. orders 70/30 분리 ===")
    if not os.path.exists(ORDERS_FILE):
        raise FileNotFoundError(f"{ORDERS_FILE} 파일이 없습니다.")

    # 주문 데이터 로드 (시간 컬럼 파싱)
    orders = pd.read_csv(
        ORDERS_FILE,
        parse_dates=["order_purchase_timestamp"],
    )

    print("orders 총 행 수:", len(orders))
    print("orders 컬럼:", list(orders.columns))

    # 시간순 정렬 (오래된 주문 → 최신 주문)
    orders = orders.sort_values("order_purchase_timestamp")

    total = len(orders)
    cut = int(total * 0.7)  # 아래 70%, 위 30%

    orders_70 = orders.iloc[:cut].copy()   # 과거 70%
    orders_30 = orders.iloc[cut:].copy()   # 미래 30%

    print("70% 주문 수:", len(orders_70))
    print("30% 주문 수:", len(orders_30))

    # sanity check
    assert len(orders_70) + len(orders_30) == total

    # 시간 범위 확인 (참고용)
    print("70% 주문 범위:",
          orders_70["order_purchase_timestamp"].min(),
          "~",
          orders_70["order_purchase_timestamp"].max())
    print("30% 주문 범위:",
          orders_30["order_purchase_timestamp"].min(),
          "~",
          orders_30["order_purchase_timestamp"].max())

    # CSV로 저장
    orders_70.to_csv(ORDERS_70_FILE, index=False)
    orders_30.to_csv(ORDERS_30_FILE, index=False)
    print(f"→ {ORDERS_70_FILE}")
    print(f"→ {ORDERS_30_FILE}")

    # ---------------------------
    # 2. order_items 70% / 30% 분리
    # ---------------------------
    print("\n=== 2. order_items 70/30 분리 ===")
    if not os.path.exists(ORDER_ITEMS_FILE):
        raise FileNotFoundError(f"{ORDER_ITEMS_FILE} 파일이 없습니다.")

    items = pd.read_csv(ORDER_ITEMS_FILE)

    print("order_items 총 행 수:", len(items))
    print("order_items 컬럼:", list(items.columns))

    # 70% 주문에 해당하는 order_id 집합
    initial_order_ids = set(orders_70["order_id"])

    items_70 = items[items["order_id"].isin(initial_order_ids)].copy()
    items_30 = items[~items["order_id"].isin(initial_order_ids)].copy()

    print("70%에 해당하는 order_items 수:", len(items_70))
    print("30%에 해당하는 order_items 수:", len(items_30))
    print("합:", len(items_70) + len(items_30), "(원래:", len(items), ")")

    # CSV로 저장
    items_70.to_csv(ITEMS_70_FILE, index=False)
    items_30.to_csv(ITEMS_30_FILE, index=False)
    print(f"→ {ITEMS_70_FILE}")
    print(f"→ {ITEMS_30_FILE}")

    print("\n=== 완료: 70% 초기데이터 / 30% 미래데이터 분리 끝 ===")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # 에러 발생 시 error_log.txt에 스택 트레이스 저장
        os.makedirs(os.path.dirname(ERROR_LOG_FILE), exist_ok=True)
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "=" * 60 + "\n")
            f.write(f"[{datetime.now().isoformat()}] split_orders_70_30.py 실행 중 오류 발생\n")
            traceback.print_exc(file=f)
        print(f"\n오류가 발생해서 로그를 {ERROR_LOG_FILE} 에 저장했다.")
        # 필요하면 여기서 다시 raise 해서 바로 에러 보이게 할 수도 있음
        # raise
