import os
import pandas as pd
from sqlalchemy import create_engine

# =====================
# DB 접속 설정
# =====================
DB_USER = "appuser"      # docker-compose의 MYSQL_USER
DB_PASS = "apppass"      # docker-compose의 MYSQL_PASSWORD
DB_HOST = "127.0.0.1"
DB_PORT = 3307           # docker-compose에서 3307:3306으로 매핑
DB_NAME = "ecommerce"    # MYSQL_DATABASE

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=False, future=True)

# CSV 파일이 있는 디렉토리 (위에 제안한 구조 기준)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")


def load_table(csv_filename: str, table_name: str, parse_dates=None, chunksize: int = 10000):
    csv_path = os.path.join(DATA_DIR, csv_filename)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"{csv_path} 파일이 존재하지 않습니다.")

    print(f"\n=== [{table_name}] {csv_path} 적재 시작 ===")

    preview_df = pd.read_csv(csv_path, nrows=5)
    print(f"[{table_name}] 샘플 5행:")
    print(preview_df)

    total_rows = 0

    for chunk in pd.read_csv(csv_path, chunksize=chunksize, parse_dates=parse_dates):
        # 여기부터 추가
        if table_name == "products":
            chunk = chunk.rename(columns={
                "product_name_lenght": "product_name_length",
                "product_description_lenght": "product_description_length",
            })
        # 여기까지 추가

        rows_in_chunk = len(chunk)
        total_rows += rows_in_chunk

        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )
        print(f"[{table_name}] {rows_in_chunk}행 적재 (누적 {total_rows}행)")

    print(f"=== [{table_name}] 적재 완료 (총 {total_rows}행) ===")


def main():
    # 1. customers
    load_table(
        csv_filename="olist_customers_dataset.csv",
        table_name="customers",
        parse_dates=None
    )

    # 2. products
    load_table(
        csv_filename="olist_products_dataset.csv",
        table_name="products",
        parse_dates=None
    )

    # 3. orders
    load_table(
        csv_filename="olist_orders_dataset.csv",
        table_name="orders",
        parse_dates=[
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]
    )

    # 4. order_items
    load_table(
        csv_filename="olist_order_items_dataset.csv",
        table_name="order_items",
        parse_dates=[
            "shipping_limit_date",
        ]
    )

    print("\n=== 모든 테이블 초기 적재 완료 ===")



if __name__ == "__main__":
    main()
