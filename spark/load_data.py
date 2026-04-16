import glob
import logging
import os
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

# 프로젝트 루트의 .env 자동 로드 (없어도 스킵)
try:
    from dotenv import load_dotenv
    _ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(_ROOT, ".env"))
except ImportError:
    pass  # python-dotenv 미설치 시 OS 환경변수에서 직접 읽음

# =====================
# DB 접속 설정 (환경변수에서 읽음, 기본값은 로컬 docker-compose와 일치)
# =====================
DB_USER = os.environ["MYSQL_USER"]
DB_PASS = os.environ["MYSQL_PASSWORD"]
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ.get("MYSQL_DATABASE", "ecommerce")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=False, future=True)

# split_orders_70_30.py(Spark)가 생성한 데이터 디렉토리
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "spark-submit", "data")

# 테이블별 컬럼 rename 매핑 (CSV 컬럼명 → DB 컬럼명)
# Olist 원본 CSV와 init.sql 모두 오타(lenght)를 사용하므로 rename 불필요.
# CSV가 정규 컬럼명(length)으로 저장된 경우에만 DB 오타에 맞춰 역매핑.
_COLUMN_RENAMES: dict[str, dict[str, str]] = {
    "products": {
        "product_name_length": "product_name_lenght",
        "product_description_length": "product_description_lenght",
    }
}


def _rename_columns(chunk: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """테이블별 컬럼 rename 적용. 매핑이 없으면 그대로 반환."""
    renames = _COLUMN_RENAMES.get(table_name)
    if renames:
        chunk = chunk.rename(columns=renames)
    return chunk


def load_table(
    csv_filename: str,
    table_name: str,
    parse_dates: Optional[list[str]] = None,
    chunksize: int = 10000,
) -> None:
    csv_path = os.path.join(DATA_DIR, csv_filename)

    # Spark 출력은 디렉토리(part-*.csv) 형태로 저장됨
    if os.path.isdir(csv_path):
        parts = sorted(glob.glob(os.path.join(csv_path, "part-*.csv")))
        if not parts:
            raise FileNotFoundError(f"part-*.csv 없음: {csv_path}")
        csv_path = parts[0]
    elif not os.path.exists(csv_path):
        raise FileNotFoundError(f"{csv_path} 파일이 존재하지 않습니다.")

    logging.info("\n=== [%s] %s 적재 시작 ===", table_name, csv_path)

    preview_df = pd.read_csv(csv_path, nrows=5)
    logging.info("[%s] 샘플 5행:\n%s", table_name, preview_df)

    total_rows = 0

    for chunk in pd.read_csv(csv_path, chunksize=chunksize, parse_dates=parse_dates):
        # Strip timezone info from datetime columns before MySQL insert.
        # parse_dates parses ISO 8601 "Z" suffix as UTC-aware Timestamp,
        # which pymysql drops silently when writing to DATETIME (no tz support).
        if parse_dates:
            for col in parse_dates:
                if col in chunk.columns and hasattr(chunk[col], "dt"):
                    chunk[col] = chunk[col].dt.tz_localize(None) if chunk[col].dt.tz is None \
                        else chunk[col].dt.tz_convert(None)

        chunk = _rename_columns(chunk, table_name)

        rows_in_chunk = len(chunk)
        total_rows += rows_in_chunk

        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )
        logging.info("[%s] %d행 적재 (누적 %d행)", table_name, rows_in_chunk, total_rows)

    logging.info("=== [%s] 적재 완료 (총 %d행) ===", table_name, total_rows)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )

    # 1. customers 전체 적재
    load_table(
        csv_filename="olist_customers_dataset.csv",
        table_name="customers",
        parse_dates=None
    )

    # 2. products 전체 적재 (오타 컬럼 rename은 _rename_columns에서 처리)
    load_table(
        csv_filename="olist_products_dataset.csv",
        table_name="products",
        parse_dates=None
    )

    # 3. orders - 과거 70%만 적재 (Spark가 생성한 디렉토리)
    load_table(
        csv_filename="orders_initial_70",
        table_name="orders",
        parse_dates=[
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]
    )

    # 4. order_items - 70%에 해당하는 아이템만 적재 (Spark가 생성한 디렉토리)
    load_table(
        csv_filename="order_items_initial_70",
        table_name="order_items",
        parse_dates=[
            "shipping_limit_date",
        ]
    )

    logging.info("\n=== 모든 테이블 초기 적재 완료 ===")


if __name__ == "__main__":
    main()
