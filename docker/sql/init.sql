-- init.sql (for Olist CSV schema)
-- Target DB: ecommerce

CREATE DATABASE IF NOT EXISTS ecommerce
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE ecommerce;

-- FK 때문에 삭제 순서 중요
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

-- 1) customers
CREATE TABLE customers (
  customer_id              CHAR(32)      NOT NULL,
  customer_unique_id       CHAR(32)      NULL,
  customer_zip_code_prefix INT           NULL,
  customer_city            VARCHAR(100)  NULL,
  customer_state           CHAR(2)       NULL,
  PRIMARY KEY (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2) products
CREATE TABLE products (
  product_id                 CHAR(32)      NOT NULL,
  product_category_name      VARCHAR(100)  NULL,
  product_name_lenght        INT           NULL,
  product_description_lenght INT           NULL,
  product_photos_qty         INT           NULL,
  product_weight_g           INT           NULL,
  product_length_cm          INT           NULL,
  product_height_cm          INT           NULL,
  product_width_cm           INT           NULL,
  PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3) orders
CREATE TABLE orders (
  order_id                      CHAR(32)     NOT NULL,
  customer_id                   CHAR(32)     NOT NULL,
  order_status                  VARCHAR(30)  NULL,
  order_purchase_timestamp      DATETIME     NULL,
  order_approved_at             DATETIME     NULL,
  order_delivered_carrier_date  DATETIME     NULL,
  order_delivered_customer_date DATETIME     NULL,
  order_estimated_delivery_date DATETIME     NULL,
  PRIMARY KEY (order_id),
  KEY idx_orders_customer_id (customer_id),
  CONSTRAINT fk_orders_customer
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4) order_items
CREATE TABLE order_items (
  order_id            CHAR(32)       NOT NULL,
  order_item_id       INT            NOT NULL,
  product_id          CHAR(32)       NOT NULL,
  seller_id           CHAR(32)       NULL,
  shipping_limit_date DATETIME       NULL,
  price               DECIMAL(10,2)  NULL,
  freight_value       DECIMAL(10,2)  NULL,
  PRIMARY KEY (order_id, order_item_id),
  KEY idx_order_items_product_id (product_id),
  CONSTRAINT fk_order_items_order
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
  CONSTRAINT fk_order_items_product
    FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ADR-007: Debezium 전용 replication 유저
-- appuser와 분리하여 권한 최소화(least privilege) 원칙 준수.
-- 비밀번호는 MySQL initdb가 환경변수 치환을 지원하지 않아 여기에 하드코딩됨 —
-- 운영 환경에선 Secrets Manager/Vault가 필요하다 (ADR-007 Trade-offs 참조).
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbz';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
