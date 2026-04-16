"""
=============================================================
 Olist -> Supabase (PostgreSQL)
 Script de carga inicial -- Bronze Source
=============================================================
 Pre-requisitos:
   pip install psycopg2-binary pandas sqlalchemy tqdm

 Como usar:
   1. Preencha as variaveis em CONFIG abaixo com os dados
      do seu projeto Supabase (Settings > Database)
   2. Coloque os 9 CSVs do Olist em uma pasta e informe
      o caminho em CSV_DIR
   3. Execute: python 01_load_olist_to_supabase.py
=============================================================
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

# =============================================================
#  CONFIG
# =============================================================
import os

CONFIG = {
    "host":     os.environ.get("SUPABASE_HOST", "aws-0-sa-east-1.pooler.supabase.com"),
    "port":     int(os.environ.get("SUPABASE_PORT", "6543")),
    "database": os.environ.get("SUPABASE_DB", "postgres"),
    "user":     os.environ.get("SUPABASE_USER", "postgres.zmrziezvnptoylhccevv"),
    "password": os.environ.get("SUPABASE_PASSWORD", ""),
}
CSV_DIR = r"C:\Users\rasko\OneDrive\IT\Brazilian E-Commerce\olist"

BATCH_SIZE = 5_000

DDL = """
DROP TABLE IF EXISTS order_reviews        CASCADE;
DROP TABLE IF EXISTS order_payments       CASCADE;
DROP TABLE IF EXISTS order_items          CASCADE;
DROP TABLE IF EXISTS orders               CASCADE;
DROP TABLE IF EXISTS customers            CASCADE;
DROP TABLE IF EXISTS sellers              CASCADE;
DROP TABLE IF EXISTS products             CASCADE;
DROP TABLE IF EXISTS geolocation          CASCADE;
DROP TABLE IF EXISTS category_translation CASCADE;

CREATE TABLE geolocation (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat              DOUBLE PRECISION,
    geolocation_lng              DOUBLE PRECISION,
    geolocation_city             VARCHAR(100),
    geolocation_state            CHAR(2)
);

CREATE TABLE category_translation (
    product_category_name         VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100)
);

CREATE TABLE customers (
    customer_id              VARCHAR(50) PRIMARY KEY,
    customer_unique_id       VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city            VARCHAR(100),
    customer_state           CHAR(2)
);

CREATE TABLE sellers (
    seller_id              VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10),
    seller_city            VARCHAR(100),
    seller_state           CHAR(2)
);

CREATE TABLE products (
    product_id                   VARCHAR(50) PRIMARY KEY,
    product_category_name        VARCHAR(100),
    product_name_lenght          INTEGER,
    product_description_lenght   INTEGER,
    product_photos_qty           INTEGER,
    product_weight_g             DOUBLE PRECISION,
    product_length_cm            DOUBLE PRECISION,
    product_height_cm            DOUBLE PRECISION,
    product_width_cm             DOUBLE PRECISION
);

CREATE TABLE orders (
    order_id                        VARCHAR(50) PRIMARY KEY,
    customer_id                     VARCHAR(50),
    order_status                    VARCHAR(30),
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP
);

CREATE TABLE order_items (
    order_id             VARCHAR(50),
    order_item_id        INTEGER,
    product_id           VARCHAR(50),
    seller_id            VARCHAR(50),
    shipping_limit_date  TIMESTAMP,
    price                NUMERIC(10,2),
    freight_value        NUMERIC(10,2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_payments (
    order_id              VARCHAR(50),
    payment_sequential    INTEGER,
    payment_type          VARCHAR(30),
    payment_installments  INTEGER,
    payment_value         NUMERIC(10,2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE order_reviews (
    review_id                VARCHAR(50),
    order_id                 VARCHAR(50),
    review_score             SMALLINT,
    review_comment_title     TEXT,
    review_comment_message   TEXT,
    review_creation_date     TIMESTAMP,
    review_answer_timestamp  TIMESTAMP
);
"""

TABLES = [
    {
        "file":  "olist_geolocation_dataset.csv",
        "table": "geolocation",
        "dtype": {
            "geolocation_zip_code_prefix": str,
            "geolocation_lat": float,
            "geolocation_lng": float,
            "geolocation_city": str,
            "geolocation_state": str,
        },
        "parse_dates": [],
    },
    {
        "file":  "product_category_name_translation.csv",
        "table": "category_translation",
        "dtype": {
            "product_category_name": str,
            "product_category_name_english": str,
        },
        "parse_dates": [],
    },
    {
        "file":  "olist_customers_dataset.csv",
        "table": "customers",
        "dtype": {
            "customer_id": str,
            "customer_unique_id": str,
            "customer_zip_code_prefix": str,
            "customer_city": str,
            "customer_state": str,
        },
        "parse_dates": [],
    },
    {
        "file":  "olist_sellers_dataset.csv",
        "table": "sellers",
        "dtype": {
            "seller_id": str,
            "seller_zip_code_prefix": str,
            "seller_city": str,
            "seller_state": str,
        },
        "parse_dates": [],
    },
    {
        "file":  "olist_products_dataset.csv",
        "table": "products",
        "dtype": {
            "product_id": str,
            "product_category_name": str,
        },
        "parse_dates": [],
    },
    {
        "file":  "olist_orders_dataset.csv",
        "table": "orders",
        "dtype": {
            "order_id": str,
            "customer_id": str,
            "order_status": str,
        },
        "parse_dates": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    },
    {
        "file":  "olist_order_items_dataset.csv",
        "table": "order_items",
        "dtype": {
            "order_id": str,
            "product_id": str,
            "seller_id": str,
        },
        "parse_dates": ["shipping_limit_date"],
    },
    {
        "file":  "olist_order_payments_dataset.csv",
        "table": "order_payments",
        "dtype": {
            "order_id": str,
            "payment_type": str,
        },
        "parse_dates": [],
    },
    {
        "file":  "olist_order_reviews_dataset.csv",
        "table": "order_reviews",
        "dtype": {
            "review_id": str,
            "order_id": str,
            "review_comment_title": str,
            "review_comment_message": str,
        },
        "parse_dates": ["review_creation_date", "review_answer_timestamp"],
    },
]


def get_engine():
    url = (
        f"postgresql+psycopg2://{CONFIG['user']}:{CONFIG['password']}"
        f"@{CONFIG['host']}:{CONFIG['port']}/{CONFIG['database']}"
        f"?sslmode=require"
        f"&connect_timeout=60"
        f"&keepalives=1"
        f"&keepalives_idle=30"
        f"&keepalives_interval=10"
        f"&keepalives_count=5"
    )
    return create_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_timeout=60,
    )


def create_tables(engine):
    print("\nCriando tabelas no Supabase...")
    with engine.connect() as conn:
        for statement in DDL.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
        conn.commit()
    print("   Tabelas criadas com sucesso.\n")


def load_table(engine, cfg):
    filepath = os.path.join(CSV_DIR, cfg["file"])

    if not os.path.exists(filepath):
        print(f"   Arquivo nao encontrado: {filepath} -- pulando.")
        return

    print(f"Carregando {cfg['file']} -> {cfg['table']}")

    df = pd.read_csv(
        filepath,
        dtype=cfg["dtype"],
        parse_dates=cfg["parse_dates"] if cfg["parse_dates"] else False,
        low_memory=False,
    )

    str_cols = [c for c, t in cfg["dtype"].items() if t == str]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("nan", None)

    total = len(df)
    batches = range(0, total, BATCH_SIZE)

    with tqdm(total=total, unit="rows", desc=f"   {cfg['table']}") as pbar:
        for start in batches:
            chunk = df.iloc[start : start + BATCH_SIZE]
            with engine.connect() as conn:
                chunk.to_sql(
                    cfg["table"],
                    conn,
                    if_exists="append",
                    index=False,
                    method=None,
                    chunksize=500,
                )
                conn.commit()
            pbar.update(len(chunk))

    print(f"   {total:,} linhas inseridas.\n")


def main():
    print("=" * 60)
    print(" Olist -> Supabase -- Carga inicial")
    print("=" * 60)

    engine = get_engine()

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Conexao com Supabase estabelecida.\n")
    except Exception as e:
        print(f"Erro de conexao: {e}")
        print("  Verifique host, senha e sslmode=require.")
        return

    create_tables(engine)

    for cfg in TABLES:
        load_table(engine, cfg)

    print("=" * 60)
    print(" Carga concluida! Dados disponiveis no Supabase.")
    print(" Proximo passo: conectar o Fabric via JDBC.")
    print("=" * 60)


if __name__ == "__main__":
    main()