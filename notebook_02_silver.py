# =============================================================
#  NOTEBOOK 02 — SILVER
#  Transformação: Bronze (Delta raw) → Silver (Delta limpo)
#
#  Cole cada célula em uma célula separada no Fabric Notebook
# =============================================================

# -------------------------------------------------------------
# CÉLULA 1 — Paths e imports
# -------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, TimestampType, ShortType
)
from delta.tables import DeltaTable

BRONZE_PATH = "abfss://seu_workspace@onelake.dfs.fabric.microsoft.com/seu_lakehouse.Lakehouse/Tables/bronze"
SILVER_PATH = "abfss://seu_workspace@onelake.dfs.fabric.microsoft.com/seu_lakehouse.Lakehouse/Tables/silver"

def read_bronze(table: str):
    return spark.read.format("delta").load(f"{BRONZE_PATH}/{table}")

def write_silver(df, table: str):
    path = f"{SILVER_PATH}/{table}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver_{table}
        USING DELTA LOCATION '{path}'
    """)
    count = df.count()
    print(f"   ✓ silver_{table}: {count:,} linhas gravadas.")
    return count

print("✓ Configuração Silver carregada.")


# -------------------------------------------------------------
# CÉLULA 2 — dim_customers
#  Problema: customer_id muda a cada pedido (customer_unique_id
#  é o identificador real do cliente). Precisamos deduplicar.
# -------------------------------------------------------------

print("\n⏳ Processando dim_customers...")

df_cust_raw = read_bronze("customers")

# Pega o registro mais recente por customer_unique_id
# (usa customer_id como desempate deterministico)
window_cust = Window.partitionBy("customer_unique_id").orderBy(F.desc("customer_id"))

df_customers = (
    df_cust_raw
    # Remove colunas de controle bronze
    .drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")
    # Deduplicação por customer_unique_id — mantém 1 linha por cliente real
    .withColumn("rn", F.row_number().over(window_cust))
    .filter(F.col("rn") == 1)
    .drop("rn")
    # Normaliza estado (garante maiúsculo, 2 chars)
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    # Normaliza cidade
    .withColumn("customer_city", F.initcap(F.trim(F.col("customer_city"))))
    # Padroniza CEP (5 dígitos com zero à esquerda)
    .withColumn(
        "customer_zip_code_prefix",
        F.lpad(F.col("customer_zip_code_prefix"), 5, "0")
    )
    # Flag de cliente recorrente (terá múltiplos customer_id)
    .withColumn("is_repeat_customer",
        F.count("customer_unique_id").over(
            Window.partitionBy("customer_unique_id")
        ) > 1
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
)

write_silver(df_customers, "dim_customers")


# -------------------------------------------------------------
# CÉLULA 3 — dim_sellers
# -------------------------------------------------------------

print("\n⏳ Processando dim_sellers...")

df_sellers = (
    read_bronze("sellers")
    .drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")
    .dropDuplicates(["seller_id"])
    .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    .withColumn("seller_city",  F.initcap(F.trim(F.col("seller_city"))))
    .withColumn(
        "seller_zip_code_prefix",
        F.lpad(F.col("seller_zip_code_prefix"), 5, "0")
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
)

write_silver(df_sellers, "dim_sellers")


# -------------------------------------------------------------
# CÉLULA 4 — dim_products
#  Problema: product_category_name em português, NULLs em
#  dimensões físicas, categorias sem tradução
# -------------------------------------------------------------

print("\n⏳ Processando dim_products...")

df_prod_raw  = read_bronze("products").drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")
df_cat_trans = read_bronze("category_translation").drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")

df_products = (
    df_prod_raw
    .dropDuplicates(["product_id"])
    # Join com tradução de categoria
    .join(df_cat_trans, on="product_category_name", how="left")
    # Categoria sem tradução → usa o nome original
    .withColumn(
        "product_category_name_english",
        F.when(
            F.col("product_category_name_english").isNull(),
            F.col("product_category_name")
        ).otherwise(F.col("product_category_name_english"))
    )
    # Imputa dimensões físicas nulas com mediana por categoria
    # (simplificado: usa 0 como fallback — ajuste conforme análise)
    .withColumn("product_weight_g",    F.coalesce(F.col("product_weight_g"),    F.lit(0.0)))
    .withColumn("product_length_cm",   F.coalesce(F.col("product_length_cm"),   F.lit(0.0)))
    .withColumn("product_height_cm",   F.coalesce(F.col("product_height_cm"),   F.lit(0.0)))
    .withColumn("product_width_cm",    F.coalesce(F.col("product_width_cm"),    F.lit(0.0)))
    # Calcula volume em cm³
    .withColumn(
        "product_volume_cm3",
        F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")
    )
    # Flag de produto sem informações físicas
    .withColumn(
        "has_physical_info",
        (F.col("product_weight_g") > 0) & (F.col("product_volume_cm3") > 0)
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
)

write_silver(df_products, "dim_products")


# -------------------------------------------------------------
# CÉLULA 5 — dim_geolocation
#  Problema: CEP tem múltiplas linhas (lat/lng levemente
#  diferentes). Agrega por média.
# -------------------------------------------------------------

print("\n⏳ Processando dim_geolocation...")

df_geo = (
    read_bronze("geolocation")
    .drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")
    # Agrega lat/lng por CEP (resolve duplicatas)
    .groupBy("geolocation_zip_code_prefix", "geolocation_state")
    .agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng"),
        F.first("geolocation_city").alias("geolocation_city"),
    )
    .withColumn(
        "geolocation_zip_code_prefix",
        F.lpad(F.col("geolocation_zip_code_prefix"), 5, "0")
    )
    .withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
    .withColumn("geolocation_city",  F.initcap(F.trim(F.col("geolocation_city"))))
    .withColumn("_silver_processed_at", F.current_timestamp())
)

write_silver(df_geo, "dim_geolocation")


# -------------------------------------------------------------
# CÉLULA 6 — fact_orders
#  Problema: datas impossíveis (entrega antes do pedido),
#  pedidos sem aprovação, status variados
# -------------------------------------------------------------

print("\n⏳ Processando fact_orders...")

df_orders_raw = (
    read_bronze("orders")
    .drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")
)

df_items_agg = (
    read_bronze("order_items")
    .drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")
    .groupBy("order_id")
    .agg(
        F.count("order_item_id").alias("total_items"),
        F.sum("price").alias("total_price"),
        F.sum("freight_value").alias("total_freight"),
        F.countDistinct("seller_id").alias("total_sellers"),
        F.countDistinct("product_id").alias("total_products"),
    )
)

df_payments_agg = (
    read_bronze("order_payments")
    .drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("total_payment_value"),
        F.max("payment_installments").alias("max_installments"),
        F.collect_set("payment_type").alias("payment_types"),
    )
    .withColumn("payment_types", F.concat_ws(",", F.col("payment_types")))
)

df_reviews_agg = (
    read_bronze("order_reviews")
    .drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")
    .groupBy("order_id")
    .agg(
        F.avg("review_score").alias("avg_review_score"),
        F.min("review_score").alias("min_review_score"),
    )
)

df_orders = (
    df_orders_raw
    # Joins com agregações
    .join(df_items_agg,    on="order_id", how="left")
    .join(df_payments_agg, on="order_id", how="left")
    .join(df_reviews_agg,  on="order_id", how="left")

    # Calcula métricas de tempo
    .withColumn(
        "approval_time_hours",
        (F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_timestamp")) / 3600
    )
    .withColumn(
        "delivery_time_days",
        (F.unix_timestamp("order_delivered_customer_date") - F.unix_timestamp("order_purchase_timestamp")) / 86400
    )
    .withColumn(
        "estimated_delivery_days",
        (F.unix_timestamp("order_estimated_delivery_date") - F.unix_timestamp("order_purchase_timestamp")) / 86400
    )
    # Atraso em dias (positivo = atrasado, negativo = adiantado)
    .withColumn(
        "delay_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull() &
            F.col("order_estimated_delivery_date").isNotNull(),
            (F.unix_timestamp("order_delivered_customer_date") -
             F.unix_timestamp("order_estimated_delivery_date")) / 86400
        ).otherwise(F.lit(None))
    )
    # Flag de entrega no prazo
    .withColumn(
        "is_on_time",
        F.when(F.col("delay_days").isNotNull(), F.col("delay_days") <= 0).otherwise(F.lit(None))
    )
    # Flag de data inválida (entrega antes do pedido)
    .withColumn(
        "has_invalid_dates",
        F.col("delivery_time_days") < 0
    )
    # Período (ano-mês) do pedido
    .withColumn(
        "order_year_month",
        F.date_format("order_purchase_timestamp", "yyyy-MM")
    )
    # Normaliza status
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    .withColumn("_silver_processed_at", F.current_timestamp())
)

write_silver(df_orders, "fact_orders")


# -------------------------------------------------------------
# CÉLULA 7 — fact_order_items (nível item)
# -------------------------------------------------------------

print("\n⏳ Processando fact_order_items...")

df_order_items = (
    read_bronze("order_items")
    .drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")
    # Calcula GMV por item
    .withColumn("gmv", F.col("price") + F.col("freight_value"))
    # Flag de frete grátis
    .withColumn("is_free_shipping", F.col("freight_value") == 0)
    # Percentual do frete sobre o total
    .withColumn(
        "freight_pct",
        F.when(
            (F.col("price") + F.col("freight_value")) > 0,
            F.col("freight_value") / (F.col("price") + F.col("freight_value"))
        ).otherwise(F.lit(0.0))
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
)

write_silver(df_order_items, "fact_order_items")


# -------------------------------------------------------------
# CÉLULA 8 — fact_reviews (nível review individual)
# -------------------------------------------------------------

print("\n⏳ Processando fact_reviews...")

df_reviews = (
    read_bronze("order_reviews")
    .drop("_bronze_ingested_at", "_bronze_source", "_bronze_batch_id")
    .dropDuplicates(["review_id"])
    # Flag de review positivo (4-5), neutro (3), negativo (1-2)
    .withColumn(
        "review_sentiment",
        F.when(F.col("review_score") >= 4, "positive")
         .when(F.col("review_score") == 3, "neutral")
         .otherwise("negative")
    )
    # Flag de review com comentário
    .withColumn(
        "has_comment",
        F.col("review_comment_message").isNotNull() &
        (F.trim(F.col("review_comment_message")) != "")
    )
    # Tempo de resposta ao review em horas
    .withColumn(
        "response_time_hours",
        (F.unix_timestamp("review_answer_timestamp") -
         F.unix_timestamp("review_creation_date")) / 3600
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
)

write_silver(df_reviews, "fact_reviews")


# -------------------------------------------------------------
# CÉLULA 9 — Validação da camada Silver
# -------------------------------------------------------------

silver_tables = [
    "dim_customers", "dim_sellers", "dim_products",
    "dim_geolocation", "fact_orders", "fact_order_items", "fact_reviews"
]

print("\n📊 Validação — camada Silver:\n")
for tbl in silver_tables:
    df = spark.read.format("delta").load(f"{SILVER_PATH}/{tbl}")
    print(f"  {tbl}: {df.count():,} linhas | {len(df.columns)} colunas")

print("\n✓ Silver concluído. Execute o Notebook 03 — Gold.")
