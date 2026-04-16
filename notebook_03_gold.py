# =============================================================
#  NOTEBOOK 03 — GOLD
#  Marts analíticos: Silver → Gold (Power BI ready)
#
#  Cole cada célula em uma célula separada no Fabric Notebook
# =============================================================

# -------------------------------------------------------------
# CÉLULA 1 — Paths e leitura das tabelas Silver
# -------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

SILVER_PATH = "abfss://seu_workspace@onelake.dfs.fabric.microsoft.com/seu_lakehouse.Lakehouse/Tables/silver"
GOLD_PATH   = "abfss://seu_workspace@onelake.dfs.fabric.microsoft.com/seu_lakehouse.Lakehouse/Tables/gold"

def read_silver(table: str):
    return spark.read.format("delta").load(f"{SILVER_PATH}/{table}")

def write_gold(df, table: str):
    path = f"{GOLD_PATH}/{table}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_{table}
        USING DELTA LOCATION '{path}'
    """)
    count = df.count()
    print(f"   ✓ gold_{table}: {count:,} linhas gravadas.")
    return count

# Leitura antecipada das tabelas Silver
df_orders     = read_silver("fact_orders")
df_items      = read_silver("fact_order_items")
df_customers  = read_silver("dim_customers")
df_sellers    = read_silver("dim_sellers")
df_products   = read_silver("dim_products")
df_geo        = read_silver("dim_geolocation")
df_reviews    = read_silver("fact_reviews")

print("✓ Tabelas Silver carregadas.")


# -------------------------------------------------------------
# CÉLULA 2 — mart_sales
#  Granularidade: pedido × categoria × estado × mês
#  Métricas: GMV, ticket médio, volume de pedidos,
#            itens por pedido, receita de frete
# -------------------------------------------------------------

print("\n⏳ Construindo mart_sales...")

df_mart_sales = (
    df_orders
    # Filtra apenas pedidos entregues ou em trânsito (exclui cancelados)
    .filter(F.col("order_status").isin(["delivered", "shipped", "invoiced", "approved"]))
    # Join com customers para obter estado
    .join(
        df_customers.select("customer_id", "customer_state", "customer_city"),
        on="customer_id", how="left"
    )
    # Join com geolocalização do cliente
    .join(
        df_geo.select(
            F.col("geolocation_zip_code_prefix").alias("customer_zip_code_prefix"),
            F.col("geolocation_lat").alias("customer_lat"),
            F.col("geolocation_lng").alias("customer_lng"),
        ),
        on="customer_zip_code_prefix", how="left"
    )
    # Join com itens para obter categoria
    .join(
        df_items.join(
            df_products.select("product_id", "product_category_name_english"),
            on="product_id", how="left"
        ).select("order_id", "product_category_name_english", "price", "freight_value", "gmv"),
        on="order_id", how="left"
    )
    # Agrupa por dimensões de análise
    .groupBy(
        "order_year_month",
        "customer_state",
        "customer_city",
        "product_category_name_english",
        "order_status",
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("total_items").alias("total_items_sold"),
        F.sum("total_price").alias("total_revenue"),
        F.sum("total_freight").alias("total_freight_revenue"),
        F.sum("total_payment_value").alias("total_gmv"),
        F.avg("total_price").alias("avg_ticket"),
        F.avg("total_items").alias("avg_items_per_order"),
        F.avg("max_installments").alias("avg_installments"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn("revenue_per_order",  F.col("total_revenue") / F.col("total_orders"))
    .withColumn("freight_pct_revenue",
        F.when(F.col("total_gmv") > 0,
            F.col("total_freight_revenue") / F.col("total_gmv")
        ).otherwise(F.lit(0.0))
    )
    .withColumn("_gold_processed_at", F.current_timestamp())
)

write_gold(df_mart_sales, "mart_sales")


# -------------------------------------------------------------
# CÉLULA 3 — mart_delivery
#  Granularidade: pedido (1 linha por pedido entregue)
#  Métricas: SLA, atraso, lead time, on-time rate por região
# -------------------------------------------------------------

print("\n⏳ Construindo mart_delivery...")

df_mart_delivery = (
    df_orders
    .filter(F.col("order_status") == "delivered")
    .filter(F.col("has_invalid_dates") == False)
    # Join com customer para estado
    .join(
        df_customers.select("customer_id", "customer_state", "customer_city",
                            "customer_zip_code_prefix"),
        on="customer_id", how="left"
    )
    # Join com geo para lat/lng do cliente
    .join(
        df_geo.select(
            F.col("geolocation_zip_code_prefix").alias("customer_zip_code_prefix"),
            F.col("geolocation_lat").alias("customer_lat"),
            F.col("geolocation_lng").alias("customer_lng"),
        ),
        on="customer_zip_code_prefix", how="left"
    )
    # Join com seller via items
    .join(
        df_items.groupBy("order_id")
                .agg(F.first("seller_id").alias("main_seller_id")),
        on="order_id", how="left"
    )
    .join(
        df_sellers.select(
            F.col("seller_id").alias("main_seller_id"),
            F.col("seller_state"),
            F.col("seller_city"),
            F.col("seller_zip_code_prefix"),
        ),
        on="main_seller_id", how="left"
    )
    # Join com geo do seller
    .join(
        df_geo.select(
            F.col("geolocation_zip_code_prefix").alias("seller_zip_code_prefix"),
            F.col("geolocation_lat").alias("seller_lat"),
            F.col("geolocation_lng").alias("seller_lng"),
        ),
        on="seller_zip_code_prefix", how="left"
    )
    .select(
        "order_id",
        "order_year_month",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_time_days",
        "estimated_delivery_days",
        "delay_days",
        "is_on_time",
        "customer_state",
        "customer_city",
        "customer_lat",
        "customer_lng",
        "seller_state",
        "seller_city",
        "seller_lat",
        "seller_lng",
        "total_items",
        "total_freight",
        F.current_timestamp().alias("_gold_processed_at"),
    )
    # Classifica atraso em faixas
    .withColumn(
        "delay_bucket",
        F.when(F.col("delay_days") <= -7,  "7+ dias adiantado")
         .when(F.col("delay_days") <= 0,   "no prazo")
         .when(F.col("delay_days") <= 3,   "até 3 dias de atraso")
         .when(F.col("delay_days") <= 7,   "4 a 7 dias de atraso")
         .otherwise("mais de 7 dias de atraso")
    )
    # Rota de entrega (estado vendedor → estado cliente)
    .withColumn(
        "delivery_route",
        F.concat_ws(" → ", F.col("seller_state"), F.col("customer_state"))
    )
)

write_gold(df_mart_delivery, "mart_delivery")


# -------------------------------------------------------------
# CÉLULA 4 — mart_satisfaction
#  Granularidade: pedido × review
#  Métricas: NPS proxy, score por categoria/vendedor, correlação
#            com atraso
# -------------------------------------------------------------

print("\n⏳ Construindo mart_satisfaction...")

df_mart_satisfaction = (
    df_reviews
    .join(
        df_orders.select(
            "order_id", "order_year_month", "customer_id",
            "delay_days", "is_on_time", "delivery_time_days",
            "order_status", "total_price"
        ),
        on="order_id", how="left"
    )
    .join(
        df_customers.select("customer_id", "customer_state"),
        on="customer_id", how="left"
    )
    # Pega categoria do produto principal do pedido
    .join(
        df_items
        .join(df_products.select("product_id", "product_category_name_english"),
              on="product_id", how="left")
        .groupBy("order_id")
        .agg(F.first("product_category_name_english").alias("main_category"),
             F.first("seller_id").alias("main_seller_id")),
        on="order_id", how="left"
    )
    .select(
        "review_id",
        "order_id",
        "order_year_month",
        "customer_state",
        "main_category",
        "main_seller_id",
        "review_score",
        "review_sentiment",
        "has_comment",
        "response_time_hours",
        "delay_days",
        "is_on_time",
        "delivery_time_days",
        "total_price",
        F.current_timestamp().alias("_gold_processed_at"),
    )
    # NPS proxy: promotores (5) vs detratores (1-2)
    .withColumn(
        "nps_category",
        F.when(F.col("review_score") >= 5, "promoter")
         .when(F.col("review_score") >= 4, "passive")
         .otherwise("detractor")
    )
    # Score ponderado (para média ponderada no Power BI)
    .withColumn(
        "nps_score_contribution",
        F.when(F.col("nps_category") == "promoter",  F.lit(100))
         .when(F.col("nps_category") == "passive",   F.lit(0))
         .otherwise(F.lit(-100))
    )
)

write_gold(df_mart_satisfaction, "mart_satisfaction")


# -------------------------------------------------------------
# CÉLULA 5 — mart_seller_performance (bônus)
#  Granularidade: vendedor × mês
#  Para ranking de melhores/piores sellers no Power BI
# -------------------------------------------------------------

print("\n⏳ Construindo mart_seller_performance...")

df_seller_perf = (
    df_items
    .join(df_orders.select("order_id", "order_year_month", "order_status",
                           "is_on_time", "delay_days", "total_price"),
          on="order_id", how="left")
    .join(df_sellers.select("seller_id", "seller_state", "seller_city"),
          on="seller_id", how="left")
    .join(df_reviews.select("order_id", "review_score", "review_sentiment"),
          on="order_id", how="left")
    .filter(F.col("order_status").isin(["delivered", "shipped"]))
    .groupBy("seller_id", "seller_state", "seller_city", "order_year_month")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("price").alias("total_revenue"),
        F.sum("freight_value").alias("total_freight"),
        F.avg("review_score").alias("avg_review_score"),
        F.avg(F.col("is_on_time").cast("integer")).alias("on_time_rate"),
        F.avg("delay_days").alias("avg_delay_days"),
        F.countDistinct("product_id").alias("unique_products_sold"),
    )
    .withColumn(
        "seller_tier",
        F.when(
            (F.col("avg_review_score") >= 4.5) & (F.col("on_time_rate") >= 0.9), "Excelente"
        ).when(
            (F.col("avg_review_score") >= 4.0) & (F.col("on_time_rate") >= 0.75), "Bom"
        ).when(
            (F.col("avg_review_score") >= 3.0), "Regular"
        ).otherwise("Crítico")
    )
    .withColumn("_gold_processed_at", F.current_timestamp())
)

write_gold(df_seller_perf, "mart_seller_performance")


# -------------------------------------------------------------
# CÉLULA 6 — Resumo final e dicas para o Power BI
# -------------------------------------------------------------

gold_tables = [
    "mart_sales",
    "mart_delivery",
    "mart_satisfaction",
    "mart_seller_performance",
]

print("\n📊 Resumo — camada Gold:\n")
for tbl in gold_tables:
    df = spark.read.format("delta").load(f"{GOLD_PATH}/{tbl}")
    print(f"  {tbl}: {df.count():,} linhas | {len(df.columns)} colunas")

print("""
✓ Gold concluído!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 Próximos passos no Power BI:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

 1. Conecte no Fabric Lakehouse via "Microsoft Fabric"
    no Power BI Desktop (Get Data)

 2. Importe as 4 tabelas Gold

 3. Relacionamentos sugeridos:
    mart_sales ──────── order_year_month
    mart_delivery ────── order_year_month
    mart_satisfaction ─── order_year_month + main_seller_id
    mart_seller_performance ─ seller_id + order_year_month

 4. Páginas sugeridas do dashboard:
    • Visão Geral de Vendas   (mart_sales)
    • Logística & Entregas    (mart_delivery)
    • Satisfação & NPS        (mart_satisfaction)
    • Ranking de Vendedores   (mart_seller_performance)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")
