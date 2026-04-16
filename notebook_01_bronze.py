# =============================================================
#  NOTEBOOK 01 — BRONZE
#  Ingestão: Supabase (PostgreSQL) → Fabric Lakehouse (Delta)
#
#  Cole cada célula em uma célula separada no Fabric Notebook
# =============================================================

# -------------------------------------------------------------
# CÉLULA 1 — Configuração da conexão JDBC
# -------------------------------------------------------------
# Preencha com os dados do seu projeto Supabase
# Settings > Database > Connection parameters

SUPABASE_HOST     = "db.xxxxxxxxxxxx.supabase.co"
SUPABASE_PORT     = "5432"
SUPABASE_DB       = "postgres"
SUPABASE_USER     = "postgres"
SUPABASE_PASSWORD = "sua_senha_aqui"

JDBC_URL = (
    f"jdbc:postgresql://{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
    f"?sslmode=require"
)

JDBC_PROPERTIES = {
    "user":                 SUPABASE_USER,
    "password":             SUPABASE_PASSWORD,
    "driver":               "org.postgresql.Driver",
    "sslmode":              "require",
    "fetchsize":            "10000",   # linhas por fetch — melhora performance
}

# Caminho base no Lakehouse (ajuste para o nome do seu Lakehouse)
LAKEHOUSE_PATH = "abfss://seu_workspace@onelake.dfs.fabric.microsoft.com/seu_lakehouse.Lakehouse/Tables/bronze"

print("✓ Configuração carregada.")


# -------------------------------------------------------------
# CÉLULA 2 — Lista de tabelas a ingerir
# -------------------------------------------------------------

TABELAS = [
    "geolocation",
    "category_translation",
    "customers",
    "sellers",
    "products",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
]

print(f"✓ {len(TABELAS)} tabelas configuradas para ingestão.")


# -------------------------------------------------------------
# CÉLULA 3 — Função de ingestão genérica com watermark
# -------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable
from datetime import datetime

def ingest_table_bronze(table_name: str):
    """
    Lê uma tabela do Supabase via JDBC e grava como Delta Table
    na camada Bronze, adicionando metadados de ingestão.
    """
    print(f"\n⏳ Ingerindo: {table_name}")

    # --- Leitura via JDBC ---
    df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", SUPABASE_USER)
        .option("password", SUPABASE_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("sslmode", "require")
        .option("fetchsize", "10000")
        .load()
    )

    raw_count = df.count()
    print(f"   → {raw_count:,} linhas lidas do Supabase.")

    # --- Adiciona colunas de controle de ingestão ---
    df = df.withColumn("_bronze_ingested_at", F.current_timestamp()) \
           .withColumn("_bronze_source",       F.lit(f"supabase.{table_name}")) \
           .withColumn("_bronze_batch_id",     F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))

    # --- Grava como Delta Table (overwrite para bronze) ---
    delta_path = f"{LAKEHOUSE_PATH}/{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(delta_path)
    )

    # --- Registra no metastore do Fabric ---
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze_{table_name}
        USING DELTA
        LOCATION '{delta_path}'
    """)

    print(f"   ✓ Gravado em: {delta_path}")
    print(f"   ✓ Tabela registrada: bronze_{table_name}")
    return raw_count


# -------------------------------------------------------------
# CÉLULA 4 — Execução da ingestão de todas as tabelas
# -------------------------------------------------------------

from pyspark.sql import SparkSession

resultados = {}

for tabela in TABELAS:
    try:
        count = ingest_table_bronze(tabela)
        resultados[tabela] = {"status": "✓ OK", "linhas": count}
    except Exception as e:
        resultados[tabela] = {"status": f"✗ ERRO: {e}", "linhas": 0}
        print(f"   ✗ Erro em {tabela}: {e}")

# Resumo
print("\n" + "=" * 55)
print(" RESUMO — Ingestão Bronze")
print("=" * 55)
for tabela, info in resultados.items():
    print(f"  {info['status']:10} {tabela:30} {info['linhas']:>10,} linhas")
print("=" * 55)


# -------------------------------------------------------------
# CÉLULA 5 — Validação: contagens e amostra de cada tabela
# -------------------------------------------------------------

print("\n📊 Validação das tabelas Bronze:\n")

for tabela in TABELAS:
    try:
        df_check = spark.read.format("delta").load(f"{LAKEHOUSE_PATH}/{tabela}")
        count    = df_check.count()
        cols     = len(df_check.columns)
        nulls_order_id = (
            df_check.filter(F.col(df_check.columns[0]).isNull()).count()
            if count > 0 else "N/A"
        )
        print(f"  {tabela}")
        print(f"    Linhas : {count:,}")
        print(f"    Colunas: {cols}")
        print()
    except Exception as e:
        print(f"  {tabela} — erro na leitura: {e}\n")


# -------------------------------------------------------------
# CÉLULA 6 — Análise rápida de qualidade (dados sujos esperados)
# -------------------------------------------------------------

print("🔍 Análise de qualidade — camada Bronze\n")

# Pedidos com datas nulas (problema conhecido no Olist)
df_orders = spark.read.format("delta").load(f"{LAKEHOUSE_PATH}/orders")

print("Pedidos com campos de data nulos:")
for col in [
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
]:
    null_count = df_orders.filter(F.col(col).isNull()).count()
    total      = df_orders.count()
    pct        = null_count / total * 100
    print(f"  {col}: {null_count:,} nulos ({pct:.1f}%)")

# Distribuição de status de pedidos
print("\nDistribuição de order_status:")
df_orders.groupBy("order_status").count().orderBy(F.desc("count")).show()

# Reviews com comentário vazio
df_reviews = spark.read.format("delta").load(f"{LAKEHOUSE_PATH}/order_reviews")
sem_comentario = df_reviews.filter(
    F.col("review_comment_message").isNull() |
    (F.trim(F.col("review_comment_message")) == "")
).count()
print(f"\nReviews sem comentário: {sem_comentario:,} de {df_reviews.count():,}")

# Produtos sem categoria
df_products = spark.read.format("delta").load(f"{LAKEHOUSE_PATH}/products")
sem_categoria = df_products.filter(F.col("product_category_name").isNull()).count()
print(f"Produtos sem categoria : {sem_categoria:,} de {df_products.count():,}")

print("\n✓ Bronze concluído. Execute o Notebook 02 — Silver.")
