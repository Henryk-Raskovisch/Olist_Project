# Olist E-Commerce — End-to-End Data Engineering with Microsoft Fabric

A portfolio data engineering project built on the [Brazilian E-Commerce Public Dataset (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), implementing a full medallion architecture on Microsoft Fabric with a Power BI dashboard.

---

## Architecture

```
CSV (Kaggle)
     │
     ▼
PostgreSQL (Supabase)              ← transactional source system
     │
     ▼ JDBC / PySpark
┌─────────────────────────────────────────────┐
│           Microsoft Fabric Lakehouse         │
│                                             │
│  Bronze  →  Silver  →  Gold                 │
│  (raw)      (clean)     (marts)             │
└─────────────────────────────────────────────┘
     │
     ▼ OneLake Shortcuts (Data Mesh)
lakehouse_analytics
     │
     ▼ Direct Lake
Semantic Model (sm_olist)
     │
     ▼
Power BI Dashboard (5 pages)
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Source system | PostgreSQL on Supabase (free tier) |
| Local ingestion | Python + SQLAlchemy + pandas |
| Processing | PySpark (Microsoft Fabric) |
| Storage | Delta Lake / OneLake |
| Orchestration | Fabric Data Factory Pipeline |
| Semantic layer | Power BI Semantic Model (Direct Lake) |
| Visualization | Power BI Dashboard |

---

## Project Structure

```
olist-fabric/
├── 01_load_olist_to_supabase.py        # Loads CSVs into PostgreSQL
├── notebook_01_bronze.py               # Bronze ingestion via JDBC
├── notebook_02_silver.py               # Silver transformations
├── notebook_03_gold.py                 # Gold analytical marts
└── pyspark_data_engineering_guide.pdf  # PySpark functions reference
```

---

## Dataset

**Brazilian E-Commerce Public Dataset by Olist** — 9 tables, ~1.5 million rows

| Table | Rows | Description |
|-------|------|-------------|
| orders | 99,441 | Orders and status |
| order_items | 112,650 | Items per order |
| order_payments | 103,886 | Payments |
| order_reviews | 99,224 | Customer reviews |
| customers | 99,441 | Customers |
| sellers | 3,095 | Sellers |
| products | 32,951 | Products |
| geolocation | 1,000,163 | Zip codes and coordinates |
| category_translation | 71 | Category name translations |

**Dataset quirks handled:**
- Column name typos: `product_name_lenght` and `product_description_lenght` (missing 'h')
- `review_id` contains duplicates — no PK in the source
- `geolocation` has 1 million rows for ~19k unique zip codes

---

## Medallion Architecture

### Bronze — Raw Ingestion
- JDBC connection to Supabase via connection pooler (port 6543)
- Data stored as Delta Tables without transformation
- Control metadata columns: `_bronze_ingested_at`, `_bronze_source`, `_bronze_batch_id`

### Silver — Cleansing and Enrichment
7 transformed tables:

| Table | Key Transformations |
|-------|-------------------|
| silver_dim_customers | Dedup by customer_unique_id, state/city/zip normalization |
| silver_dim_sellers | Dedup by seller_id, normalization |
| silver_dim_products | Join with category_translation, volume_cm3, typo fixes |
| silver_dim_geolocation | groupBy(zip_code) + avg(lat/lng) — 1M rows → 19k unique zips |
| silver_fact_orders | Joins with items/payments/reviews, delay_days, is_on_time |
| silver_fact_order_items | GMV = price + freight, is_free_shipping, freight_pct |
| silver_fact_reviews | Dedup review_id, review_sentiment, has_comment, response_time_hours |

### Gold — Analytical Marts
4 marts ready for Power BI consumption:

| Mart | Granularity | Key Metrics |
|------|------------|-------------|
| mart_sales | month × state × category | GMV, avg ticket, orders, unique customers |
| mart_delivery | delivered order | delay_days, is_on_time, delivery_route, delay_bucket |
| mart_satisfaction | order × review | review_sentiment, nps_category, nps_score_contribution |
| mart_seller_performance | seller × month | on_time_rate, avg_review_score, seller_tier |

---

## Orchestration Pipeline

`pipeline_medallion` on Fabric Data Factory:

```
Bronze Notebook → Silver Notebook → Gold Notebook
```

- Scheduled daily at 6:00 AM (Brasília timezone)
- Success-based dependency — Silver only runs if Bronze completes without error
- Gold only runs if Silver completes without error

---

## Data Mesh with OneLake Shortcuts

`lakehouse_analytics` demonstrates the Data Mesh concept — it points to Gold tables from `lakehouse_olist` via OneLake Shortcuts without duplicating data.

```
lakehouse_olist (processing)
    └── schema gold/
        ├── mart_sales
        ├── mart_delivery
        ├── mart_satisfaction
        └── mart_seller_performance
              ↑ OneLake Shortcuts
lakehouse_analytics (consumption)
```

---

## Semantic Model and Dashboard

**Semantic Model (`sm_olist`)** in Direct Lake mode:
- `Dim_calendar` built in M (Power Query) with daily granularity
- One-to-Many relationships via `date_reference` and `order_date`
- DAX measures centralized in the SM (single source of truth)
- No scheduled refresh needed — Direct Lake reads Delta files live from OneLake

**Power BI Dashboard — 5 pages:**

| Page | Description |
|------|-------------|
| Executive Overview | KPI cards, GMV trend, top categories, state map |
| Sales & Revenue | GMV breakdown, avg ticket, top categories and states |
| Logistics | Lead time, on-time rate, delay bucket, worst delivery routes |
| Satisfaction & NPS | NPS score, promoters/detractors, score by category and sentiment |
| Seller Performance | Tier distribution, seller ranking, on-time rate by tier |

---

## How to Reproduce

### Prerequisites
- Python 3.10+
- Supabase account (free tier)
- Microsoft Fabric account (trial or license)
- Olist dataset from Kaggle

### 1. Load data into Supabase
```bash
pip install pandas sqlalchemy psycopg2-binary tqdm
python 01_load_olist_to_supabase.py
```

Set environment variables:
```
SUPABASE_HOST=aws-0-sa-east-1.pooler.supabase.com
SUPABASE_PORT=6543
SUPABASE_USER=postgres.{project_id}
SUPABASE_PASSWORD={your_password}
```

### 2. Microsoft Fabric
1. Create workspace `olist-medallion`
2. Create Lakehouse `lakehouse_olist`
3. Create Environment `env_olist` with PostgreSQL JDBC driver (`postgresql-42.7.3.jar`)
4. Create and run notebooks in order: Bronze → Silver → Gold
5. Create Pipeline with success dependencies
6. Create `lakehouse_analytics` with OneLake Shortcuts pointing to the `gold` schema
7. Create Semantic Model via Direct Lake

### 3. Power BI
1. Connect to the SM via Live Connection
2. Create DAX measures in the SM
3. Build the dashboard with 5 pages

---

## Supabase JDBC Connection

Supabase free tier requires the **connection pooler** — direct connections on port 5432 are blocked from external networks:

```python
JDBC_URL = "jdbc:postgresql://aws-0-sa-east-1.pooler.supabase.com:6543/postgres?sslmode=require"
SUPABASE_USER = "postgres.{project_id}"  # project_id included in username
```

---

## Key Technical Decisions

- **Direct Lake over Import mode** — no refresh schedule needed, data is always live from OneLake
- **Dim_calendar built in M (Power Query)** — DAX calculated tables cannot reference Direct Lake tables
- **Gold saved in separate `gold` schema** — enables clean OneLake Shortcuts by domain
- **Measures in SM (not in .pbix)** — reusable across multiple reports, single source of truth
- **Defensive ingestion to Supabase** — chunked writes, per-batch connections to handle free tier timeout drops

---

## Author

**Henryk Raskovisch**
- [LinkedIn](https://www.linkedin.com/in/henryk-raskovisch)
- [GitHub](https://github.com/henryk-raskovisch)

---

*Dataset: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — license CC BY-NC-SA 4.0*
