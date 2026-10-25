# Databricks notebook source
# Structure of every table the load and DQ jobs write to.
# Idempotent: CREATE TABLE IF NOT EXISTS leaves existing tables and their data untouched.

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")

BASE = f"{CATALOG}.{SCHEMA_PREFIX}"
BRONZE = f"{CATALOG}.{SCHEMA_PREFIX}_bronze"
SILVER = f"{CATALOG}.{SCHEMA_PREFIX}_silver"
GOLD = f"{CATALOG}.{SCHEMA_PREFIX}_gold"

# COMMAND ----------

# Bronze: source columns as read by Auto Loader plus ingestion metadata

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE}.orders (
    order_id            INT,
    customer_id         INT,
    order_time          TIMESTAMP,
    payment_method      STRING,
    discount_pct        INT,
    subtotal_usd        DOUBLE,
    total_usd           DOUBLE,
    country             STRING,
    device              STRING,
    source              STRING,
    _rescued_data       STRING,
    source_filename     STRING,
    ingestion_timestamp TIMESTAMP,
    load_date           DATE
)
COMMENT 'Raw order headers from raw_data/orders*.csv'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE}.order_items (
    order_id            INT,
    product_id          INT,
    unit_price_usd      DOUBLE,
    quantity            INT,
    line_total_usd      DOUBLE,
    _rescued_data       STRING,
    source_filename     STRING,
    ingestion_timestamp TIMESTAMP,
    load_date           DATE
)
COMMENT 'Raw order lines from raw_data/order_items/'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE}.customers (
    customer_id         INT,
    name                STRING,
    email               STRING,
    country             STRING,
    age                 INT,
    signup_date         DATE,
    marketing_opt_in    BOOLEAN,
    loyalty_tier        STRING,
    _rescued_data       STRING,
    source_filename     STRING,
    ingestion_timestamp TIMESTAMP,
    load_date           DATE
)
COMMENT 'Raw customers from raw_data/customers*.csv'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE}.products (
    product_id          INT,
    category            STRING,
    name                STRING,
    price_usd           DOUBLE,
    cost_usd            DOUBLE,
    margin_usd          DOUBLE,
    _rescued_data       STRING,
    source_filename     STRING,
    ingestion_timestamp TIMESTAMP,
    load_date           DATE
)
COMMENT 'Raw products from raw_data/products*.csv'
""")

# COMMAND ----------

# Silver: validated, deduplicated data and the quarantine of rejected records

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.customers (
    customer_sk         BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id         INT       NOT NULL,
    name                STRING,
    email               STRING,
    country             STRING,
    age                 INT,
    signup_date         DATE,
    marketing_opt_in    BOOLEAN,
    effective_from      TIMESTAMP NOT NULL,
    effective_to        TIMESTAMP,
    is_current          BOOLEAN   NOT NULL,
    source_filename     STRING,
    ingestion_timestamp TIMESTAMP,
    processed_at        TIMESTAMP NOT NULL,
    loyalty_tier        STRING
)
CLUSTER BY (customer_id)
COMMENT 'Customers with SCD2 history, one row per customer version'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.products (
    product_id          INT       NOT NULL,
    category            STRING,
    name                STRING,
    price_usd           DOUBLE,
    cost_usd            DOUBLE,
    margin_usd          DOUBLE,
    source_filename     STRING,
    ingestion_timestamp TIMESTAMP,
    processed_at        TIMESTAMP NOT NULL
)
CLUSTER BY (product_id)
COMMENT 'Products, current state only (SCD1)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.orders (
    order_id            INT       NOT NULL,
    customer_id         INT       NOT NULL,
    order_time          TIMESTAMP NOT NULL,
    payment_method      STRING,
    discount_pct        INT       NOT NULL,
    subtotal_usd        DOUBLE    NOT NULL,
    total_usd           DOUBLE    NOT NULL,
    country             STRING,
    device              STRING,
    source              STRING,
    source_filename     STRING,
    ingestion_timestamp TIMESTAMP,
    processed_at        TIMESTAMP
)
CLUSTER BY (order_id)
COMMENT 'Validated order headers, one row per order'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.order_items (
    order_id            INT    NOT NULL,
    line_number         INT    NOT NULL COMMENT 'Position of the line within its order; the source has no line id',
    product_id          INT    NOT NULL,
    unit_price_usd      DOUBLE NOT NULL,
    quantity            INT    NOT NULL,
    line_total_usd      DOUBLE NOT NULL,
    source_filename     STRING,
    ingestion_timestamp TIMESTAMP,
    processed_at        TIMESTAMP
)
CLUSTER BY (order_id)
COMMENT 'Validated order lines, one row per line within an order'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.dq_quarantine (
    source_table   STRING        NOT NULL,
    record         STRING        NOT NULL COMMENT 'Rejected bronze row as JSON',
    quarantined_at TIMESTAMP     NOT NULL,
    failed_rules   ARRAY<STRING> COMMENT 'Names of the data-quality rules the record breaks'
)
COMMENT 'Bronze records rejected by silver validation'
""")

# COMMAND ----------

# Gold: star schema; dimensions first because the fact references them

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.dim_date (
    date_key     INT     NOT NULL COMMENT 'Date as yyyyMMdd, e.g. 20250314',
    full_date    DATE    NOT NULL,
    year         INT,
    quarter      INT,
    month        INT,
    month_name   STRING,
    day_of_month INT,
    day_name     STRING,
    is_weekend   BOOLEAN,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
)
COMMENT 'Calendar dimension, one row per day'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.dim_customer (
    customer_sk      BIGINT NOT NULL COMMENT 'Surrogate key of a customer version, reused from silver',
    customer_id      INT    NOT NULL,
    customer_name    STRING,
    email            STRING,
    country          STRING COMMENT 'Home country of the customer; may differ from the order country',
    age              INT,
    loyalty_tier     STRING,
    signup_date      DATE,
    marketing_opt_in BOOLEAN,
    effective_from   TIMESTAMP,
    effective_to     TIMESTAMP,
    is_current       BOOLEAN,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
)
COMMENT 'Customer dimension with full SCD2 history'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.dim_product (
    product_id   INT           NOT NULL,
    product_name STRING,
    category     STRING,
    price_usd    DECIMAL(12,2) COMMENT 'Current list price; the price actually paid is in the fact',
    CONSTRAINT pk_dim_product PRIMARY KEY (product_id)
)
COMMENT 'Product dimension (SCD1)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.dim_country (
    country_key  BIGINT GENERATED ALWAYS AS IDENTITY,
    country_code STRING NOT NULL COMMENT 'ISO-2 code of the order country; Unknown when missing',
    CONSTRAINT pk_dim_country PRIMARY KEY (country_key)
)
COMMENT 'Order country dimension'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.dim_order_channel (
    order_channel_key BIGINT GENERATED ALWAYS AS IDENTITY,
    payment_method    STRING NOT NULL,
    device            STRING NOT NULL,
    source            STRING NOT NULL,
    CONSTRAINT pk_dim_order_channel PRIMARY KEY (order_channel_key)
)
COMMENT 'Junk dimension: combinations of payment method, device and source'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.fact_order_items (
    order_id            INT           NOT NULL,
    line_number         INT           NOT NULL,
    date_key            INT           NOT NULL,
    customer_sk         BIGINT        NOT NULL COMMENT 'Customer version valid at order time',
    product_id          INT           NOT NULL,
    country_key         BIGINT        NOT NULL,
    order_channel_key   BIGINT        NOT NULL,
    quantity            INT           NOT NULL,
    unit_price_usd      DECIMAL(12,2) NOT NULL COMMENT 'Price actually paid at order time',
    discount_pct        INT           NOT NULL COMMENT 'Order-level discount applied to every line',
    gross_amount_usd    DECIMAL(12,2) NOT NULL COMMENT 'unit_price_usd * quantity, before discount',
    discount_amount_usd DECIMAL(12,2) NOT NULL,
    net_amount_usd      DECIMAL(12,2) NOT NULL COMMENT 'Revenue after discount; sums to the order total',
    processed_at        TIMESTAMP,
    CONSTRAINT pk_fact_order_items PRIMARY KEY (order_id, line_number),
    CONSTRAINT fk_fact_date     FOREIGN KEY (date_key)          REFERENCES {GOLD}.dim_date (date_key),
    CONSTRAINT fk_fact_customer FOREIGN KEY (customer_sk)       REFERENCES {GOLD}.dim_customer (customer_sk),
    CONSTRAINT fk_fact_product  FOREIGN KEY (product_id)        REFERENCES {GOLD}.dim_product (product_id),
    CONSTRAINT fk_fact_country  FOREIGN KEY (country_key)       REFERENCES {GOLD}.dim_country (country_key),
    CONSTRAINT fk_fact_channel  FOREIGN KEY (order_channel_key) REFERENCES {GOLD}.dim_order_channel (order_channel_key)
)
CLUSTER BY (date_key)
COMMENT 'Sales fact at order line grain'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.agg_daily_sales (
    date_key            INT           NOT NULL,
    country_key         BIGINT        NOT NULL,
    category            STRING        NOT NULL,
    orders_count        BIGINT,
    items_count         BIGINT,
    quantity            BIGINT,
    gross_amount_usd    DECIMAL(14,2),
    discount_amount_usd DECIMAL(14,2),
    net_amount_usd      DECIMAL(14,2),
    processed_at        TIMESTAMP,
    CONSTRAINT pk_agg_daily_sales PRIMARY KEY (date_key, country_key, category),
    CONSTRAINT fk_agg_date    FOREIGN KEY (date_key)    REFERENCES {GOLD}.dim_date (date_key),
    CONSTRAINT fk_agg_country FOREIGN KEY (country_key) REFERENCES {GOLD}.dim_country (country_key)
)
COMMENT 'Daily sales by order country and product category, pre-aggregated for the dashboard'
""")

# COMMAND ----------

# Data-quality history, appended by the DQ job

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BASE}.dq_results (
    run_id         STRING        NOT NULL COMMENT 'Job run id; groups the checks of one suite run',
    checked_at     TIMESTAMP     NOT NULL,
    check_type     STRING        NOT NULL COMMENT 'dqx or reconciliation',
    table_name     STRING        NOT NULL COMMENT 'Checked table, or source -> target for reconciliation',
    check_name     STRING        NOT NULL,
    dimension      STRING        NOT NULL COMMENT 'completeness, uniqueness, validity, consistency, timeliness or reconciliation',
    criticality    STRING        NOT NULL COMMENT 'error fails the suite, warn is only recorded',
    failed_rows    BIGINT,
    total_rows     BIGINT,
    expected_value DECIMAL(22,2) COMMENT 'Reconciliation: value on the source side',
    actual_value   DECIMAL(22,2) COMMENT 'Reconciliation: value on the target side',
    passed         BOOLEAN       NOT NULL
)
COMMENT 'One row per check per DQ suite run'
""")
