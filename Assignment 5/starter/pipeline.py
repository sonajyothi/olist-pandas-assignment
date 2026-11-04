"""
DataKern | Assignment 5 | Olist seller scorecard: windows and Parquet

YOU write the bodies marked TODO. Everything else (schemas, driver) is given.
Run the whole thing:   python pipeline.py
Check yourself:        python check_my_work.py
"""
from pathlib import Path

from pyspark.sql import functions as F  # noqa: F401  (you will need it)
from pyspark.sql.window import Window  # noqa: F401
from pyspark.sql.types import StringType

from helpers import OUT_DIR, count_files, csv_path, folder_size_mb, fresh_dir, get_spark, plan_text, timed, uri

# Explicit schemas (given). A CSV has no types, and column ORDER matters: Spark matches by position.
ORDERS_DDL = (
    "order_id STRING, customer_id STRING, order_status STRING, order_purchase_timestamp TIMESTAMP, "
    "order_approved_at TIMESTAMP, order_delivered_carrier_date TIMESTAMP, "
    "order_delivered_customer_date TIMESTAMP, order_estimated_delivery_date TIMESTAMP"
)
ITEMS_DDL = (
    "order_id STRING, order_item_id INT, product_id STRING, seller_id STRING, "
    "shipping_limit_date TIMESTAMP, price DOUBLE, freight_value DOUBLE"
)
CUSTOMERS_DDL = (
    "customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, "
    "customer_city STRING, customer_state STRING"
)
PRODUCTS_DDL = (
    "product_id STRING, product_category_name STRING, product_name_lenght INT, "
    "product_description_lenght INT, product_photos_qty INT, product_weight_g INT, "
    "product_length_cm INT, product_height_cm INT, product_width_cm INT"
)
TRANSLATION_DDL = "product_category_name STRING, product_category_name_english STRING"

# The columns of the "sales" table you build in Part 1 (one row per order item).
SALES_COLUMNS = [
    "order_id", "order_item_id", "customer_unique_id", "purchase_ts", "seller_id",
    "product_id", "category", "price", "freight_value", "state",
]


# ============================================================================ PART 1
def build_sales(spark):
    """Read the five CSV files and return ONE wide DataFrame with exactly SALES_COLUMNS.

    - read with the explicit schemas above (header=True), using csv_path("<file name>")
    - files: olist_orders_dataset.csv, olist_order_items_dataset.csv, olist_customers_dataset.csv,
             olist_products_dataset.csv, product_category_name_translation.csv
    - one output row per order item: do not lose items, do not duplicate them
    - purchase_ts = order_purchase_timestamp, state = customer_state
    - category = the ENGLISH category name, or the text 'unknown' when there is none
    """
    
    orders = spark.read.csv(
        csv_path("olist_orders_dataset.csv"),
        schema=ORDERS_DDL,
        header=True
    )
    

    items = spark.read.csv(
        csv_path("olist_order_items_dataset.csv"),
        schema=ITEMS_DDL,
        header=True
    )

    customers = spark.read.csv(
        csv_path("olist_customers_dataset.csv"),
        schema=CUSTOMERS_DDL,
        header=True
    )

    products = spark.read.csv(
        csv_path("olist_products_dataset.csv"),
        schema=PRODUCTS_DDL,
        header=True
    )

    translation = spark.read.csv(
        csv_path("product_category_name_translation.csv"),
        schema=TRANSLATION_DDL,
        header=True
    )

    sales = (
        items
        .join(orders, on="order_id", how="left")
        .join(customers, on="customer_id", how="left")
        .join(products, on="product_id", how="left")
        .join(translation, on="product_category_name", how="left")
        .select(
            F.col("order_id"),
            F.col("order_item_id"),
            F.col("customer_unique_id"),
            F.col("order_purchase_timestamp").alias("purchase_ts"),
            F.col("seller_id"),
            F.col("product_id"),
            F.coalesce(
                F.col("product_category_name_english"),
                F.lit("unknown")
            ).alias("category"),
            F.col("price"),
            F.col("freight_value").alias("freight_value"),
            F.col("customer_state").alias("state")
        )
    )

    return sales


# ============================================================================ PART 2
def write_silver(sales, silver_path):
    """Write `sales` as Parquet, one folder per month, ONE tidy file per folder.

    - add a column year_month like '2018-03' (text) derived from purchase_ts
    - folders on disk: year_month=2018-03/ ...
    - think about what the in-memory partitioning must look like BEFORE the write
    - re-running must replace the old output
    - use uri(silver_path) when you give the path to Spark
    """
def write_silver(sales, silver_path):
    """Write sales as Parquet, one folder per month, ONE tidy file per folder."""

    sales_monthly = sales.withColumn(
        "year_month",
        F.date_format(F.col("purchase_ts"), "yyyy-MM")
    )

    (
        sales_monthly
        .repartition(1, "year_month")
        .write
        .mode("overwrite")
        .partitionBy("year_month")
        .parquet(uri(silver_path))
    )


def compare_csv_parquet(sales, workdir):
    """Write `sales` once as CSV (with header) and once as plain Parquet (no partitioning)
    under workdir/sales_csv and workdir/sales_parquet. Return a dict:
        {"csv_mb": float, "parquet_mb": float, "csv_files": int, "parquet_files": int}
    Measure on DISK with folder_size_mb() and count_files(folder, suffix).
    """
    
    csv_dir = Path(workdir) / "sales_csv"
    parquet_dir = Path(workdir) / "sales_parquet"

    fresh_dir(csv_dir)
    fresh_dir(parquet_dir)

    (
        sales.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(uri(csv_dir))
    )

    (
        sales.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(uri(parquet_dir))
    )

    return {
        "csv_mb": folder_size_mb(csv_dir),
        "parquet_mb": folder_size_mb(parquet_dir),
        "csv_files": count_files(csv_dir, ".csv"),
        "parquet_files": count_files(parquet_dir, ".parquet")
    }


# ============================================================================ PART 3
def latest_order_per_customer(sales):
    """One row per customer_unique_id: their MOST RECENT order.
    Columns: customer_unique_id, order_id, purchase_ts.
    Ties on purchase_ts are broken by order_id, larger first (the answer must be repeatable).
    """
    
    window_spec = Window.partitionBy(
        "customer_unique_id"
    ).orderBy(
        F.col("purchase_ts").desc(),
        F.col("order_id").desc()
    )

    result = (
        sales
        .withColumn(
            "row_num",
            F.row_number().over(window_spec)
        )
        .filter(F.col("row_num") == 1)
        .select(
            "customer_unique_id",
            "order_id",
            "purchase_ts"
        )
    )

    return result


def top3_products_per_category(sales):
    """The 3 best-selling products (by revenue) inside each category.
    Columns: category, product_id, revenue, rank (1, 2, 3).
    revenue = round(sum(price), 2). Ties on revenue are broken by product_id, A to Z.
    A category with fewer than 3 products simply returns fewer rows.
    """
    

def top3_products_per_category(sales):
    product_revenue = (
        sales
        .groupBy("category", "product_id")
        .agg(
            F.round(F.sum("price"), 2).alias("revenue")
        )
    )

    window_spec = Window.partitionBy("category").orderBy(
        F.col("revenue").desc(),
        F.col("product_id").asc()
    )

    result = (
        product_revenue
        .withColumn(
            "rank",
            F.row_number().over(window_spec)
        )
        .filter(F.col("rank") <= 3)
        .select(
            "category",
            "product_id",
            "revenue",
            "rank"
        )
    )

    return result
# ============================================================================
# PART 3c

def seller_monthly(sales):
    if "year_month" not in sales.columns:
        sales = sales.withColumn(
            "year_month",
            F.date_format(F.col("purchase_ts"), "yyyy-MM")
        )

    monthly = (
        sales
        .groupBy("seller_id", "year_month")
        .agg(
            F.round(F.sum("price"), 2).alias("month_revenue")
        )
    )

    window_spec = Window.partitionBy(
        "seller_id"
    ).orderBy("year_month")

    monthly = monthly.withColumn(
        "prev_month_revenue",
        F.lag("month_revenue", 1).over(window_spec)
    )

    monthly = monthly.withColumn(
        "mom_growth_pct",
        F.when(
            F.col("prev_month_revenue").isNotNull()
            & (F.col("prev_month_revenue") != 0),
            F.round(
                (
                    (F.col("month_revenue") - F.col("prev_month_revenue"))
                    / F.col("prev_month_revenue")
                ) * 100,
                1
            )
        )
    )

    running_window = window_spec.rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )

    monthly = monthly.withColumn(
        "running_revenue",
        F.round(
            F.sum("month_revenue").over(running_window),
            2
        )
    )

    return monthly

# ============================================================================ PART 4
def read_month(spark, silver_path, year_month):
    """Read ONLY one month from the silver Parquet folder, e.g. year_month='2018-03'.
    Return a DataFrame. The physical plan must show that Spark skips the other folders
    (look for PartitionFilters in plan_text(df)). Do not call an action here.
    """
def read_month(spark, silver_path, year_month):
    df = (
        spark.read
        .parquet(uri(silver_path))
        .filter(F.col("year_month") == year_month)
    )

    return df


# ============================================================================ PART 5
def add_price_band_builtin(df):
    """Add column price_band: 'budget' if price < 20, 'mid' if price < 100, else 'premium'.
    Use ONLY built-in Spark functions (no Python function per row)."""
def add_price_band_builtin(df):
    return df.withColumn(
        "price_band",
        F.when(F.col("price") < 20, "budget")
        .when(F.col("price") < 100, "mid")
        .otherwise("premium")
    )


def add_price_band_udf(df):
    """Same result as above, but written as a Python UDF. (Yes, on purpose: you will measure the cost.)"""
def add_price_band_udf(df):
    def get_price_band(price):
        if price < 20:
            return "budget"
        elif price < 100:
            return "mid"
        else:
            return "premium"

    price_band_udf = F.udf(get_price_band, StringType())

    return df.withColumn(
        "price_band",
        price_band_udf(F.col("price"))
    )


# ============================================================================ PART 6
def write_gold(tables, gold_path):
    """`tables` is a dict {name: DataFrame}. Write each one as Parquet to gold_path/<name>/.
    These tables are small and will be read whole: each folder must hold exactly ONE parquet file
    and no partition sub-folders. Re-running must replace the old output.
    """
def write_gold(tables, gold_path):
    """Write each DataFrame as one Parquet file in its own folder."""

    for name, df in tables.items():
        target = Path(gold_path) / name

        fresh_dir(target)

        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .parquet(uri(target))
        )

# ============================================================================ driver (given)
def main():
    spark = get_spark()

    sales = build_sales(spark)
    print(f"[1] sales rows: {sales.count():,}")

    silver = OUT_DIR / "silver" / "sales"
    write_silver(sales, silver)
    print(f"[2] silver: {count_files(silver)} parquet files, {folder_size_mb(silver):.1f} MB")
    sizes = compare_csv_parquet(sales, OUT_DIR / "compare")
    print(f"    CSV {sizes['csv_mb']:.1f} MB vs Parquet {sizes['parquet_mb']:.1f} MB")

    silver_df = spark.read.parquet(uri(silver))
    tables = {
        "latest_order": latest_order_per_customer(silver_df),
        "top3_products": top3_products_per_category(silver_df),
        "seller_monthly": seller_monthly(silver_df),
    }
    for name, df in tables.items():
        print(f"[3] {name}: {df.count():,} rows")

    march = read_month(spark, silver, "2018-03")
    print(f"[4] 2018-03 rows: {march.count():,}")

    _, t_builtin = timed(lambda: add_price_band_builtin(silver_df).groupBy("price_band").count().collect())
    _, t_udf = timed(lambda: add_price_band_udf(silver_df).groupBy("price_band").count().collect())
    print(f"[5] built-in {t_builtin:.2f}s | UDF {t_udf:.2f}s")

    gold = OUT_DIR / "gold"
    fresh_dir(gold)
    write_gold(tables, gold)
    print(f"[6] gold written: {count_files(gold)} parquet files")
    spark.stop()


if __name__ == "__main__":
    main()
