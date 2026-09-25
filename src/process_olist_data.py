import pandas as pd
from pathlib import Path

# Folders
raw_dir = Path("data/raw")
processed_dir = Path("data/processed")

# Make sure processed folder exists
processed_dir.mkdir(parents=True, exist_ok=True)

# Read raw CSV files
orders = pd.read_csv(raw_dir / "olist_orders_dataset.csv")
customers = pd.read_csv(raw_dir / "olist_customers_dataset.csv")
order_items = pd.read_csv(raw_dir / "olist_order_items_dataset.csv")
products = pd.read_csv(raw_dir / "olist_products_dataset.csv")
order_reviews = pd.read_csv(raw_dir / "olist_order_reviews_dataset.csv")

# Convert order date columns to datetime
order_date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column in order_date_columns:
    orders[column] = pd.to_datetime(orders[column], errors="coerce")

# Convert shipping date to datetime
order_items["shipping_limit_date"] = pd.to_datetime(
    order_items["shipping_limit_date"],
    errors="coerce"
)

# Convert review dates to datetime
order_reviews["review_creation_date"] = pd.to_datetime(
    order_reviews["review_creation_date"],
    errors="coerce"
)

order_reviews["review_answer_timestamp"] = pd.to_datetime(
    order_reviews["review_answer_timestamp"],
    errors="coerce"
)

# Save processed files
orders.to_csv(processed_dir / "olist_orders_dataset.csv", index=False)
customers.to_csv(processed_dir / "olist_customers_dataset.csv", index=False)
order_items.to_csv(processed_dir / "olist_order_items_dataset.csv", index=False)
products.to_csv(processed_dir / "olist_products_dataset.csv", index=False)
order_reviews.to_csv(processed_dir / "olist_order_reviews_dataset.csv", index=False)

print("Processing completed successfully.")