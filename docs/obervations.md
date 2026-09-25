### File: olist_orders_dataset.csv

- Observation:
  The orders dataset contains 99,441 rows and 8 columns. Each row appears to represent one order, identified by `order_id`.

- Evidence (operation/output):
  `orders.shape` returned `(99441, 8)`.
  Inspection using `head()`, `tail()`, `.loc[]`, and `.iloc[]` showed that the dataset contains order identifiers, customer identifiers, order status, purchase timestamp, approval timestamp, carrier delivery date, customer delivery date, and estimated delivery date.

- Why it may matter:
  These columns provide information about the order lifecycle and delivery process. The timestamp fields are important for understanding how long orders take to be delivered.

- Proposed action (or no action):
  Keep the existing columns unless further investigation shows that a specific column requires processing. Date-like columns should be considered for conversion to datetime in the processing stage.

- Reason / assumption to verify:
  The date columns were initially read as object/text values, so their data types should be checked and converted carefully if required.

 ### Order status observation

 - Observation:
  The `order_status` column contains different order lifecycle categories. The notebook confirmed that `delivered` and `canceled` are among the observed statuses.

- Evidence (operation/output):
  `orders["order_status"].unique()` and `orders["order_status"].value_counts()` were used to inspect the categories.
  Filtering with `orders["order_status"] == "canceled"` returned a shape of `(625, 8)`.

- Why it may matter:
  Different order statuses represent different stages or outcomes of an order. Cancelled orders should not be treated in the same way as successfully delivered orders when calculating delivery performance.

- Proposed action (or no action):
  Preserve the status categories. When analysing delivery time, use delivered orders with a valid customer delivery date rather than treating cancelled orders as delivered.

- Reason / assumption to verify:
  The meaning of each status should be understood before applying any cleaning rule.

  ### Delivery-time observation

  - Observation:
  A `delivery_days` field was calculated for orders with a customer delivery date by subtracting the purchase timestamp from the customer delivery date.

- Evidence (operation/output):
  The resulting `delivery_days` summary showed:
  count = 96,476
  mean = 12.094086 days
  standard deviation = 9.551746 days
  minimum = 0 days
  25% = 6 days
  50% = 10 days
  75% = 15 days
  maximum = 209 days

- Why it may matter:
  Most delivery times are concentrated around the lower values, but the maximum of 209 days is considerably higher than the median. This may indicate unusually long delivery cases that deserve investigation.

- Proposed action (or no action):
  Do not automatically remove long delivery times. Keep them unless a business rule confirms that they are invalid. The `delivery_days` calculation can be useful for later analysis.

- Reason / assumption to verify:
  A long delivery time may be a genuine business event rather than an error, so the underlying order should be investigated before removing it.

  ### Duplicate observation

  - Observation:
  Potential duplicate rows should be checked before deciding whether duplicates need to be removed.

- Evidence (operation/output):
  The assignment requires `orders.duplicated().sum()` to be used to identify completely duplicated rows.

- Why it may matter:
  Duplicate records could affect order counts and downstream analysis.

- Proposed action (or no action):
  Only remove duplicates if the evidence confirms that they are true duplicate records. Otherwise, preserve the records.

- Reason / assumption to verify:
  A repeated identifier or similar-looking record does not automatically mean that the record is a duplicate.

  ###  Overall decision for orders

  - Observation:
  The orders file is suitable for further processing, but several fields require careful handling, particularly timestamp columns, missing delivery information, order statuses, and unusually long delivery durations.

- Evidence (operation/output):
  The dataset was inspected using `head()`, `tail()`, `shape`, `info()`, `describe()`, missing-value checks, duplicate checks, category counts, row selection, filtering, sorting, and delivery-time calculation.

- Why it may matter:
  These checks establish a baseline for the raw data and help ensure that processing decisions are based on evidence.

- Proposed action (or no action):
  Preserve the raw orders file unchanged. Process a separate copy and apply only justified transformations.

- Reason / assumption to verify:
  Any cleaning decision should be validated against the business meaning of the fields rather than based only on unusual values.


  ## Customers — olist_customers_dataset.csv

### Observation 1 — Customer records and identifiers
The customers dataset contains 99,441 records. `customer_id` has 99,441 unique values, while `customer_unique_id` has 96,096 unique values.

**Evidence:** `customers.shape`, `customers["customer_id"].nunique()`, and `customers["customer_unique_id"].nunique()`.

**Why it matters:** `customer_id` uniquely identifies the records, while `customer_unique_id` can represent the same customer across multiple records.

**Proposed action:** Preserve both identifier columns and do not remove the repeated `customer_unique_id` records.

**Reason/assumption:** The repeated unique-customer identifiers may represent multiple customer records and should not be treated as accidental duplicates without further evidence.

### Observation 2 — Customer location
The dataset contains customer city, state, and ZIP-code-prefix information. There are 27 unique states and 14,994 unique ZIP-code prefixes.

**Evidence:** `customers["customer_state"].nunique()` and `customers["customer_zip_code_prefix"].nunique()`.

**Why it matters:** These fields provide geographic information that may be useful when joining or analyzing customer-related data.

**Proposed action:** Preserve the geographic columns without removing or changing their values.

**Reason/assumption:** No evidence from the exploration justifies altering these location fields.

### Observation 3 — Duplicate rows
The duplicate-row check was performed using `customers.duplicated().sum()`.

**Evidence:** Duplicate check performed during exploration.

**Why it matters:** Completely duplicated rows could affect analysis.

**Proposed action:** Do not remove rows unless they are confirmed to be erroneous duplicates.

**Reason/assumption:** A repeated `customer_unique_id` does not by itself mean that the complete customer record is duplicated.


## Order Items — olist_order_items_dataset.csv

### Observation 1 — Order-item records and relationships
The dataset contains 112,650 order-item records, representing 98,666 unique orders, 32,951 unique products, and 3,095 unique sellers.

**Evidence:** `order_items.shape`, `product_id.nunique()`, `order_id.nunique()`, and `seller_id.nunique()`.

**Why it matters:** An order can contain multiple products, and products and sellers can appear across multiple orders.

**Proposed action:** Preserve the order, product, and seller identifiers without removing repeated IDs.

**Reason/assumption:** Repeated identifiers represent relationships between records rather than duplicate rows.

### Observation 2 — Multiple items per order
The `order_item_id` values range from 1 to 21, and the value-count check shows that some orders contain multiple item records.

**Evidence:** `order_items["order_item_id"].describe()` and `order_items["order_id"].value_counts()`.

**Why it matters:** The data is at the order-item level rather than one row per order.

**Proposed action:** Preserve all order-item records.

**Reason/assumption:** Multiple item records within an order are expected and should not be treated as duplicates.

### Observation 3 — Missing values and duplicate rows
No missing values were found in the seven columns, and the complete-row duplicate check returned 0.

**Evidence:** `order_items.isna().sum()` and `order_items.duplicated().sum()`.

**Why it matters:** No missing-value handling or duplicate-row removal is currently justified.

**Proposed action:** Keep the records unchanged.

**Reason/assumption:** The exploration found no missing values or exact duplicate rows requiring correction.

### Observation 4 — Price and freight values
The `price` column ranges from 0.85 to 6,735.00, with a median of 74.99 and mean of 120.65. The `freight_value` column ranges from 0.00 to 409.68, with a median of 16.26 and mean of 19.90.

**Evidence:** `price.describe()` and `freight_value.describe()`.

**Why it matters:** Price and freight are important numerical fields for order-value analysis.

**Proposed action:** Preserve the values without removing high-value observations.

**Reason/assumption:** The exploration alone does not establish that unusually high values are errors.

### Observation 5 — Zero freight values
There are 383 records where `freight_value` is zero. No records have a price less than or equal to zero.

**Evidence:** `(order_items["price"] <= 0).sum()` and `(order_items["freight_value"] <= 0).sum()`.

**Why it matters:** Zero freight may have a valid business meaning and should not automatically be treated as missing or erroneous.

**Proposed action:** Keep zero freight values.

**Reason/assumption:** There is no evidence from this exploration that zero freight represents invalid data.

### Observation 6 — Shipping limit date
The `shipping_limit_date` field was converted to datetime. The observed dates range from September 2016 to April 2020.

**Evidence:** `pd.to_datetime()` and `shipping_limit_date.describe()`.

**Why it matters:** This field represents a date/time value and should be stored as a datetime for future analysis.

**Proposed action:** Preserve the field as datetime in processing.

**Reason/assumption:** Datetime storage allows date-based analysis and avoids treating timestamps as ordinary text.

## Order Reviews — olist_order_reviews_dataset.csv

### Observation 1 — Review records and identifiers
The dataset contains 99,224 review records, 98,410 unique review IDs, and 98,673 unique orders.

**Evidence:** `order_reviews.shape`, `review_id.nunique()`, and `order_id.nunique()`.

**Why it matters:** The number of review records differs from the number of unique review IDs and orders, so the review data should not automatically be assumed to contain one unique review per row or one review per order.

**Proposed action:** Preserve the review records and identifier columns.

**Reason/assumption:** Repeated identifiers should not be removed unless they are confirmed to represent erroneous duplicate records.

### Observation 2 — Duplicate rows
The complete-row duplicate check returned 0.

**Evidence:** `order_reviews.duplicated().sum()`.

**Why it matters:** No completely duplicated rows were identified during exploration.

**Proposed action:** Do not remove rows based on duplicate-row detection.

**Reason/assumption:** The exploration provides no evidence that any records are accidental duplicates.

### Observation 3 — Review scores
Review scores range from 1 to 5, with no values outside that range. The distribution contains 11,424 one-star reviews, 3,151 two-star reviews, 8,179 three-star reviews, 19,142 four-star reviews, and 57,328 five-star reviews. The mean score is approximately 4.086 and the median is 5.

**Evidence:** `review_score.value_counts().sort_index()` and `review_score.describe()`.

**Why it matters:** Review score is an important categorical/ordinal field for customer-feedback analysis.

**Proposed action:** Preserve the review scores as recorded.

**Reason/assumption:** All observed values are within the expected 1–5 range.

### Observation 4 — Missing review comments
There are 87,656 missing review-comment titles and 58,247 missing review-comment messages. Only 11,568 titles and 40,977 messages are present.

**Evidence:** `order_reviews.isna().sum()`.

**Why it matters:** Written comments are optional information and their absence does not necessarily indicate an invalid review.

**Proposed action:** Preserve the missing values rather than filling them with invented text.

**Reason/assumption:** There is no evidence that missing comments represent data errors.

### Observation 5 — Review dates
`review_creation_date` and `review_answer_timestamp` were initially stored as strings and were converted to datetime values during exploration.

**Evidence:** The original `info()` output showed both columns as `str`; after conversion, both columns have datetime dtypes.

**Why it matters:** Date/time fields should use datetime types for chronological analysis.

**Proposed action:** Preserve these fields as datetime values during processing.

**Reason/assumption:** The values represent dates/timestamps and therefore benefit from datetime representation.

## Products Data Exploration

### Observation 1 — Dataset structure
- File: `olist_products_dataset.csv`
- Shape: 32,951 rows and 9 columns.
- Each row represents a product.
- `product_id` is unique for all 32,951 rows.
- Action: Preserve the product records and use `product_id` as the product identifier.
- Reason: The identifier is unique and is needed for relationships with order-item data.

### Observation 2 — Missing values
- Missing values were found in several product attributes.
- `product_category_name`, `product_name_length`, `product_description_length`, and `product_photos_qty` each have 610 missing values.
- The physical-dimension columns have 2 missing values each.
- Action: Do not invent missing product information.
- Reason: Missing product attributes may represent unavailable source information, so filling them without evidence could introduce incorrect data.

### Observation 3 — Product categories
- There are 73 unique product categories.
- The most frequent categories include `cama_mesa_banho`, `esporte_lazer`, `moveis_decoracao`, and `beleza_saude`.
- Action: Preserve the existing category values.
- Reason: The categories are useful for product-level analysis and should not be arbitrarily regrouped.

### Observation 4 — Product dimensions and unusual values
- Four values in `product_weight_g` are less than or equal to zero.
- No non-positive values were found for product length, height, or width.
- The maximum product weight is 40,425 g.
- Action: Do not automatically remove or replace the four non-positive weights.
- Reason: The source data should remain traceable, and there is not enough evidence to determine the correct replacement values.

### Observation 5 — Duplicate products
- No completely duplicated rows were found.
- `product_id` has 32,951 unique values, matching the number of rows.
- Action: No duplicate removal is required.
- Reason: Each product has a unique identifier and the dataset contains no complete duplicate rows.

### Overall decision
- Keep the raw products data unchanged.
- In processing, convert/retain appropriate data types and preserve missing values unless a documented transformation is justified.
- Do not remove unusual weight values without additional evidence.