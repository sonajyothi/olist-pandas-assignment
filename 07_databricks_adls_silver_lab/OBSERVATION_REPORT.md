# OBSERVATION REPORT

## 1. Why I stored untouched files in `raw/`
I first downloaded the three monthly NYC taxi Parquet files and the single NYC daily weather JSON into the `raw/` area of the lake before transforming anything. I did this so the pipeline would have a reproducible evidence layer: if Bronze or Silver logic is wrong later, I can rebuild from the lake’s own copy instead of downloading from the internet again. Keeping the source files unchanged also makes auditing easier because the raw layer preserves the exact original payload before any schema changes, filtering, or type conversions. The validation output confirmed that `raw/taxi` contains exactly the three expected monthly Parquet files and `raw/weather` contains the expected JSON file.

## 2. Bronze ingestion observations
### 2.1 Bronze taxi
For Bronze taxi, I combined the August, September, and October 2025 raw Parquet files into one Delta dataset while keeping the 20 source columns exactly as delivered by the source system. I only added three metadata columns: `source_file`, `source_month`, and `ingested_at`. This matches the Bronze goal of preserving source evidence while adding lineage information. The final validated Bronze row counts by month were:

* `2025-08`: 3,574,091
* `2025-09`: 4,251,015
* `2025-10`: 4,428,699

These counts matched the raw files exactly, which shows no rows were lost and no duplicate rows were introduced during Bronze ingestion. The Bronze taxi schema also preserved the original source typing patterns that stood out during observation: `passenger_count` arrived as `long`, both trip timestamp columns arrived as `timestamp_ntz`, and `RatecodeID` also arrived as `long`. Although passenger count sounds like a small business value, source systems often use a wider integer type for consistency across pipelines or because upstream ingestion tools infer a generic integer type rather than the narrowest possible business type.

### 2.2 Bronze weather
The weather JSON came as one object containing parallel arrays such as `time`, `temperature_2m_max`, `temperature_2m_min`, `precipitation_sum`, `snowfall_sum`, and `wind_speed_10m_max`. That shape is awkward for analytics because each metric is stored in a separate array position rather than as one row that represents one day. I flattened those arrays into one row per date, then added `source_file` and `ingested_at`. This row-based structure makes filtering by day, joining on date, counting days, and plotting trends straightforward. The final Bronze weather table had 92 rows and 92 distinct dates, which matches the full range from 2025-08-01 through 2025-10-31.

## 3. Bronze validation and rerunnability
The Bronze validation cell finished with 10 out of 10 checks passed. That evidence confirmed the following:

* both Bronze tables are external
* the Bronze taxi table points to `/bronze/taxi_trips`
* the Bronze weather table points to `/bronze/weather_daily`
* Bronze taxi is stored as Delta
* the 20 source taxi columns were preserved unchanged, plus the 3 metadata columns
* Bronze taxi row counts per month matched the raw files
* Bronze weather contains 92 rows and 92 distinct dates
* Bronze weather kept the API column names

The current validated Bronze totals are the month counts listed above. I could verify the final validated counts from the notebook outputs, but I did not re-run the Bronze write logic in this turn, so this report documents the verified final state rather than a new before-versus-after rerun snapshot.

## 4. Bronze external table registration
The `bronze` schema was registered in Unity Catalog and both Bronze tables were created as external Delta tables over ADLS locations. The `DESCRIBE EXTENDED` evidence showed:

* `sona_db_practices.bronze.taxi_trips` → Type: `EXTERNAL`, Provider: `delta`, Location: `abfss://lakehouse@dkdedls01.dfs.core.windows.net/bronze/taxi_trips`
* `sona_db_practices.bronze.weather_daily` → Type: `EXTERNAL`, Provider: `delta`, Location: `abfss://lakehouse@dkdedls01.dfs.core.windows.net/bronze/weather_daily`

An important observation is that dropping an external table removes the Unity Catalog metadata entry, but it does not delete the underlying files in ADLS. That is different from managed-table behavior and is why external registration is useful when storage location and table metadata need to stay loosely coupled.

## 5. Silver layer observations
### 5.1 Silver taxi cleaning and rejected rows
The Silver layer converts Bronze evidence into analysis-ready tables. In `silver.taxi_trips_rejected`, invalid taxi records were separated from valid ones using five agreed reject reasons:

* `distance_invalid`
* `dropoff_not_after_pickup`
* `duration_over_6h`
* `fare_invalid`
* `pickup_outside_source_month`

The validated reject counts were:

* `distance_invalid`: 351,499
* `dropoff_not_after_pickup`: 172,980
* `duration_over_6h`: 3,490
* `fare_invalid`: 766,477
* `pickup_outside_source_month`: 37

Example rejected rows confirmed the logic matched the business rules. For example, distance-invalid rows included trips with `trip_distance = 0`, dropoff-invalid rows included trips where pickup and dropoff timestamps were equal, duration-invalid rows included trips lasting more than 6 hours, fare-invalid rows included negative fares, and pickup-outside-source-month rows showed trips whose pickup timestamp fell outside the file’s labeled month.

### 5.2 Silver taxi standardization
After excluding rejected rows, the Silver taxi table standardized names and types for analytics. For example, the source `VendorID` became `vendor_id`, the NTZ timestamps were cast to standard `timestamp` columns `pickup_ts` and `dropoff_ts`, and numeric business fields were cast into cleaner reporting types such as `decimal(8,2)` and `decimal(10,2)`. Passenger count was cleaned to business-safe values by keeping the trip but replacing counts outside 1–6 with `NULL`. This shows the main difference between Bronze and Silver: Bronze preserves source evidence exactly, while Silver applies business rules and presentation-friendly typing.

### 5.3 Critical Silver bug and fix
The most important Silver debugging observation was that rejected rows containing nulls were leaking into `silver.taxi_trips`. The cause was the anti join used to exclude rejected rows: a normal equality-based join does not treat nulls as equal in Spark, so rejected rows with null values in the source columns failed to match and incorrectly stayed in the “good rows” dataset. The fix was to change the exclusion logic to a null-safe anti join across all source columns using null-safe equality. After that correction, invalid rows no longer leaked into Silver and the row-count reconciliation started matching exactly.

## 6. Silver validation results
The final Silver validation cell finished with 14 out of 14 checks passed. The validated results showed that:

* all three Silver tables are external
* `silver.taxi_trips` has exactly the agreed columns and data types
* no Silver trip has duration less than or equal to 0 minutes or greater than 360 minutes
* no Silver trip has fare less than or equal to 0 or greater than or equal to 500
* no Silver trip has distance less than or equal to 0 or greater than or equal to 100
* `passenger_count` is either null or between 1 and 6
* every pickup falls inside its own `source_month`
* the rejected table includes `reject_reason` and `rejected_at`
* every reject reason is one of the five agreed strings
* `silver.weather_daily` has the agreed columns and types
* `silver.weather_daily` contains 92 rows, one per date

The final reconciled row totals were:

* Bronze: 12,253,805
* Silver: 10,959,322
* Rejected: 1,294,483
* Duplicates removed: 0

This proves the contract equation holds exactly:

`bronze = silver + rejected + duplicates_removed`

## 7. Rainy-versus-dry join observation
The final join query between `silver.taxi_trips` and `silver.weather_daily` succeeded and returned 92 daily rows, one for each pickup date, with a derived rainy/dry classification. This demonstrated why the Silver layer exists: once taxi pickups were standardized to `pickup_date` and weather was standardized to `weather_date`, the business join became simple and reliable. Sample output showed days such as 2025-08-01 labeled rainy with 118,038 trips and 2025-08-02 labeled dry with 123,058 trips. The important observation is not only the numbers but the fact that cleaned, typed, date-aligned Silver tables make cross-domain analysis possible with a straightforward join.

## 8. Overall reflection
This two-notebook pipeline demonstrates the purpose of layered lakehouse design clearly. The raw layer protects the original internet files. Bronze preserves source structure while adding lineage metadata. Silver enforces business rules, separates bad records, standardizes names and types, and produces clean data ready for joins and analysis. The debugging work on Silver also showed that pipeline correctness is not only about syntax or schema; join semantics, especially around null handling, can change business results materially. Once that null-safe anti join was fixed, the pipeline satisfied both the schema contract and the row-count reconciliation contract.
