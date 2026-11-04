# Reflection: Assignment 5 (Spark windows and Parquet)

Answer in your own words, with numbers from YOUR runs. Two to four sentences each.

**1. CSV vs Parquet.** What were the sizes of `sales` as CSV and as Parquet on your machine? Name two reasons Parquet is smaller and one reason it is faster for a query that needs only `price`.
** Answer: On my machine, the sales dataset was 20.816 MB as CSV and 9.872 MB as Parquet.

Parquet is smaller for two main reasons:
1. It uses efficient compression and encoding techniques.
2. It stores data in a columnar format, which can represent repeated values efficiently.
Parquet is faster when a query needs only price because Spark can read just that column instead of reading every column in the dataset. This is called column pruning.

**2. Tidy write.** How many files did `write_silver` produce, and how many did it produce when you removed `repartition`? Explain the difference in terms of memory partitions and folders on disk.
** Answer: write_silver produced 24 monthly folders, with one Parquet file per folder, for 24 files in total.
The repartition(1, "year_month") operation reshuffles data so each month is written from one Spark partition. Without it, the existing memory partitions can produce multiple files inside each monthly folder.
The exact number of files without repartition was not recorded.

**3. Window design.** For `latest_order_per_customer`, what would change in the output if you removed the `order_id` tie-break? Did it change on your data? How did you find out?
** Answers: I used order_id as a tie-breaker when ordering by purchase_ts. Without this tie-breaker, if a customer has multiple orders with the same purchase timestamp, Spark may choose either order. The result can be nondeterministic.
Whether this changed the output on my data was not recorded.

**4. Rank functions.** In `top3_products_per_category`, how many rows do you get with `dense_rank` ordered by revenue only, compared with `row_number`? Which would you choose for a dashboard and why?
** Answers: row_number gives each product a unique rank, so there are at most three products per category.
dense_rank gives tied products the same rank. Therefore, more than three rows can appear in a category if multiple products tie within the top three ranks.
I would choose row_number for a dashboard that must show exactly three products per category, and dense_rank if I want to include all products tied in the top three positions.
The exact dense_rank row count was not recorded. The row_number result had 213 rows.

**5. The one-partition trap.** What warning did Spark print when you removed `partitionBy` from a window? What did the Stage page in the Spark UI show (number of tasks), and what would happen at 100 GB?
** Answers: A window without partitionBy can cause Spark to move all data into one partition. This can create a performance bottleneck because the window operation cannot be distributed across multiple groups.
At 100 GB, this could cause very slow processing, high memory pressure, or even an out-of-memory failure.
The exact Spark warning and Stage task count from my run were not recorded.

**6. Pruning.** Paste the `PartitionFilters` line from `plan_text(...)` for the month read and for the version that filters on `purchase_ts` instead. What is the difference, and why does it matter?
** Answers: For the month read, Spark's physical plan showed:
PartitionFilters: [isnotnull(year_month#477), (year_month#477 = 2018-03)]
This shows that Spark can use the partition column to read only the March 2018 partition. The corresponding purchase_ts filter plan was not recorded.
Filtering directly on the partition column allows partition pruning, avoiding unnecessary reads from other month folders.

**7. UDF.** Built-in vs UDF timings on your machine, and the ratio. Would you ever ship the UDF? Name one case where a UDF is unavoidable.
** Answers: The built-in Spark expression and Python UDF produced the same price bands:
- Budget: 9,268 rows
- Mid: 62,897 rows
- Premium: 40,485 rows
A built-in Spark expression is generally faster because Spark can optimize it without transferring data to Python for each row.
I would avoid shipping a Python UDF when a built-in Spark function can do the same job. A UDF may be necessary when the required logic cannot be expressed using available Spark functions.
The exact timings and ratio were not recorded.

**8. Over-partitioning.** How many folders and files did `partitionBy("seller_id")` create, and how large was the average file? Why is that a problem for the next job that reads it?
** Answers: Partitioning by seller_id can create many folders and small files because each seller gets a separate partition folder.
This is a problem for the next job because Spark must discover and read many small files, increasing file-management overhead and potentially slowing down processing.
The exact folder count, file count, and average file size were not recorded.

**9. What surprised you,** and one thing you would still like to understand better.
** Answers: I was surprised that Parquet was significantly smaller than CSV, and that partitioning could make such a difference to query performance. I also learned that having too many small files can be a problem, even when the data is organized into partitions.
One thing I would still like to understand better is how Spark decides the number of tasks and memory partitions during execution, and how this affects performance on larger datasets.
