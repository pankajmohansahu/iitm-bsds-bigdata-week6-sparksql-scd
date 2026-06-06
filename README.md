# Week 6 Graded Assignment 5 — Intro to Big Data
## GCP Dataproc + SparkSQL: Aggregations & Slowly Changing Dimensions (SCD)

---

## Objectives

1. **Deploy a distributed big data pipeline** on Google Cloud Platform using Dataproc and Apache Spark.
2. **Practice pure SparkSQL** — all transformations are implemented using SQL queries only; no DataFrame API is used.
3. **Perform aggregation analytics** on transactional data — ranking, grouping, and summarising across 200 transactions and 10 customers.
4. **Implement Slowly Changing Dimensions (SCD)**:
   - **SCD Type I** — overwrite stale dimension records in-place (no history retained).
   - **SCD Type II** — preserve full change history by expiring old rows and inserting new current rows.
5. **Understand GCS as distributed storage** for both input data and output results in a cloud-native Spark workflow.
6. **Validate dimension table integrity** before and after each SCD operation using SQL `show()` outputs and row-count checks.

---

## File Descriptions

### Code Scripts

| File | Purpose |
|------|---------|
| `spark_assignment.py` | Main PySpark script. Uses **only SparkSQL** (no DataFrame API). Reads input CSVs from GCS, runs all aggregation queries, implements SCD Type I and SCD Type II, and writes all results back to GCS. |

### Input Files (uploaded to GCS `/data/`)

| File | Description |
|------|-------------|
| `customer_master.csv` | Original customer dimension table with **10 customers** (c001–c010). Contains SCD fields: `effective_date` (all set to `2025-01-01`), `expiry_date` (NULL), `current_flag` (all `1`). Customers span 5 cities: Bengaluru, Chennai, Coimbatore, Hyderabad, Mumbai. |
| `customer_updates.csv` | **5 customer update records** (c002, c007, c008, c009, c010) with updated `customer_name`, `city`, `dob`, and a `change_date` of `2025-10-23`. Used to drive both SCD Type I and SCD Type II transformations. |
| `transactions.csv` | **200 transaction records** (t0001–t0200) with `transaction_id`, `customer_id`, `transaction_date`, `transaction_amount`, `payment_mode`, `city`. Covers Jan–Mar 2025. Payment modes: cash, card, upi, wallet, netbanking. |

### Output Files (written to GCS `/output/`)

| Folder | Contents |
|--------|----------|
| `top5_customers/` | CSV: Top 5 customers ranked by total transaction amount (SparkSQL JOIN + GROUP BY + ORDER BY). |
| `customers_per_city/` | CSV: Count of customers grouped by city from the customer master table. |
| `avg_transaction_value/` | CSV: Overall average transaction value, total transaction count, and grand total across all 200 transactions. |
| `scd_type1_customer_master/` | CSV: Updated customer master after **SCD Type I** — old values overwritten with new values; 10 rows in, 10 rows out. |
| `scd_type2_customer_master/` | CSV: Updated customer master after **SCD Type II** — old records expired, new records inserted; 10 rows in, **15 rows out** (5 unchanged + 5 expired + 5 new current). |

---

## SCD Logic Summary

### SCD Type I (Overwrite)
- Updated fields (`customer_name`, `city`, `dob`) are **overwritten** in place using `COALESCE(new_value, old_value)` via a `LEFT JOIN`.
- No historical record is kept. The table always reflects the latest state.
- Row count stays the same: **10 rows → 10 rows**.
- **SQL approach**: `LEFT JOIN customer_updates` + `COALESCE()` on changed columns.

### SCD Type II (Add Row)
- The **old record** is retained with `expiry_date = change_date` and `current_flag = 0`.
- A **new record** is inserted with `effective_date = change_date`, `expiry_date = NULL`, `current_flag = 1`.
- Full history is preserved. Records not in updates carry over unchanged.
- Row count increases: **10 rows → 15 rows** (5 unchanged + 5 expired + 5 new).
- **SQL approach**: Three-part `UNION ALL` — unchanged rows + expired old rows + new current rows.

---

## GCP Setup Used

- **Platform**: Google Cloud Platform (GCP)
- **Compute**: Google Cloud Dataproc (Single-node cluster, `n1-standard-2`)
- **Storage**: Google Cloud Storage (GCS) — bucket [`gs://bigdata-iitm-week5`](https://console.cloud.google.com/storage/browser/bigdata-iitm-week5)
- **Runtime**: Apache Spark 2.1 on Debian 11
- **Job type**: PySpark job submitted via `gcloud dataproc jobs submit pyspark`

---

## How to Run

```bash
# 1. Create bucket and upload inputs
gsutil mb gs://bigdata-iitm-week5
gsutil cp *.csv gs://bigdata-iitm-week5/data/
gsutil cp spark_assignment.py gs://bigdata-iitm-week5/

# 2. Create Dataproc cluster
gcloud dataproc clusters create bigdata-cluster \
    --region=us-central1 --single-node \
    --master-machine-type=n1-standard-2 \
    --image-version=2.1-debian11

# 3. Submit job
gcloud dataproc jobs submit pyspark \
    gs://bigdata-iitm-week5/spark_assignment.py \
    --cluster=bigdata-cluster --region=us-central1

# 4. Download outputs
gsutil -m cp -r gs://bigdata-iitm-week5/output/ ./output/
```

---

## Findings

### Dataset Observations

| Metric | Value |
|--------|-------|
| Total customers | 10 (c001–c010) |
| Customers updated via SCD | 5 (c002, c007, c008, c009, c010) |
| Customers unchanged | 5 (c001, c003, c004, c005, c006) |
| Total transactions | 200 (Jan–Mar 2025) |
| Payment modes available | 5 (cash, card, upi, wallet, netbanking) |
| Cities in customer master | 5 (Bengaluru, Chennai, Coimbatore, Hyderabad, Mumbai) |

### Customer Distribution by City (from `customer_master.csv`)

| City | Customers |
|------|-----------|
| Bengaluru | 3 (c001, c005, c009) |
| Chennai | 3 (c003, c007, c010) |
| Coimbatore | 2 (c004, c006) |
| Hyderabad | 1 (c002) |
| Mumbai | 1 (c008) |

### SCD Type I — What Changed

| customer_id | Old Name | New Name | Old City | New City | Old DOB | New DOB |
|-------------|----------|----------|----------|----------|---------|---------|
| c002 | Customer_002 | Customer_002_Upd | Hyderabad | Kochi | 2003-09-28 | 2003-09-28 |
| c007 | Customer_007 | Customer_007_Upd | Chennai | Bengaluru | 1985-04-28 | 1985-03-30 |
| c008 | Customer_008 | Customer_008 | Mumbai | Bengaluru | 1996-11-04 | 1996-11-14 |
| c009 | Customer_009 | Customer_009 | Bengaluru | Hyderabad | 1998-06-15 | 1998-07-25 |
| c010 | Customer_010 | Customer_010 | Chennai | Pune | 1981-04-01 | 1981-04-01 |

### SCD Type II — Row Count Before vs After

| State | current_flag=1 | current_flag=0 | Total Rows |
|-------|---------------|---------------|------------|
| Before (original) | 10 | 0 | 10 |
| After SCD Type II | 5 (unchanged) + 5 (new) = 10 | 5 (expired) | 15 |

- All 5 updated customers now have **two rows**: one expired historical record (change_date `2025-10-23` as `expiry_date`) and one active current record.
- All 5 unchanged customers (c001, c003, c004, c005, c006) carry over with their original `effective_date = 2025-01-01` and `current_flag = 1`.

### Aggregation Results (Expected)

- **Top 5 customers** are determined by summing `transaction_amount` grouped by `customer_id`, joined with `customer_master` for names.
- **Customers per city** reflects the original dimension table distribution (pre-SCD).
- **Average transaction value** is computed across all 200 records — amounts range from ~205 to ~9,979.

---

## Key Learnings

### 1. SparkSQL vs DataFrame API
- SparkSQL enables SQL-first big data processing — any analyst familiar with SQL can write Spark jobs without learning the DataFrame API.
- `createOrReplaceTempView()` is the bridge: it registers a DataFrame as a SQL-queryable table in the Spark session's catalog.
- Complex multi-step transformations (SCD Type II's three-way `UNION ALL`) are naturally expressed in SQL and execute efficiently on Spark's distributed engine.

### 2. SCD Type I vs Type II Trade-offs

| Aspect | SCD Type I | SCD Type II |
|--------|-----------|------------|
| History preserved? | No | Yes |
| Row count after update | Same (10) | Increases (15) |
| Storage cost | Lower | Higher |
| Query complexity | Simpler | Needs `WHERE current_flag=1` filter for latest |
| Use case | Non-critical attribute changes | Audit trails, time-travel analytics |

### 3. COALESCE for Safe Overwrite (Type I)
- `COALESCE(cu.column, cm.column)` elegantly handles the Type I merge: if a new value exists from `customer_updates`, use it; otherwise keep the original. This avoids NULL overwrites for customers not in the update set.

### 4. UNION ALL Pattern for SCD Type II
- The three-part `UNION ALL` is the canonical pure-SQL approach for SCD Type II:
  - **Part 1**: Rows not touched — pass through as-is.
  - **Part 2**: Old versions of updated rows — close them (`current_flag=0`, `expiry_date = change_date`).
  - **Part 3**: New versions from the update set — open them (`current_flag=1`, `effective_date = change_date`, `expiry_date = NULL`).
- This pattern works entirely within SQL with no procedural logic or loops.

### 5. GCP Dataproc Workflow
- Dataproc decouples compute (cluster) from storage (GCS) — input and output data persist in GCS independently of cluster lifecycle.
- Submitting a PySpark job via `gcloud dataproc jobs submit pyspark` streams logs back to the terminal in real time, making debugging straightforward.
- Single-node clusters (`--single-node`) are sufficient for small-to-medium datasets and significantly reduce cost during development and testing.

### 6. Schema Inference on CSV
- `inferSchema=True` in `spark.read.csv()` automatically detects date strings and numeric types, avoiding the need to manually define a `StructType` schema for well-formatted CSV files.
- Using `header=True` ensures the first row is treated as column names rather than data.

### 7. Output as Partitioned CSV in GCS
- Spark writes output as a **directory** of part files (e.g., `part-00000-*.csv`), not a single file. This is normal distributed behaviour.
- Use `gsutil -m cp -r` to download all part files at once.
- The `.option("header", "true")` flag ensures each part file includes a header row for readability.
