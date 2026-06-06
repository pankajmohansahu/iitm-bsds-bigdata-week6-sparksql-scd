"""
Week 6 Graded Assignment 5 - Big Data
SparkSQL on GCP Dataproc: Aggregations + SCD Type I & II
Author: Student
"""

from pyspark.sql import SparkSession

# ─────────────────────────────────────────────
# CONFIG  →  change only this line
# ─────────────────────────────────────────────
BUCKET = "gs://bigdata-iitm-week5"
INPUT  = f"{BUCKET}/data"
OUTPUT = f"{BUCKET}/output"
# ─────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("BigData_Week5_SCD_Assignment") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ══════════════════════════════════════════════
# 1. LOAD DATA FROM GCS AND CREATE TEMP VIEWS
# ══════════════════════════════════════════════

spark.read.csv(
    f"{INPUT}/customer_master.csv",
    header=True, inferSchema=True
).createOrReplaceTempView("customer_master")

spark.read.csv(
    f"{INPUT}/customer_updates.csv",
    header=True, inferSchema=True
).createOrReplaceTempView("customer_updates")

spark.read.csv(
    f"{INPUT}/transactions.csv",
    header=True, inferSchema=True
).createOrReplaceTempView("transactions")

print("\n" + "="*60)
print("DATA LOADED SUCCESSFULLY")
print("="*60)

print("\n--- customer_master (raw) ---")
spark.sql("SELECT * FROM customer_master ORDER BY customer_id").show(truncate=False)

print("\n--- customer_updates (raw) ---")
spark.sql("SELECT * FROM customer_updates ORDER BY customer_id").show(truncate=False)

print("\n--- transactions sample (first 10) ---")
spark.sql("SELECT * FROM transactions LIMIT 10").show(truncate=False)

# ══════════════════════════════════════════════
# 2. AGGREGATION QUERIES
# ══════════════════════════════════════════════

print("\n" + "="*60)
print("AGGREGATION QUERIES")
print("="*60)

# ── Q1: Top 5 customers by total transaction amount ──────────
print("\n--- Q1: Top 5 Customers by Total Transaction Amount ---")
top5_customers = spark.sql("""
    SELECT
        t.customer_id,
        cm.customer_name,
        SUM(t.transaction_amount) AS total_transaction_amount
    FROM transactions t
    JOIN customer_master cm ON t.customer_id = cm.customer_id
    GROUP BY t.customer_id, cm.customer_name
    ORDER BY total_transaction_amount DESC
    LIMIT 5
""")
top5_customers.show(truncate=False)
top5_customers.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{OUTPUT}/top5_customers")
print(f"Saved → {OUTPUT}/top5_customers")

# ── Q2: Number of customers per city ────────────────────────
print("\n--- Q2: Number of Customers per City ---")
customers_per_city = spark.sql("""
    SELECT
        city,
        COUNT(customer_id) AS number_of_customers
    FROM customer_master
    GROUP BY city
    ORDER BY number_of_customers DESC
""")
customers_per_city.show(truncate=False)
customers_per_city.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{OUTPUT}/customers_per_city")
print(f"Saved → {OUTPUT}/customers_per_city")

# ── Q3: Average transaction value overall ────────────────────
print("\n--- Q3: Average Transaction Value (Overall) ---")
avg_transaction = spark.sql("""
    SELECT
        ROUND(AVG(transaction_amount), 2) AS avg_transaction_value,
        COUNT(*)                          AS total_transactions,
        SUM(transaction_amount)           AS grand_total
    FROM transactions
""")
avg_transaction.show(truncate=False)
avg_transaction.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{OUTPUT}/avg_transaction_value")
print(f"Saved → {OUTPUT}/avg_transaction_value")

# ══════════════════════════════════════════════
# 3. SCD TYPE I  –  Overwrite (no history kept)
#    Latest values replace old values in-place.
# ══════════════════════════════════════════════

print("\n" + "="*60)
print("SCD TYPE I  (Overwrite – no history)")
print("="*60)

print("\n--- BEFORE SCD Type I (original customer_master) ---")
spark.sql("SELECT * FROM customer_master ORDER BY customer_id").show(truncate=False)

scd_type1 = spark.sql("""
    SELECT
        cm.customer_id,
        COALESCE(cu.customer_name, cm.customer_name) AS customer_name,
        COALESCE(cu.city,          cm.city)           AS city,
        COALESCE(cu.dob,           cm.dob)            AS dob,
        cm.effective_date,
        cm.expiry_date,
        cm.current_flag
    FROM customer_master cm
    LEFT JOIN customer_updates cu
           ON cm.customer_id = cu.customer_id
    ORDER BY cm.customer_id
""")

print("\n--- AFTER SCD Type I (updated customer_master) ---")
scd_type1.show(truncate=False)

scd_type1.createOrReplaceTempView("customer_master_scd1")

# Show what changed
print("\n--- CHANGED ROWS (SCD Type I) ---")
spark.sql("""
    SELECT
        cm.customer_id,
        cm.customer_name   AS old_name,   cu.customer_name AS new_name,
        cm.city            AS old_city,   cu.city          AS new_city,
        cm.dob             AS old_dob,    cu.dob           AS new_dob
    FROM customer_master cm
    JOIN customer_updates cu ON cm.customer_id = cu.customer_id
    ORDER BY cm.customer_id
""").show(truncate=False)

scd_type1.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{OUTPUT}/scd_type1_customer_master")
print(f"Saved → {OUTPUT}/scd_type1_customer_master")

# ══════════════════════════════════════════════
# 4. SCD TYPE II  –  Add new row, keep history
#    Old row gets expiry_date + current_flag=0
#    New row gets effective_date + current_flag=1
# ══════════════════════════════════════════════

print("\n" + "="*60)
print("SCD TYPE II  (Add row – full history preserved)")
print("="*60)

print("\n--- BEFORE SCD Type II (original customer_master) ---")
spark.sql("SELECT * FROM customer_master ORDER BY customer_id").show(truncate=False)

scd_type2 = spark.sql("""
    -- Rows NOT in updates  →  carry over unchanged
    SELECT
        customer_id, customer_name, city, dob,
        effective_date, expiry_date, current_flag
    FROM customer_master
    WHERE customer_id NOT IN (SELECT customer_id FROM customer_updates)

    UNION ALL

    -- Existing rows that ARE being updated  →  expire them
    SELECT
        cm.customer_id,
        cm.customer_name,
        cm.city,
        cm.dob,
        cm.effective_date,
        cu.change_date  AS expiry_date,   -- record closed on change_date
        0               AS current_flag
    FROM customer_master cm
    JOIN customer_updates cu ON cm.customer_id = cu.customer_id

    UNION ALL

    -- New rows from updates  →  insert as current
    SELECT
        cu.customer_id,
        cu.customer_name,
        cu.city,
        cu.dob,
        cu.change_date  AS effective_date,
        NULL            AS expiry_date,
        1               AS current_flag
    FROM customer_updates cu

    ORDER BY customer_id, current_flag
""")

print("\n--- AFTER SCD Type II (customer_master with full history) ---")
scd_type2.show(20, truncate=False)

scd_type2.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{OUTPUT}/scd_type2_customer_master")
print(f"Saved → {OUTPUT}/scd_type2_customer_master")

# ── Quick count check ──
print("\n--- SCD Type II row counts ---")
spark.sql("""
    SELECT current_flag, COUNT(*) AS row_count
    FROM customer_master
    GROUP BY current_flag
""").show()

scd_type2.createOrReplaceTempView("customer_master_scd2")
spark.sql("""
    SELECT current_flag, COUNT(*) AS row_count
    FROM customer_master_scd2
    GROUP BY current_flag
""").show()

print("\n" + "="*60)
print("ALL TASKS COMPLETED SUCCESSFULLY")
print(f"Outputs written to: {OUTPUT}/")
print("  ├── top5_customers/")
print("  ├── customers_per_city/")
print("  ├── avg_transaction_value/")
print("  ├── scd_type1_customer_master/")
print("  └── scd_type2_customer_master/")
print("="*60 + "\n")

spark.stop()
