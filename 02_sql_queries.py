"""
Step 2: SQL Queries for Retail Analytics
=========================================
4 interview-ready SQL queries run against the SQLite database.
Each query demonstrates a specific SQL concept you can discuss.

Queries:
  1. Total Revenue & Profit by Region  (GROUP BY, aggregation)
  2. Month-over-Month Sales Growth     (Window function: LAG)
  3. Top 10 Products by Profit Margin  (CTE / subquery)
  4. Customers with >3 Orders but      (Multi-join + HAVING)
     Negative Total Profit
"""

import sqlite3
import pandas as pd
import os

# ─── CONFIG ──────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORK_DIR = os.environ.get("WORK_DIR", SCRIPT_DIR)
DB_PATH = os.path.join(WORK_DIR, "superstore.db")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "query_results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

conn = sqlite3.connect(DB_PATH)

# ═══════════════════════════════════════════════════════════════════════════════
# QUERY 1: Total Revenue and Profit by Region
# Concept: GROUP BY, SUM, ROUND, ORDER BY
# Interview talking point: "I used GROUP BY to segment performance by region,
#   which revealed that the West drives the most revenue but East has
#   better profit margins."
# ═══════════════════════════════════════════════════════════════════════════════

QUERY_1 = """
SELECT
    Region,
    COUNT(DISTINCT Order_ID)            AS Total_Orders,
    COUNT(*)                            AS Total_Line_Items,
    ROUND(SUM(Sales), 2)               AS Total_Revenue,
    ROUND(SUM(Profit), 2)             AS Total_Profit,
    ROUND(SUM(Profit) / SUM(Sales) * 100, 2) AS Profit_Margin_Pct
FROM orders
GROUP BY Region
ORDER BY Total_Revenue DESC;
"""

print("=" * 70)
print("QUERY 1: Total Revenue and Profit by Region")
print("=" * 70)
print(QUERY_1)
df1 = pd.read_sql_query(QUERY_1, conn)
print(df1.to_string(index=False))
df1.to_csv(os.path.join(OUTPUT_DIR, "q1_revenue_by_region.csv"), index=False)
print(f"\n-> Saved to query_results/q1_revenue_by_region.csv\n")


# ═══════════════════════════════════════════════════════════════════════════════
# QUERY 2: Month-over-Month Sales Growth
# Concept: Window function (LAG), DATE functions, percentage calculation
# Interview talking point: "I used LAG() to compare each month's sales to the
#   previous month, calculating growth rate — this is a common pattern for
#   time-series analysis in SQL."
# ═══════════════════════════════════════════════════════════════════════════════

QUERY_2 = """
WITH monthly_sales AS (
    SELECT
        STRFTIME('%Y-%m', Order_Date)  AS Month,
        ROUND(SUM(Sales), 2)          AS Monthly_Revenue,
        ROUND(SUM(Profit), 2)         AS Monthly_Profit,
        COUNT(DISTINCT Order_ID)       AS Order_Count
    FROM orders
    GROUP BY STRFTIME('%Y-%m', Order_Date)
    ORDER BY Month
)
SELECT
    Month,
    Monthly_Revenue,
    Monthly_Profit,
    Order_Count,
    LAG(Monthly_Revenue) OVER (ORDER BY Month)  AS Prev_Month_Revenue,
    ROUND(
        (Monthly_Revenue - LAG(Monthly_Revenue) OVER (ORDER BY Month))
        / LAG(Monthly_Revenue) OVER (ORDER BY Month) * 100,
        2
    ) AS MoM_Growth_Pct
FROM monthly_sales;
"""

print("=" * 70)
print("QUERY 2: Month-over-Month Sales Growth (Window Function: LAG)")
print("=" * 70)
print(QUERY_2)
df2 = pd.read_sql_query(QUERY_2, conn)
print(df2.to_string(index=False))
df2.to_csv(os.path.join(OUTPUT_DIR, "q2_mom_sales_growth.csv"), index=False)
print(f"\n-> Saved to query_results/q2_mom_sales_growth.csv\n")


# ═══════════════════════════════════════════════════════════════════════════════
# QUERY 3: Top 10 Products by Profit Margin
# Concept: CTE, HAVING (minimum sales threshold), calculated fields
# Interview talking point: "I used a CTE to first aggregate product-level
#   metrics, then filtered for products with meaningful sales volume before
#   ranking by margin — this avoids misleading results from low-volume items."
# ═══════════════════════════════════════════════════════════════════════════════

QUERY_3 = """
WITH product_metrics AS (
    SELECT
        Product_Name,
        Category,
        Sub_Category,
        COUNT(*)                               AS Times_Sold,
        ROUND(SUM(Sales), 2)                  AS Total_Sales,
        ROUND(SUM(Profit), 2)                AS Total_Profit,
        ROUND(SUM(Profit) / SUM(Sales) * 100, 2) AS Profit_Margin_Pct
    FROM orders
    GROUP BY Product_Name, Category, Sub_Category
    HAVING SUM(Sales) > 500   -- filter out low-volume noise
)
SELECT *
FROM product_metrics
ORDER BY Profit_Margin_Pct DESC
LIMIT 10;
"""

print("=" * 70)
print("QUERY 3: Top 10 Products by Profit Margin (CTE)")
print("=" * 70)
print(QUERY_3)
df3 = pd.read_sql_query(QUERY_3, conn)
print(df3.to_string(index=False))
df3.to_csv(os.path.join(OUTPUT_DIR, "q3_top_products_margin.csv"), index=False)
print(f"\n-> Saved to query_results/q3_top_products_margin.csv\n")


# ═══════════════════════════════════════════════════════════════════════════════
# QUERY 4: Customers with >3 Orders but Negative Total Profit
# Concept: Multi-aggregation + HAVING with multiple conditions
# Interview talking point: "This query identifies your most problematic
#   repeat customers — people who order frequently but destroy margin.
#   These are candidates for discount policy review or outreach."
# ═══════════════════════════════════════════════════════════════════════════════

QUERY_4 = """
SELECT
    Customer_ID,
    Customer_Name,
    Segment,
    COUNT(DISTINCT Order_ID)   AS Order_Count,
    ROUND(SUM(Sales), 2)      AS Total_Sales,
    ROUND(SUM(Profit), 2)    AS Total_Profit,
    ROUND(AVG(Discount), 2)   AS Avg_Discount,
    ROUND(SUM(Profit) / SUM(Sales) * 100, 2) AS Profit_Margin_Pct
FROM orders
GROUP BY Customer_ID, Customer_Name, Segment
HAVING
    COUNT(DISTINCT Order_ID) > 3
    AND SUM(Profit) < 0
ORDER BY Total_Profit ASC;
"""

print("=" * 70)
print("QUERY 4: Repeat Customers with Negative Profit (HAVING)")
print("=" * 70)
print(QUERY_4)
df4 = pd.read_sql_query(QUERY_4, conn)
print(df4.to_string(index=False))
df4.to_csv(os.path.join(OUTPUT_DIR, "q4_unprofitable_customers.csv"), index=False)
print(f"\n-> Saved to query_results/q4_unprofitable_customers.csv\n")


# ─── SUMMARY ─────────────────────────────────────────────────────────────────
print("=" * 70)
print("ALL QUERIES COMPLETE")
print("=" * 70)
print(f"Results exported to: {OUTPUT_DIR}/")
for f in sorted(os.listdir(OUTPUT_DIR)):
    rows = len(pd.read_csv(os.path.join(OUTPUT_DIR, f)))
    print(f"  {f:40s} ({rows} rows)")

conn.close()
