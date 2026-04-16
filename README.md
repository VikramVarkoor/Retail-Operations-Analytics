# Retail Operations Analytics Dashboard

End-to-end retail analytics project: Python data pipeline, SQL analysis, and Tableau visualization built on the Sample Superstore dataset (~10K rows of US retail orders).

## Tech Stack

- **Python 3** (pandas, sqlite3) — data generation, cleaning, ETL
- **SQLite** — relational database for SQL queries
- **SQL** — 4 interview-ready analytical queries (GROUP BY, window functions, CTEs, HAVING)
- **Tableau Public** — interactive dashboard (map, time series, category breakdown)

## Project Structure

```
SQL_Proj_Amazon/
├── 01_load_data.py              # Generate/clean data, load into SQLite
├── 02_sql_queries.py            # Run all 4 SQL queries, export CSVs
├── superstore.db                # SQLite database
├── data/
│   ├── superstore_raw.csv       # Raw data (with nulls, original formats)
│   └── superstore_clean.csv     # Cleaned data (nulls filled, dates parsed)
├── sql/
│   ├── 01_revenue_by_region.sql
│   ├── 02_mom_sales_growth.sql
│   ├── 03_top_products_margin.sql
│   └── 04_unprofitable_customers.sql
├── query_results/
│   ├── q1_revenue_by_region.csv
│   ├── q2_mom_sales_growth.csv
│   ├── q3_top_products_margin.csv
│   └── q4_unprofitable_customers.csv
└── README.md
```

## How to Run

```bash
# 1. Generate data, clean it, load into SQLite
python 01_load_data.py

# 2. Run SQL queries and export results to CSV
python 02_sql_queries.py
```

Requirements: Python 3.8+, pandas (`pip install pandas`)

## Dataset

~9,994 rows of simulated US retail orders (2020–2023) modeled after the Kaggle "Sample Superstore" dataset. Columns include Order ID, Order/Ship Date, Ship Mode, Customer info, Region, Product Category/Sub-Category, Sales, Quantity, Discount, and Profit.

## SQL Queries — What They Do and Why

### Query 1: Revenue & Profit by Region
**SQL concepts:** `GROUP BY`, `SUM`, `COUNT DISTINCT`, `ORDER BY`

Segments total revenue, profit, and profit margin by US region. The West generates the highest revenue but the East has the best profit margin — useful for resource allocation decisions.

### Query 2: Month-over-Month Sales Growth
**SQL concepts:** `CTE`, window function (`LAG`), date functions

Uses `LAG()` to compare each month's sales against the previous month, calculating percentage growth. This reveals seasonality patterns (Q4 spikes, mid-year dips) and overall trend direction.

### Query 3: Top 10 Products by Profit Margin
**SQL concepts:** `CTE`, `HAVING` (volume filter), calculated columns

Aggregates product-level metrics inside a CTE, filters for products with >$500 in total sales to avoid low-volume noise, then ranks by profit margin. Office Supplies items dominate the top 10.

### Query 4: Unprofitable Repeat Customers
**SQL concepts:** Multi-aggregation, `HAVING` with compound conditions

Identifies customers who placed >3 orders but have negative total profit. These are high-activity, money-losing accounts — typically driven by heavy discounting. Actionable insight for discount policy review.

## Tableau Dashboard (Build It Yourself)

Connect Tableau Public (free) to the CSVs in `query_results/` and build 3 views:

1. **Map view** — Region revenue/profit on a US map (use `q1_revenue_by_region.csv`)
2. **Time series** — Monthly sales trend line with MoM growth (use `q2_mom_sales_growth.csv`)
3. **Bar chart** — Product category profit comparison (use `q3_top_products_margin.csv`)

Add interactive filters for Region and Category. Publish to your Tableau Public profile.

**Resources:**
- [Tableau Public](https://public.tableau.com) (free desktop app)
- [SQLite Browser](https://sqlitebrowser.org) (GUI for running queries)
- YouTube: "Tableau for beginners" by Alex The Analyst

## Interview Talking Points

- **Data Pipeline:** "I built an end-to-end pipeline: Python for ETL, SQLite for storage, SQL for analysis, Tableau for visualization."
- **Window Functions:** "I used `LAG()` to calculate month-over-month growth — this is the standard approach for time-series comparisons in SQL."
- **CTEs:** "I used CTEs to break complex queries into readable steps. The product margin query first aggregates, then filters, then ranks — each step is clear."
- **Business Insight:** "The unprofitable customers query found 85 repeat buyers with negative total profit. Most had above-average discount rates, suggesting the discount policy needs review for high-frequency accounts."
- **Data Cleaning:** "The raw data had 20 null values across postal codes and cities. I filled postal codes with a sentinel value and cities with 'Unknown' — then validated date consistency and removed duplicates."
