-- ============================================================
-- Query 2: Month-over-Month Sales Growth
-- Concepts: CTE, Window Function (LAG), STRFTIME, percentage calc
-- ============================================================
-- Interview talking point:
--   "I used LAG() to compare each month's sales to the previous month,
--    calculating growth rate. This is a common time-series pattern in SQL
--    that shows seasonality and trend direction at a glance."

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
