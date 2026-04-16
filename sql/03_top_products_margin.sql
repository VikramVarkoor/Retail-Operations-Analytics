-- ============================================================
-- Query 3: Top 10 Products by Profit Margin
-- Concepts: CTE, HAVING (minimum volume filter), calculated fields
-- ============================================================
-- Interview talking point:
--   "I used a CTE to first aggregate product-level metrics, then filtered
--    for products with meaningful sales volume (>$500) before ranking by
--    margin. This avoids misleading results from one-off low-volume items."

WITH product_metrics AS (
    SELECT
        Product_Name,
        Category,
        Sub_Category,
        COUNT(*)                                   AS Times_Sold,
        ROUND(SUM(Sales), 2)                      AS Total_Sales,
        ROUND(SUM(Profit), 2)                    AS Total_Profit,
        ROUND(SUM(Profit) / SUM(Sales) * 100, 2) AS Profit_Margin_Pct
    FROM orders
    GROUP BY Product_Name, Category, Sub_Category
    HAVING SUM(Sales) > 500   -- filter out low-volume noise
)
SELECT *
FROM product_metrics
ORDER BY Profit_Margin_Pct DESC
LIMIT 10;
