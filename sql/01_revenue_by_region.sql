-- ============================================================
-- Query 1: Total Revenue and Profit by Region
-- Concepts: GROUP BY, SUM, ROUND, COUNT DISTINCT, ORDER BY
-- ============================================================
-- Interview talking point:
--   "I used GROUP BY to segment performance by region. This revealed
--    that the West drives the most revenue but East actually has
--    better profit margins — useful for strategic resource allocation."

SELECT
    Region,
    COUNT(DISTINCT Order_ID)                   AS Total_Orders,
    COUNT(*)                                   AS Total_Line_Items,
    ROUND(SUM(Sales), 2)                      AS Total_Revenue,
    ROUND(SUM(Profit), 2)                    AS Total_Profit,
    ROUND(SUM(Profit) / SUM(Sales) * 100, 2) AS Profit_Margin_Pct
FROM orders
GROUP BY Region
ORDER BY Total_Revenue DESC;
