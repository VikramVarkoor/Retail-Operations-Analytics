-- ============================================================
-- Query 4: Repeat Customers with Negative Total Profit
-- Concepts: Multi-aggregation, HAVING with multiple conditions
-- ============================================================
-- Interview talking point:
--   "This identifies the most problematic repeat customers — people who
--    order frequently but destroy margin, usually due to heavy discounting.
--    These are candidates for discount policy review or outreach."

SELECT
    Customer_ID,
    Customer_Name,
    Segment,
    COUNT(DISTINCT Order_ID)                   AS Order_Count,
    ROUND(SUM(Sales), 2)                      AS Total_Sales,
    ROUND(SUM(Profit), 2)                    AS Total_Profit,
    ROUND(AVG(Discount), 2)                   AS Avg_Discount,
    ROUND(SUM(Profit) / SUM(Sales) * 100, 2) AS Profit_Margin_Pct
FROM orders
GROUP BY Customer_ID, Customer_Name, Segment
HAVING
    COUNT(DISTINCT Order_ID) > 3
    AND SUM(Profit) < 0
ORDER BY Total_Profit ASC;
