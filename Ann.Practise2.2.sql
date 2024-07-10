--part 1: Xuất file có các trường
With category_data as
(
Select 
FORMAT_DATE('%Y-%m', t1.created_at) as Month,
FORMAT_DATE('%Y', t1.created_at) as Year,
t2.category as Product_category,
round(sum(t3.sale_price),2) as TPV,
count(t3.order_id) as TPO,
round(sum(t2.cost),2) as Total_cost
from bigquery-public-data.thelook_ecommerce.orders as t1 
Join bigquery-public-data.thelook_ecommerce.products as t2 on t1.order_id=t2.id 
Join bigquery-public-data.thelook_ecommerce.order_items as t3 on t2.id=t3.id
Group by Month, Year, Product_category
)
Select Month, Year, Product_category, TPV, TPO,
round(cast((TPV - lag(TPV) OVER(PARTITION BY Product_category ORDER BY Year, Month))
      /lag(TPV) OVER(PARTITION BY Product_category ORDER BY Year, Month) as Decimal)*100.00,2) || '%'
       as Revenue_growth,
round(cast((TPO - lag(TPO) OVER(PARTITION BY Product_category ORDER BY Year, Month))
      /lag(TPO) OVER(PARTITION BY Product_category ORDER BY Year, Month) as Decimal)*100.00,2) || '%'
       as Order_growth,
Total_cost,
round(TPV - Total_cost,2) as Total_profit,
round((TPV - Total_cost)/Total_cost,2) as Profit_to_cost_ratio
from category_data
Order by Product_category, Year, Month

--PART 2: COHORT RETENTION CHART

WITH online_retail_index AS (
  SELECT 
    id,
    revenue,
    FORMAT_DATE('%Y-%m', created_at) AS cohort_date,
    (EXTRACT(YEAR FROM created_at) - EXTRACT(YEAR FROM first_purchase_date)) * 12 + 
    (EXTRACT(MONTH FROM created_at) - EXTRACT(MONTH FROM first_purchase_date)) + 1 AS index 
  FROM (
    SELECT 
      a.id,
      SUM(b.sale_price) OVER (PARTITION BY b.product_id) AS revenue,
      MIN(b.created_at) OVER (PARTITION BY b.product_id) AS first_purchase_date,
      b.created_at
    FROM bigquery-public-data.thelook_ecommerce.products AS a
    JOIN bigquery-public-data.thelook_ecommerce.order_items AS b
    ON a.id = b.product_id
  )
),
aggregated_data AS (
  SELECT 
    cohort_date,
    index,
    COUNT(DISTINCT id) AS cnt,
    SUM(revenue) AS total_revenue
  FROM online_retail_index
  GROUP BY cohort_date, index
),
customer_cohort AS (
  SELECT 
    cohort_date, 
    SUM(CASE WHEN index = 1 THEN cnt ELSE 0 END) AS m1, 
    SUM(CASE WHEN index = 2 THEN cnt ELSE 0 END) AS m2,
    SUM(CASE WHEN index = 3 THEN cnt ELSE 0 END) AS m3,
    SUM(CASE WHEN index = 4 THEN cnt ELSE 0 END) AS m4,
    SUM(CASE WHEN index = 5 THEN cnt ELSE 0 END) AS m5,
    SUM(CASE WHEN index = 6 THEN cnt ELSE 0 END) AS m6,
    SUM(CASE WHEN index = 7 THEN cnt ELSE 0 END) AS m7,
    SUM(CASE WHEN index = 8 THEN cnt ELSE 0 END) AS m8,
    SUM(CASE WHEN index = 9 THEN cnt ELSE 0 END) AS m9,
    SUM(CASE WHEN index = 10 THEN cnt ELSE 0 END) AS m10,
    SUM(CASE WHEN index = 11 THEN cnt ELSE 0 END) AS m11,
    SUM(CASE WHEN index = 12 THEN cnt ELSE 0 END) AS m12,
    SUM(CASE WHEN index = 13 THEN cnt ELSE 0 END) AS m13
  FROM aggregated_data
  GROUP BY cohort_date
  ORDER BY cohort_date
)

-- Retention Cohort 
SELECT 
  cohort_date,
  (100 - ROUND(100.00 * m1 / m1, 2)) || '%' AS m1,
  (100 - ROUND(100.00 * m2 / m1, 2)) || '%' AS m2,
  (100 - ROUND(100.00 * m3 / m1, 2)) || '%' AS m3,
  (100 - ROUND(100.00 * m4 / m1, 2)) || '%' AS m4,
  (100 - ROUND(100.00 * m5 / m1, 2)) || '%' AS m5,
  (100 - ROUND(100.00 * m6 / m1, 2)) || '%' AS m6,
  (100 - ROUND(100.00 * m7 / m1, 2)) || '%' AS m7,
  (100 - ROUND(100.00 * m8 / m1, 2)) || '%' AS m8,
  (100 - ROUND(100.00 * m9 / m1, 2)) || '%' AS m9,
  (100 - ROUND(100.00 * m10 / m1, 2)) || '%' AS m10,
  (100 - ROUND(100.00 * m11 / m1, 2)) || '%' AS m11,
  (100 - ROUND(100.00 * m12 / m1, 2)) || '%' AS m12,
  (100 - ROUND(100.00 * m13 / m1, 2)) || '%' AS m13
FROM customer_cohort;


Bị lỗi: 
/* No matching signature for operator - for argument types: STRING, STRING. Supported signatures: INT64 - INT64; NUMERIC - NUMERIC; BIGNUMERIC - BIGNUMERIC; 
FLOAT64 - FLOAT64; DATE - INT64; DATE - DATE; TIMESTAMP - TIMESTAMP; DATETIME - DATETIME; TIME - TIME; TIMESTAMP - INTERVAL; DATE - INTERVAL; DATETIME - INTERVAL;
INTERVAL - INTERVAL at [7:2]
*/

--BAI EM TỰ CODE CŨ: bị lỗi ở chỗ vẫn bị trùng lặp dữ liệu dù em đã thêm DISTINCT ở MONTH

WITH CTE1 AS (
    SELECT 
        a.order_id,
        a.product_id,
        b.category,
        b.retail_price,
        b.cost,
        c.created_at, 
        (b.retail_price - b.cost) AS profit
    FROM 
        bigquery-public-data.thelook_ecommerce.order_items AS a
    JOIN 
        bigquery-public-data.thelook_ecommerce.products AS b 
    ON 
        a.product_id = b.id
    JOIN 
        bigquery-public-data.thelook_ecommerce.orders AS c
    ON 
        a.order_id = c.order_id
),

CTE2 AS (SELECT 
    FORMAT_DATE('%y-%m', created_at) AS Month, 
    EXTRACT(YEAR FROM created_at) AS Year, 
    category, 
    SUM(retail_price) OVER (PARTITION BY FORMAT_DATE('%y-%m', created_at)) AS TPV,
    COUNT(order_id) OVER (PARTITION BY FORMAT_DATE('%y-%m', created_at)) AS TPO, 
    SUM(profit) OVER (PARTITION BY FORMAT_DATE('%y-%m', created_at)) AS total_profit,
    SUM(cost) OVER (PARTITION BY FORMAT_DATE('%y-%m', created_at)) AS total_cost, 
    (SUM(profit) OVER (PARTITION BY FORMAT_DATE('%y-%m', created_at)))/(SUM(cost) OVER (PARTITION BY FORMAT_DATE('%y-%m', created_at))) as profit_to_cost_radio

FROM 
    CTE1
ORDER BY 
    Month, category)

SELECT 
DISTINCT Month, 
Year, 
category, 
TPV, 
TPO, 
round(cast((TPV - lag(TPV) OVER(PARTITION BY category ORDER BY Year, Month))
      /lag(TPV) OVER(PARTITION BY category ORDER BY Year, Month) as Decimal)*100.00,2) || '%'
       as Revenue_growth,
round(cast((TPO - lag(TPO) OVER(PARTITION BY category ORDER BY Year, Month))
      /lag(TPO) OVER(PARTITION BY category ORDER BY Year, Month) as Decimal)*100.00,2) || '%'
       as Order_growth
FROM CTE2
Order by category, Year, Month
