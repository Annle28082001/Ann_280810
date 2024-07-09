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

WITH online_retail_index as 
(SELECT 
id,
revenue,
FORMAT_DATE('%Y-%m', created_at) as Cohort_date,
(FORMAT_DATE('%Y', created_at) - FORMAT_DATE('%Y', first_purchase_date))*12 + 
(FORMAT_DATE('%M', created_at) - FORMAT_DATE('%M', first_purchase_date)) + 1 as index 
FROM
    (SELECT 
    a.id,
    SUM(sale_price) OVER (PARTITION BY product_id) AS revenue,
    MIN(created_at) OVER (PARTITION BY product_id) AS first_purchase_date,
    b.created_at
    FROM bigquery-public-data.thelook_ecommerce.products AS a
    JOIN bigquery-public-data.thelook_ecommerce.order_items AS b
    ON a.id = b.product_id)
)
, XXX AS (SELECT 
Cohort_date,
index,
COUNT(DISTINCT id) as cnt,
SUM(revenue) as total_revenue
FROM online_retail_index
GROUP BY cohort_date, index)

, customer_cohort as 
(SELECT 
cohort_date, 
sum(CASE WHEN index=1 then cnt else 0 end) as m1, 
sum(CASE WHEN index=2 then cnt else 0 end) as m2,
sum(CASE WHEN index=3 then cnt else 0 end) as m3,
sum(CASE WHEN index=4 then cnt else 0 end) as m4,
sum(CASE WHEN index=5 then cnt else 0 end) as m5,
sum(CASE WHEN index=6 then cnt else 0 end) as m6,
sum(CASE WHEN index=7 then cnt else 0 end) as m7,
sum(CASE WHEN index=8 then cnt else 0 end) as m8,
sum(CASE WHEN index=9 then cnt else 0 end) as m9,
sum(CASE WHEN index=10 then cnt else 0 end) as m10,
sum(CASE WHEN index=11 then cnt else 0 end) as m11,
sum(CASE WHEN index=12 then cnt else 0 end) as m12,
sum(CASE WHEN index=13 then cnt else 0 end) as m13
FROM xxx
group by cohort_date
order by cohort_date)

--retention cohort 
SELECT 
cohort_date,
(100- round(100.00* m1/m1,2)) || '%' as m1,
(100- round(100.00* m2/m1,2)) || '%' as m2,
(100 -round(100.00* m2/m1,3)) || '%' as m3,
(100- round(100.00* m2/m1,4)) || '%' as m4,
(100- round(100.00* m2/m1,5)) || '%' as m5,
(100- round(100.00* m2/m1,6)) || '%' as m6,
(100- round(100.00* m2/m1,7)) || '%' as m7,
(100- round(100.00* m2/m1,8)) || '%' as m8,
(100- round(100.00* m2/m1,9)) || '%' as m9,
(100- round(100.00* m2/m1,10)) || '%' as m10,
(100- round(100.00* m2/m1,11)) || '%' as m11,
(100- round(100.00* m2/m1,12)) || '%' as m12,
(100- round(100.00* m2/m1,13)) || '%' as m13
from customer_cohort

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
