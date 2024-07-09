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

--BAI EM TỰ CODE CŨ

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
