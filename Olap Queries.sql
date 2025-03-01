USE metro_dw;


-- Question 01
SELECT 
    dp.product_name,
    DATE_FORMAT(sf.order_date, '%Y-%m') AS month,
    CASE
        WHEN DAYOFWEEK(sf.order_date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    SUM(sf.total_sale) AS total_revenue
FROM sales_facts sf
JOIN dim_products dp ON sf.product_id = dp.product_id
WHERE YEAR(sf.order_date) = 2019
GROUP BY dp.product_name, month, day_type
ORDER BY total_revenue DESC
LIMIT 5;




-- Question 02
SELECT 
    ds.store_name,
    QUARTER(sf.order_date) AS quarter,
    SUM(sf.total_sale) AS total_revenue,
    LAG(SUM(sf.total_sale), 1) OVER (PARTITION BY ds.store_id ORDER BY QUARTER(sf.order_date)) AS previous_revenue,
    ROUND(
        (SUM(sf.total_sale) - LAG(SUM(sf.total_sale), 1) OVER (PARTITION BY ds.store_id ORDER BY QUARTER(sf.order_date))) / 
        LAG(SUM(sf.total_sale), 1) OVER (PARTITION BY ds.store_id ORDER BY QUARTER(sf.order_date)) * 100, 
    2) AS growth_rate
FROM sales_facts sf
JOIN dim_stores ds ON sf.store_id = ds.store_id
WHERE YEAR(sf.order_date) = 2019
GROUP BY ds.store_id, quarter
ORDER BY ds.store_name, quarter;




-- Question 03
SELECT 
    ds.store_name,
    dp.supplier_name,
    dp.product_name,
    SUM(sf.total_sale) AS total_sales
FROM sales_facts sf
JOIN dim_products dp ON sf.product_id = dp.product_id
JOIN dim_stores ds ON sf.store_id = ds.store_id
GROUP BY ds.store_name, dp.supplier_name, dp.product_name
ORDER BY ds.store_name, dp.supplier_name, total_sales DESC;




-- Question 04
SELECT 
    dp.product_name,
    CASE
        WHEN MONTH(sf.order_date) BETWEEN 3 AND 5 THEN 'Spring'
        WHEN MONTH(sf.order_date) BETWEEN 6 AND 8 THEN 'Summer'
        WHEN MONTH(sf.order_date) BETWEEN 9 AND 11 THEN 'Fall'
        ELSE 'Winter'
    END AS season,
    SUM(sf.total_sale) AS total_sales
FROM sales_facts sf
JOIN dim_products dp ON sf.product_id = dp.product_id
GROUP BY dp.product_name, season
ORDER BY dp.product_name, season;




-- Question 05
SELECT 
    ds.store_name,
    dp.supplier_name,
    DATE_FORMAT(sf.order_date, '%Y-%m') AS month,
    SUM(sf.total_sale) AS total_revenue,
    LAG(SUM(sf.total_sale), 1) OVER (PARTITION BY ds.store_id, dp.supplier_name ORDER BY DATE_FORMAT(sf.order_date, '%Y-%m')) AS previous_revenue,
    ROUND(
        (SUM(sf.total_sale) - LAG(SUM(sf.total_sale), 1) OVER (PARTITION BY ds.store_id, dp.supplier_name ORDER BY DATE_FORMAT(sf.order_date, '%Y-%m'))) /
        LAG(SUM(sf.total_sale), 1) OVER (PARTITION BY ds.store_id, dp.supplier_name ORDER BY DATE_FORMAT(sf.order_date, '%Y-%m')) * 100, 
    2) AS revenue_volatility
FROM sales_facts sf
JOIN dim_products dp ON sf.product_id = dp.product_id
JOIN dim_stores ds ON sf.store_id = ds.store_id
GROUP BY ds.store_id, dp.supplier_name, DATE_FORMAT(sf.order_date, '%Y-%m')
ORDER BY ds.store_name, dp.supplier_name, DATE_FORMAT(sf.order_date, '%Y-%m');


-- Question 07
SELECT 
    ds.store_name,
    dp.supplier_name,
    dp.product_name,
    YEAR(sf.order_date) AS year,
    SUM(sf.total_sale) AS revenue
FROM sales_facts sf
JOIN dim_products dp ON sf.product_id = dp.product_id
JOIN dim_stores ds ON sf.store_id = ds.store_id
GROUP BY ROLLUP(ds.store_name, dp.supplier_name, dp.product_name, year)
ORDER BY ds.store_name, dp.supplier_name, dp.product_name, year;




-- Question 08
SELECT 
    dp.product_name,
    SUM(CASE WHEN MONTH(t.order_date) <= 6 THEN sf.total_sale ELSE 0 END) AS H1_revenue,
    SUM(CASE WHEN MONTH(t.order_date) <= 6 THEN t.quantity ELSE 0 END) AS H1_quantity,
    SUM(CASE WHEN MONTH(t.order_date) > 6 THEN sf.total_sale ELSE 0 END) AS H2_revenue,
    SUM(CASE WHEN MONTH(t.order_date) > 6 THEN t.quantity ELSE 0 END) AS H2_quantity,
    SUM(sf.total_sale) AS total_revenue,
    SUM(t.quantity) AS total_quantity
FROM sales_facts sf
JOIN dim_products dp ON sf.product_id = dp.product_id
JOIN transactions t ON sf.order_id = t.order_id
GROUP BY dp.product_name
ORDER BY dp.product_name;




-- Question 09
WITH DailySales AS (
    SELECT 
        sf.product_id,
        DATE(sf.order_date) AS sale_date,
        SUM(sf.total_sale) AS daily_sales
    FROM sales_facts sf
    GROUP BY sf.product_id, sale_date
), DailyAverage AS (
    SELECT 
        ds.product_id,
        AVG(ds.daily_sales) AS avg_sales
    FROM DailySales ds
    GROUP BY ds.product_id
)
SELECT 
    ds.product_id,
    dp.product_name,
    ds.sale_date,
    ds.daily_sales,
    da.avg_sales,
    CASE
        WHEN ds.daily_sales > 2 * da.avg_sales THEN 'Spike'
        ELSE 'Normal'
    END AS anomaly
FROM DailySales ds
JOIN DailyAverage da ON ds.product_id = da.product_id
JOIN dim_products dp ON ds.product_id = dp.product_id
WHERE ds.daily_sales > 2 * da.avg_sales
ORDER BY ds.product_id, ds.sale_date;



-- Question 10
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    ds.store_name,
    QUARTER(sf.order_date) AS quarter,
    SUM(sf.total_sale) AS total_sales
FROM sales_facts sf
JOIN dim_stores ds ON sf.store_id = ds.store_id
GROUP BY ds.store_name, QUARTER(sf.order_date)
ORDER BY ds.store_name, quarter;






