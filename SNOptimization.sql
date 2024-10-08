SELECT 
    customer_id,
    SUM(amount) AS total_spent
FROM 
    TRANSACTION_DBT
WHERE 
    transaction_type = 'purchase' 
    AND location_country = 'United States'
    AND YEAR(transaction_date) = 2023
GROUP BY 
    customer_id;
 
ALTER TABLE TRANSACTION_DBT
CLUSTER BY (transaction_date);
 
SELECT 
    customer_id,
    SUM(amount) AS total_spent
FROM 
    TRANSACTION_DBT
WHERE 
    transaction_type = 'purchase' 
    AND location_country = 'United States'
    AND YEAR(transaction_date) = 2023
GROUP BY 
    customer_id;
 
--Materialized view:
 
--Materialized views require Enterprise Edition. 
CREATE MATERIALIZED VIEW MV_PURCHASES_USA_2023 AS
SELECT 
    customer_id,
    SUM(amount) AS total_spent
FROM 
    TRANSACTIONS
WHERE 
    transaction_type = 'purchase' 
    AND location_country = 'United States'
    AND transaction_date >= '2023-01-01' 
    AND transaction_date < '2024-01-01'
GROUP BY 
    customer_id;