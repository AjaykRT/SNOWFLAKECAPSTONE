CREATE OR REPLACE TABLE watchlist (
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    listed_date DATE,
    source STRING
);
 
--create udf for fraud detection
CREATE OR REPLACE FUNCTION fraud_detection(
    transaction_country STRING,
    transaction_type STRING,
    amount FLOAT,
    transfer FLOAT,
    withdrawal_count INT,
    merchant_category STRING,
    customer_id STRING,
    first_name STRING,
    last_name STRING
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    CASE
        -- Check if transaction_country is not in the list
        WHEN transaction_country NOT IN ('United States', 'India', 'United Kingdom', 'Japan', 'Germany') THEN TRUE
        -- Check if transaction_type amount is greater than 50000 for purchases
        WHEN transaction_type = 'purchase' AND amount > 50000 THEN TRUE
        -- Check if transfer amount is greater than 200000
        WHEN transaction_type = 'transfer' AND transfer > 200000 THEN TRUE
        -- Check if there are more than 5 continuous withdrawals
        WHEN transaction_type = 'withdrawal' AND withdrawal_count > 5 THEN TRUE
        ELSE FALSE
    END
$$;
 
--insert values into watchlist using function
 
INSERT INTO PC_DBT_DB.DBT_SPAGIDIPALLI.WATCHLIST (entity_id, entity_name, entity_type, listed_date, source)
SELECT 
    t.CUSTOMER_ID AS entity_id,
    CONCAT(c.FIRST_NAME, ' ', c.LAST_NAME) AS entity_name,
    t.TRANSACTION_TYPE AS entity_type,
    CURRENT_DATE() AS listed_date,
    t.MERCHANT_CATEGORY AS source
FROM 
    PC_DBT_DB.DBT_SPAGIDIPALLI.TRANSACTION_DBT t
JOIN 
    PC_DBT_DB.DBT_SPAGIDIPALLI.TRANSFORMED_CUSTOMER c 
ON 
    t.CUSTOMER_ID = c.CUSTOMER_ID
WHERE 
    PC_DBT_DB.DBT_SPAGIDIPALLI.FRAUD_DETECTION(t.LOCATION_COUNTRY, t.TRANSACTION_TYPE, t.AMOUNT, 0, 
                                               (SELECT COUNT(*) 
                                                FROM PC_DBT_DB.DBT_SPAGIDIPALLI.TRANSACTION_DBT t2 
                                                WHERE t2.CUSTOMER_ID = t.CUSTOMER_ID 
                                                AND t2.TRANSACTION_TYPE = 'withdrawal' 
                                                AND t2.TRANSACTION_DATE > DATEADD(minute, -10, t.TRANSACTION_DATE)), 
                                               t.MERCHANT_CATEGORY, t.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME) = TRUE;
 
select * from watchlist;