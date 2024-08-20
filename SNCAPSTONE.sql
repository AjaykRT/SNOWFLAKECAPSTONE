------ 1. creating the warehouse --------
Create Warehouse AJCAPSTONE_WH with
Warehouse_size = 'xsmall'
Auto_suspend = 300
Auto_resume = true
Initially_suspended = true;

USE WAREHOUSE AJCAPSTONE_WH;

------ 2. creating the databases --------------
create or replace database RAW_DB;
create or replace database CLEANSE_DB;

show databases;

use database RAW_DB;

------ 3. creating the schema -----------------
create schema RAW_DB.RAW_SCHEMA;

------ 4. create external storage integration --------
CREATE OR REPLACE STORAGE INTEGRATION capstone_csv
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::590183994754:role/capstonerole'
STORAGE_ALLOWED_LOCATIONS =('s3://capstoneaj/csv/');

desc integration capstone_csv;
 
------ 5. Create external stage ------------------
CREATE OR REPLACE STAGE my_capstone
STORAGE_INTEGRATION = capstone_csv
URL = 's3://capstoneaj/csv/';

------ 6. creating the tables ---------------------
CREATE TABLE transactions (
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount FLOAT,
    currency STRING,
    transaction_type STRING,
    channel STRING,
    merchant_name STRING,
    merchant_category STRING,
    location_country STRING,
    location_city STRING,
    is_flagged BOOLEAN
);


CREATE TABLE customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    city STRING,
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
);


CREATE TABLE accounts (
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
);

CREATE TABLE credit_data (
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);

CREATE TABLE watchlist (
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    risk_category STRING,
    listed_date DATE,
    source STRING
);

----- 7. Create file formats ----------
CREATE OR REPLACE FILE FORMAT csv_raw_data
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

----- 8. creating the pipes ------------
CREATE OR REPLACE PIPE transactions_pipe
auto_ingest = true AS
COPY INTO transactions
FROM @my_capstone
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.RAW_SCHEMA.csv_raw_data')
ON_ERROR = CONTINUE;

show pipes;
desc pipe transactions_pipe;

CREATE OR REPLACE PIPE customers_pipe
auto_ingest = true AS
COPY INTO customers
FROM @my_capstone
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.RAW_SCHEMA.csv_raw_data')
ON_ERROR = CONTINUE;

CREATE OR REPLACE PIPE accounts_pipe
auto_ingest = true AS
COPY INTO accounts
FROM @my_capstone
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.RAW_SCHEMA.csv_raw_data')
ON_ERROR = CONTINUE;

CREATE OR REPLACE PIPE credit_data_pipe
auto_ingest = true AS
COPY INTO credit_data
FROM @my_capstone
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.RAW_SCHEMA.csv_raw_data')
ON_ERROR = CONTINUE;

CREATE OR REPLACE PIPE watchlist_pipe
auto_ingest = true AS
COPY INTO watchlist
FROM @my_capstone
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.RAW_SCHEMA.csv_raw_data')
ON_ERROR = CONTINUE;

-------9. to refresh pipes ---------
alter pipe transactions_pipe refresh;
alter pipe customers_pipe refresh;
alter pipe accounts_pipe refresh;
alter pipe credit_data_pipe refresh;
alter pipe watchlist_pipe refresh;
 
 
select * from transactions;
select * from customers;
select * from accounts;
select * from credit_data;
select * from watchlist;

---------------creating UDFs--------------------------
create or replace function trans_func_Test(amount number)
returns string
language sql 
as
$$
select case when amount > 500 then 'High'
        when amount between 100 and 200 then 'Medium'
        else 'Low' end
$$
;
 
grant USAGE on FUNCTION trans_func_test(NUMBER) to role PC_DBT_ROLE;

select * from transactions ;
select *, trans_func_test(amount) as risk_level from transactions;
 
 
REVOKE APPLYBUDGET ON DATABASE raw_db FROM ROLE PC_DBT_ROLE;
grant all privileges on DATABASE raw_db to role PC_DBT_ROLE;
grant all privileges on schema RAW_schema to role PC_DBT_ROLE;
grant select on all tables in schema RAW_schema to role PC_DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE raw_db TO ROLE PC_DBT_ROLE;


---------------Masking----------------
 
CREATE MASKING POLICY GLOBALBANK.RAW_DATA.EMAIL_MASK AS
(EMAIL VARCHAR) RETURNS VARCHAR ->
CASE WHEN CURRENT_ROLE = 'ADMIN' THEN EMAIL
ELSE REGEXP_REPLACE(EMAIL, '.+\@', '*****@')
END;

ALTER TABLE GLOBALBANK.RAW_DATA.CUSTOMER_RAW MODIFY COLUMN email SET MASKING POLICY GLOBALBANK.RAW_DATA.EMAIL_MASK;

CREATE MASKING POLICY GLOBALBANK.RAW_DATA.Phone_MASK AS
(PHONE VARCHAR) RETURNS VARCHAR ->
CASE WHEN CURRENT_ROLE = 'ADMIN' THEN PHONE
ELSE SUBSTR(PHONE, 0, 5) || '***-****'
END;
ALTER TABLE GLOBALBANK.RAW_DATA.CUSTOMER_RAW MODIFY COLUMN phone_number SET MASKING POLICY GLOBALBANK.RAW_DATA.Phone_MASK;
CREATE OR REPLACE MASKING POLICY GLOBALBANK.RAW_DATA.customer_id_MASK AS
(Cust_id VARCHAR) RETURNS VARCHAR ->
CASE
WHEN CURRENT_ROLE() = 'ADMIN' THEN Cust_id
ELSE 'XXXXXX'
END;
ALTER TABLE GLOBALBANK.RAW_DATA.CUSTOMER_RAW MODIFY COLUMN phone_number SET MASKING POLICY GLOBALBANK.RAW_DATA.customer_id_MASK;


select * from tra_test;