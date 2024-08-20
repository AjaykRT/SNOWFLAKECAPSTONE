CREATE OR REPLACE RESOURCE MONITOR my_resource_monitor
WITH 
    CREDIT_QUOTA = 50,                  -- Set a quota of 50 credits per month
    FREQUENCY = MONTHLY,                -- Reset the monitor monthly
    START_TIMESTAMP = '2024-08-19';
 
ALTER WAREHOUSE pc_dbt_wh
SET RESOURCE_MONITOR = my_resource_monitor;