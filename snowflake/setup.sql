-- =====================================================================
-- Step 4: Snowflake setup for the stock pipeline
-- Run ONCE in a Snowflake worksheet (Projects -> Worksheets -> +),
-- logged in as your admin user. Select all and click "Run All".
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Cost guard: suspend everything if 30 credits are used in a month
CREATE RESOURCE MONITOR IF NOT EXISTS STOCK_MONITOR
  WITH CREDIT_QUOTA = 30
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- 2. Compute: smallest size, sleeps after 60s idle
CREATE WAREHOUSE IF NOT EXISTS STOCK_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  RESOURCE_MONITOR = STOCK_MONITOR;

-- 3. Storage
CREATE DATABASE IF NOT EXISTS STOCK_DB;

-- 4. Role for the pipeline (loader, dbt, Airflow)
CREATE ROLE IF NOT EXISTS PIPELINE_ROLE;
GRANT ROLE PIPELINE_ROLE TO ROLE SYSADMIN;   -- lets you (admin) see everything it creates
GRANT USAGE ON WAREHOUSE STOCK_WH TO ROLE PIPELINE_ROLE;
GRANT USAGE, CREATE SCHEMA ON DATABASE STOCK_DB TO ROLE PIPELINE_ROLE;

-- 5. Service user that logs in with the RSA key (no password)
CREATE USER IF NOT EXISTS PIPELINE_USER
  TYPE = SERVICE
  DEFAULT_ROLE = PIPELINE_ROLE
  DEFAULT_WAREHOUSE = STOCK_WH
  DEFAULT_NAMESPACE = STOCK_DB.RAW
  RSA_PUBLIC_KEY = 'PASTE_YOUR_PUBLIC_KEY_HERE';   -- contents of keys/snowflake_rsa_key.pub, without the BEGIN/END lines
GRANT ROLE PIPELINE_ROLE TO USER PIPELINE_USER;

-- 6. Raw (bronze) objects, owned by the pipeline role
USE ROLE PIPELINE_ROLE;
USE WAREHOUSE STOCK_WH;
CREATE SCHEMA IF NOT EXISTS STOCK_DB.RAW;
USE SCHEMA STOCK_DB.RAW;

-- Each line of our bronze files is one JSON record
CREATE FILE FORMAT IF NOT EXISTS JSON_LINES
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE;

-- Internal stage: the loader uploads bronze files here before COPY INTO
CREATE STAGE IF NOT EXISTS BRONZE_STAGE
  FILE_FORMAT = JSON_LINES;

-- Raw table: keep the full JSON as-is; dbt parses it later
CREATE TABLE IF NOT EXISTS STOCK_QUOTES (
  RECORD       VARIANT,
  SOURCE_FILE  STRING,
  LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 7. Check: should show STOCK_QUOTES, BRONZE_STAGE, JSON_LINES
SHOW TABLES IN SCHEMA STOCK_DB.RAW;
SHOW STAGES IN SCHEMA STOCK_DB.RAW;
