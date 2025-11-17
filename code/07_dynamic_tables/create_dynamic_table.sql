/*****************************************************
 * Snowflake Trail Observability HOL
 * Phase 7: Dynamic Tables
 *****************************************************
 * Description: This script creates and monitors
 * dynamic tables to demonstrate automatic materialization,
 * refresh patterns, and data freshness monitoring.
 *
 * Execution Time: ~5 minutes
 * Prerequisites: Phase 1-6 completed
 *****************************************************/

-- Step 1: Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create schema for dynamic tables
CREATE SCHEMA IF NOT EXISTS OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA;
USE SCHEMA OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA;
USE WAREHOUSE OBSERVABILITY_WH;

/*****************************************************
 * PART 1: Create Base Tables
 *****************************************************/

-- Step 3: Create base table for real-time sales tracking
CREATE OR REPLACE TABLE sales_transactions (
    transaction_id INT AUTOINCREMENT,
    transaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    customer_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    region VARCHAR(50),
    payment_method VARCHAR(30)
);

-- Step 4: Insert initial sample data
INSERT INTO <TABLE_NAME> (customer_id, product_id, quantity, unit_price, total_amount, region, payment_method)--Replace with the actual table name sales_transactions
VALUES
    (101, 1, 2, 1200.00, 2400.00, 'West', 'Credit Card'),
    (102, 3, 1, 75.00, 75.00, 'East', 'Debit Card'),
    (103, 5, 1, 299.00, 299.00, 'Central', 'PayPal'),
    (104, 2, 5, 25.00, 125.00, 'West', 'Credit Card'),
    (105, 4, 2, 350.00, 700.00, 'East', 'Credit Card'),
    (101, 6, 1, 450.00, 450.00, 'West', 'Debit Card'),
    (102, 7, 10, 5.99, 59.90, 'East', 'Cash'),
    (103, 8, 3, 12.50, 37.50, 'Central', 'Credit Card');

-- Step 5: Create a customer dimension table
CREATE OR REPLACE TABLE customers_dim (
    customer_id INT,
    customer_name VARCHAR(100),
    customer_tier VARCHAR(20),
    signup_date DATE
);

INSERT INTO <TABLE_NAME> VALUES --Replace with the actual table name customers_dim
    (101, 'John Smith', 'Gold', '2023-01-15'),
    (102, 'Jane Doe', 'Silver', '2023-03-20'),
    (103, 'Bob Johnson', 'Bronze', '2023-06-10'),
    (104, 'Alice Williams', 'Gold', '2023-02-05'),
    (105, 'Charlie Brown', 'Silver', '2023-08-12');

/*****************************************************
 * PART 2: Create Dynamic Tables
 *****************************************************/

-- Step 6: Create a simple dynamic table for sales summary
-- Target lag: 1 minute means the data will be refreshed to stay within 1 minute of real-time
CREATE OR REPLACE DYNAMIC TABLE sales_summary
    TARGET_LAG = '1 minute'
    WAREHOUSE = OBSERVABILITY_WH
AS
SELECT 
    DATE_TRUNC('hour', transaction_time) AS hour,
    COUNT(*) AS transaction_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_transaction_value,
    MAX(total_amount) AS max_transaction_value
FROM sales_transactions
GROUP BY DATE_TRUNC('hour', transaction_time);

-- Step 7: Create a dynamic table for regional analysis
CREATE OR REPLACE DYNAMIC TABLE regional_sales_analysis
    TARGET_LAG = '2 minutes'
    WAREHOUSE = OBSERVABILITY_WH
AS
SELECT 
    region,
    DATE_TRUNC('day', transaction_time) AS sales_date,
    COUNT(*) AS daily_transactions,
    SUM(total_amount) AS daily_revenue,
    AVG(total_amount) AS avg_transaction,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products
FROM sales_transactions
GROUP BY region, DATE_TRUNC('day', transaction_time);

-- Step 8: Create a dynamic table with joins (customer-level summary)
CREATE OR REPLACE DYNAMIC TABLE customer_sales_summary
    TARGET_LAG = '1 minute'
    WAREHOUSE = OBSERVABILITY_WH
AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_tier,
    COUNT(s.transaction_id) AS total_transactions,
    SUM(s.total_amount) AS lifetime_value,
    AVG(s.total_amount) AS avg_transaction_value,
    MAX(s.transaction_time) AS last_transaction_time,
    DATEDIFF(day, MAX(s.transaction_time), CURRENT_TIMESTAMP()) AS days_since_last_purchase
FROM customers_dim c
LEFT JOIN sales_transactions s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_tier;

-- Step 9: Create a cascading dynamic table (built on another dynamic table)
CREATE OR REPLACE DYNAMIC TABLE top_regions
    TARGET_LAG = '5 minutes'
    WAREHOUSE = OBSERVABILITY_WH
AS
SELECT 
    region,
    SUM(daily_revenue) AS total_revenue,
    AVG(daily_transactions) AS avg_daily_transactions,
    SUM(unique_customers) AS total_unique_customers
FROM regional_sales_analysis
GROUP BY region
ORDER BY total_revenue DESC;

-- Step 10: Create a dynamic table for payment method analysis
CREATE OR REPLACE DYNAMIC TABLE payment_method_summary
    TARGET_LAG = '2 minutes'
    WAREHOUSE = OBSERVABILITY_WH
AS
SELECT 
    payment_method,
    COUNT(*) AS transaction_count,
    SUM(total_amount) AS total_amount,
    AVG(total_amount) AS avg_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) AS median_amount
FROM sales_transactions
GROUP BY payment_method;

/*****************************************************
 * PART 3: Query Dynamic Tables
 *****************************************************/

-- Step 11: Query the dynamic tables to see current data
SELECT * FROM sales_summary ORDER BY hour DESC;
SELECT * FROM regional_sales_analysis ORDER BY sales_date DESC, region;
SELECT * FROM customer_sales_summary ORDER BY lifetime_value DESC;
SELECT * FROM top_regions;
SELECT * FROM payment_method_summary ORDER BY total_amount DESC;

/*****************************************************
 * PART 4: Trigger Refreshes by Inserting New Data
 *****************************************************/

-- Step 12: Insert additional transactions to trigger dynamic table refreshes
INSERT INTO sales_transactions (customer_id, product_id, quantity, unit_price, total_amount, region, payment_method)
VALUES
    (101, 1, 1, 1200.00, 1200.00, 'West', 'Credit Card'),
    (103, 4, 1, 350.00, 350.00, 'Central', 'PayPal'),
    (105, 2, 3, 25.00, 75.00, 'East', 'Debit Card'),
    (102, 5, 1, 299.00, 299.00, 'East', 'Credit Card');

-- Step 13: Wait a moment, then query to see updated results
-- In a real scenario, dynamic tables refresh automatically based on target lag
SELECT 'Sales Summary Updated' AS status;
SELECT * FROM sales_summary ORDER BY hour DESC LIMIT 5;

-- Step 14: Insert more data for continuous monitoring
INSERT INTO sales_transactions (customer_id, product_id, quantity, unit_price, total_amount, region, payment_method)
VALUES
    (104, 6, 1, 450.00, 450.00, 'West', 'Credit Card'),
    (101, 7, 20, 5.99, 119.80, 'West', 'Debit Card'),
    (103, 3, 2, 75.00, 150.00, 'Central', 'Cash');

/*****************************************************
 * PART 5: Monitor Dynamic Table Refreshes
 *****************************************************/

-- Step 15: Show all dynamic tables in the schema
SHOW DYNAMIC TABLES IN SCHEMA OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA;

-- Step 16: Get detailed information about a specific dynamic table
DESCRIBE DYNAMIC TABLE sales_summary;

SHOW DYNAMIC TABLES IN SCHEMA OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA;
-- Step 17: Query dynamic table information from ACCOUNT_USAGE
-- Note: ACCOUNT_USAGE views may have latency (up to 45 minutes for metadata)
SELECT 
    "name" AS table_name,
    "database_name",
    "schema_name",
    "target_lag",
    "warehouse",
    "scheduling_state",
    "created_on",
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "created_on" DESC;

-- Step 18: Check refresh history
-- Note: Detailed refresh history requires ACCOUNT_USAGE privileges
-- Alternative: Use SHOW DYNAMIC TABLES to see last refresh time
SHOW DYNAMIC TABLES IN SCHEMA OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA;

SELECT 
    "name" AS table_name,
    "scheduling_state",
    "data_timestamp" AS last_successful_refresh,
    "target_lag",
    "warehouse",
    DATEDIFF(second, "data_timestamp", CURRENT_TIMESTAMP()) AS seconds_since_refresh
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "data_timestamp" DESC;

-- Step 19: Analyze refresh performance
-- Note: Detailed refresh performance metrics require ACCOUNT_USAGE privileges
-- For full refresh history analysis, you need:
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE ACCOUNTADMIN;
-- 
-- Without ACCOUNT_USAGE, you can monitor refresh status using SHOW DYNAMIC TABLES
-- and check the Snowsight UI under Transformation » Dynamic Tables for visual monitoring

--SELECT 'Refresh performance metrics require ACCOUNT_USAGE privileges' AS note,
       --'Use Snowsight UI (Transformation » Dynamic Tables) for visual monitoring' AS alternative;

-- Step 20: Check data lag for dynamic tables
-- Using SHOW DYNAMIC TABLES to check current status and lag
SHOW DYNAMIC TABLES IN SCHEMA OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA;

SELECT 
    "name" AS table_name,
    "database_name",
    "schema_name",
    "scheduling_state",
    "target_lag",
    "data_timestamp",
    CURRENT_TIMESTAMP() AS current_time,
    DATEDIFF(second, "data_timestamp", CURRENT_TIMESTAMP()) AS current_lag_seconds,
    "created_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY current_lag_seconds DESC;

/*****************************************************
 * VIEWING DYNAMIC TABLES IN SNOWSIGHT
 *****************************************************
 * 
 * After executing this script:
 * 
 * 1. Navigate to Transformation » Dynamic Tables
 * 2. Use filters to narrow down by database and status
 * 
 * Dynamic Tables View shows:
 * - Table name and status
 * - Target lag setting
 * - Current data lag
 * - Last refresh time
 * - Next scheduled refresh
 * 
 * Graph View (click on a table):
 * - Refresh history timeline
 * - Refresh duration trends
 * - Data lag over time
 * - Success/failure patterns
 * 
 * Key monitoring activities:
 * - Track refresh performance
 * - Monitor data freshness (actual lag vs target lag)
 * - Identify refresh failures
 * - Optimize target lag settings
 * - Analyze resource consumption
 *****************************************************/

/*****************************************************
 * PART 6: Advanced Dynamic Table Operations
 *****************************************************/

-- Step 21: Manually refresh a dynamic table
-- Normally dynamic tables refresh automatically, but you can force a refresh
ALTER DYNAMIC TABLE sales_summary REFRESH;

-- Step 22: Suspend a dynamic table (stops automatic refreshes)
ALTER DYNAMIC TABLE payment_method_summary SUSPEND;

-- Step 23: Resume a suspended dynamic table
ALTER DYNAMIC TABLE payment_method_summary RESUME;

-- Step 24: Modify target lag
ALTER DYNAMIC TABLE sales_summary SET TARGET_LAG = '60 seconds';

ALTER DYNAMIC TABLE sales_summary SUSPEND;

/*****************************************************
 * DYNAMIC TABLE BEST PRACTICES:
 * 
 * 1. Target Lag Selection:
 *    - Balance freshness needs with compute costs
 *    - Use DOWNSTREAM for dependent tables
 *    - Consider business requirements
 * 
 * 2. Warehouse Sizing:
 *    - Right-size warehouse for refresh workload
 *    - Enable auto-suspend and auto-resume
 *    - Monitor refresh duration trends
 * 
 * 3. Query Design:
 *    - Keep queries efficient and optimized
 *    - Use appropriate filtering and aggregations
 *    - Consider incremental refresh patterns
 * 
 * 4. Monitoring:
 *    - Track actual lag vs target lag
 *    - Monitor refresh success rates
 *    - Alert on excessive lag or failures
 * 
 * 5. Cascading Tables:
 *    - Use DOWNSTREAM lag for dependent tables
 *    - Limit cascade depth
 *    - Test refresh behavior
 * 
 * 6. Cost Management:
 *    - Suspend tables during maintenance
 *    - Adjust target lag based on usage patterns
 *    - Monitor compute consumption
 *****************************************************/

/*****************************************************
 * PART 7: Create Monitoring Queries
 *****************************************************/

-- Step 25: Query dynamic table health status
-- This query provides a health monitoring overview
SHOW DYNAMIC TABLES IN SCHEMA OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA;

SELECT 
    "name" AS table_name,
    "target_lag",
    "scheduling_state",
    "data_timestamp" AS last_successful_refresh,
    DATEDIFF(second, "data_timestamp", CURRENT_TIMESTAMP()) AS seconds_since_refresh,
    "warehouse",
    "created_on",
    CASE 
        WHEN "scheduling_state" = 'RUNNING' THEN 'Healthy'
        WHEN "scheduling_state" = 'SUSPENDED' THEN 'Suspended'
        ELSE 'Check Status'
    END AS health_status
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "name";

/*****************************************************
 * COMPARING DYNAMIC TABLES VS OTHER APPROACHES:
 * 
 * Dynamic Tables vs Materialized Views:
 * - Dynamic tables: Automatic refresh, OpenTelemetry support
 * - Materialized views: Manual refresh required
 * 
 * Dynamic Tables vs Tables with Tasks:
 * - Dynamic tables: Built-in scheduling, automatic dependency management
 * - Tasks: More control, but require manual setup
 * 
 * Dynamic Tables vs Views:
 * - Dynamic tables: Materialized data, faster queries
 * - Views: No storage cost, always current, slower queries
 *****************************************************/

/*****************************************************
 * END OF PHASE 7
 * 
 * Next Step (Optional): Phase 8 - AI Observability
 * 
 * Or proceed to cleanup:
 * Script: code/99_cleanup/cleanup.sql
 *****************************************************/

