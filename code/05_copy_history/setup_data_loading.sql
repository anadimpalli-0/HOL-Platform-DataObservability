/*****************************************************
 * Snowflake Trail Observability HOL
 * Phase 5: Copy History
 *****************************************************
 * Description: This script demonstrates data loading
 * operations and generates copy history for monitoring
 * COPY INTO, Snowpipe, and data ingestion activities.
 *
 * Execution Time: ~5 minutes
 * Prerequisites: Phase 1-4 completed
 *****************************************************/

-- Step 1: Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create schema for copy operations
CREATE SCHEMA IF NOT EXISTS OBSERVABILITY_HOL_DB.COPY_SCHEMA;
USE SCHEMA OBSERVABILITY_HOL_DB.COPY_SCHEMA;
USE WAREHOUSE OBSERVABILITY_WH;

/*****************************************************
 * PART 1: Setup for Data Loading
 *****************************************************/

-- Step 3: Create target tables for data loading
CREATE OR REPLACE TABLE sales_data (
    transaction_id INT,
    transaction_date DATE,
    customer_id INT,
    product_id INT,
    quantity INT,
    amount DECIMAL(10,2),
    region VARCHAR(50)
);

CREATE OR REPLACE TABLE customer_feedback (
    feedback_id INT,
    customer_id INT,
    feedback_date DATE,
    rating INT,
    comments VARCHAR(500)
);

-- Step 4: Create an internal stage for file uploads
CREATE OR REPLACE STAGE data_stage
    COMMENT = 'Internal stage for observability HOL data loading';

-- Step 5: Create a file format for CSV data
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMMENT = 'CSV format for data loading';

/*****************************************************
 * PART 2: Generate Sample Data Files
 * Note: We'll create CSV files in the stage to demonstrate
 * actual file-based COPY operations that populate COPY_HISTORY
 *****************************************************/

-- Step 6: Create CSV data inline and load into the stage
-- Create sales_data_batch1.csv
COPY INTO @data_stage/sales_data_batch1.csv
FROM (
    SELECT 
        '1,2024-09-01,101,1,2,2400.00,West' UNION ALL SELECT
        '2,2024-09-02,102,3,1,75.00,East' UNION ALL SELECT
        '3,2024-09-03,103,5,1,299.00,Central' UNION ALL SELECT
        '4,2024-09-04,104,2,5,125.00,West' UNION ALL SELECT
        '5,2024-09-05,105,4,2,700.00,East'
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = 'NONE' RECORD_DELIMITER = '\n')
SINGLE = TRUE
HEADER = FALSE
OVERWRITE = TRUE;

-- Create sales_data_batch2.csv
COPY INTO @data_stage/sales_data_batch2.csv
FROM (
    SELECT 
        '6,2024-09-06,101,6,1,450.00,West' UNION ALL SELECT
        '7,2024-09-07,102,7,10,59.90,East' UNION ALL SELECT
        '8,2024-09-08,103,8,3,37.50,Central' UNION ALL SELECT
        '9,2024-09-09,104,1,1,1200.00,West' UNION ALL SELECT
        '10,2024-09-10,105,3,2,150.00,East'
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = 'NONE' RECORD_DELIMITER = '\n')
SINGLE = TRUE
HEADER = FALSE
OVERWRITE = TRUE;

-- Create sales_data_batch3.csv
COPY INTO @data_stage/sales_data_batch3.csv
FROM (
    SELECT 
        '11,2024-09-11,101,5,1,299.00,West' UNION ALL SELECT
        '12,2024-09-12,102,4,1,350.00,East' UNION ALL SELECT
        '13,2024-09-13,103,2,3,75.00,Central' UNION ALL SELECT
        '14,2024-09-14,104,6,1,450.00,West' UNION ALL SELECT
        '15,2024-09-15,105,1,1,1200.00,East'
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = 'NONE' RECORD_DELIMITER = '\n')
SINGLE = TRUE
HEADER = FALSE
OVERWRITE = TRUE;

-- Step 7: Create customer feedback CSV file
COPY INTO @data_stage/customer_feedback.csv
FROM (
    SELECT 
        '1,101,2024-09-16,5,Excellent product quality!' UNION ALL SELECT
        '2,102,2024-09-17,4,Good service fast delivery' UNION ALL SELECT
        '3,103,2024-09-18,3,Product is okay nothing special' UNION ALL SELECT
        '4,104,2024-09-19,5,Exceeded expectations!' UNION ALL SELECT
        '5,105,2024-09-20,2,Disappointed with quality' UNION ALL SELECT
        '6,101,2024-09-21,5,Will definitely buy again' UNION ALL SELECT
        '7,102,2024-09-22,4,Great value for money'
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = 'NONE' RECORD_DELIMITER = '\n')
SINGLE = TRUE
HEADER = FALSE
OVERWRITE = TRUE;

/*****************************************************
 * PART 3: Demonstrate Different COPY Operations
 * Now loading from actual files which will populate COPY_HISTORY
 *****************************************************/

-- Step 8: Load initial sales data from file
COPY INTO sales_data
FROM @data_stage/sales_data_batch1.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO sales_data
FROM @data_stage/sales_data_batch2.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO sales_data
FROM @data_stage/sales_data_batch3.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Step 9: Load customer feedback from file
COPY INTO customer_feedback
FROM @data_stage/customer_feedback.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Step 10: Create a table with validation rules
CREATE OR REPLACE TABLE validated_sales (
    transaction_id INT NOT NULL,
    transaction_date DATE NOT NULL,
    customer_id INT NOT NULL,
    product_id INT,
    quantity INT,
    amount DECIMAL(10,2) ,
    region VARCHAR(50)
);

-- Step 11: COPY with successful validation from file
COPY INTO validated_sales
FROM @data_stage/sales_data_batch1.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Step 12: Create a table for incremental loading
CREATE OR REPLACE TABLE incremental_sales (
    transaction_id INT,
    transaction_date DATE,
    customer_id INT,
    product_id INT,
    quantity INT,
    amount DECIMAL(10,2),
    region VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Step 13: Simulate incremental loads from different file batches
-- This demonstrates how incremental loading works with multiple files
COPY INTO incremental_sales (transaction_id, transaction_date, customer_id, product_id, quantity, amount, region)
FROM @data_stage/sales_data_batch1.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Step 14: Load second batch
COPY INTO incremental_sales (transaction_id, transaction_date, customer_id, product_id, quantity, amount, region)
FROM @data_stage/sales_data_batch2.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Step 15: Load third batch
COPY INTO incremental_sales (transaction_id, transaction_date, customer_id, product_id, quantity, amount, region)
FROM @data_stage/sales_data_batch3.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

/*****************************************************
 * PART 4: Demonstrate Error Handling
 *****************************************************/

-- Step 16: Create a file with some problematic data for error demonstration
COPY INTO @data_stage/sales_with_errors.csv
FROM (
    SELECT 
        '101,2024-09-01,201,1,5,INVALID_AMOUNT,West' UNION ALL SELECT
        '102,2024-09-02,202,2,3,150.50,East' UNION ALL SELECT
        '103,INVALID_DATE,203,3,2,200.00,Central' UNION ALL SELECT
        '104,2024-09-04,204,4,1,350.75,West'
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = 'NONE' RECORD_DELIMITER = '\n')
SINGLE = TRUE
HEADER = FALSE
OVERWRITE = TRUE;

-- Step 17: Create a table with strict constraints
CREATE OR REPLACE TABLE strict_sales (
    transaction_id INT,
    transaction_date DATE NOT NULL,
    customer_id INT,
    product_id INT,
    quantity INT,
    amount DECIMAL(10,2) NOT NULL,
    region VARCHAR(50)
);

-- Step 18: Attempt to load data with potential errors
-- Using ON_ERROR = 'CONTINUE' to capture errors in copy history
COPY INTO strict_sales
FROM @data_stage/sales_with_errors.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Step 19: Try loading valid data again to show successful loads
COPY INTO strict_sales
FROM @data_stage/sales_data_batch2.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

/*****************************************************
 * PART 5: Query Copy History - REAL-TIME MONITORING
 *****************************************************
 * This section uses INFORMATION_SCHEMA.COPY_HISTORY which provides
 * REAL-TIME data with NO LATENCY. Use these queries for:
 * - Immediate troubleshooting
 * - Operational monitoring
 * - Testing and development
 * - Validating loads just completed
 *
 * Data Retention: 14 days
 * Latency: Real-time (seconds)
 *****************************************************/

-- Step 20: Check files in the stage (verify files were created)
LIST @data_stage;

-- Step 21: Query INFORMATION_SCHEMA for immediate results (no latency)
-- This view shows load history with minimal delay
-- Note: INFORMATION_SCHEMA.COPY_HISTORY requires specific table names, so we query each table
SELECT 
    TABLE_NAME,
    TABLE_SCHEMA_NAME,
    TABLE_CATALOG_NAME AS DATABASE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    FILE_SIZE,
    STATUS,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SALES_DATA',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
UNION ALL
SELECT 
    TABLE_NAME,
    TABLE_SCHEMA_NAME,
    TABLE_CATALOG_NAME AS DATABASE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    FILE_SIZE,
    STATUS,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CUSTOMER_FEEDBACK',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
UNION ALL
SELECT 
    TABLE_NAME,
    TABLE_SCHEMA_NAME,
    TABLE_CATALOG_NAME AS DATABASE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    FILE_SIZE,
    STATUS,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'VALIDATED_SALES',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
UNION ALL
SELECT 
    TABLE_NAME,
    TABLE_SCHEMA_NAME,
    TABLE_CATALOG_NAME AS DATABASE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    FILE_SIZE,
    STATUS,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'INCREMENTAL_SALES',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
UNION ALL
SELECT 
    TABLE_NAME,
    TABLE_SCHEMA_NAME,
    TABLE_CATALOG_NAME AS DATABASE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    FILE_SIZE,
    STATUS,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STRICT_SALES',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- Step 22: Analyze copy performance by table (using INFORMATION_SCHEMA)
WITH all_copy_history AS (
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'SALES_DATA',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'CUSTOMER_FEEDBACK',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'VALIDATED_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'INCREMENTAL_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'STRICT_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
)
SELECT 
    TABLE_NAME,
    COUNT(*) AS copy_operations,
    SUM(ROW_COUNT) AS total_rows_loaded,
    SUM(FILE_SIZE) AS total_bytes_loaded,
    AVG(ROW_COUNT) AS avg_rows_per_operation,
    SUM(ERROR_COUNT) AS total_errors
FROM all_copy_history
GROUP BY TABLE_NAME
ORDER BY total_rows_loaded DESC;

-- Step 23: Identify copy operations with errors (using INFORMATION_SCHEMA)
WITH all_copy_history AS (
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'SALES_DATA',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'CUSTOMER_FEEDBACK',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'VALIDATED_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'INCREMENTAL_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'STRICT_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
)
SELECT 
    TABLE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUMBER,
    FIRST_ERROR_CHARACTER_POS,
    LAST_LOAD_TIME
FROM all_copy_history
WHERE ERROR_COUNT > 0
ORDER BY LAST_LOAD_TIME DESC;

/*****************************************************
 * VIEWING COPY HISTORY IN SNOWSIGHT
 *****************************************************
 * 
 * After executing this script:
 * 
 * 1. Navigate to Ingestion » Copy History
 * 2. Use filters to narrow down activity:
 *    - Status (Success, Failed, Partially Loaded)
 *    - Database: OBSERVABILITY_HOL_DB
 *    - Schema: COPY_SCHEMA
 *    - Time Range: Last hour
 * 
 * 3. Review the Copy Operations view showing:
 *    - Status of each operation
 *    - Target table
 *    - Data size loaded
 *    - Number of rows loaded
 *    - Error information (if any)
 * 
 * 4. Click on any operation to see:
 *    - Individual file details
 *    - Error messages for failed loads
 *    - Detailed statistics per file
 * 
 * Key metrics to monitor:
 * - Load success/failure rates
 * - Data volume trends
 * - Load duration patterns
 * - Error types and frequencies
 *****************************************************/

/*****************************************************
 * PART 6: Advanced Copy History Analysis - REAL-TIME
 *****************************************************
 * Advanced analytical queries using INFORMATION_SCHEMA
 * for immediate, real-time insights
 *****************************************************/

-- Step 24: Calculate load performance metrics (using INFORMATION_SCHEMA)
WITH all_copy_history AS (
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'SALES_DATA',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'CUSTOMER_FEEDBACK',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'VALIDATED_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'INCREMENTAL_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'STRICT_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
)
SELECT 
    TABLE_NAME,
    DATE_TRUNC('hour', LAST_LOAD_TIME) AS load_hour,
    COUNT(*) AS operations,
    SUM(ROW_COUNT) AS rows_loaded,
    SUM(FILE_SIZE) / POWER(1024, 2) AS mb_loaded,
    AVG(FILE_SIZE / NULLIF(ROW_COUNT, 0)) AS avg_bytes_per_row
FROM all_copy_history
GROUP BY TABLE_NAME, DATE_TRUNC('hour', LAST_LOAD_TIME)
ORDER BY load_hour DESC;

-- Step 25: Monitor load patterns by status (using INFORMATION_SCHEMA)
WITH all_copy_history AS (
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'SALES_DATA',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'CUSTOMER_FEEDBACK',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'VALIDATED_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'INCREMENTAL_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
    UNION ALL
    SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'STRICT_SALES',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
    ))
)
SELECT 
    STATUS,
    COUNT(*) AS operation_count,
    SUM(ROW_COUNT) AS total_rows,
    SUM(ERROR_COUNT) AS total_errors,
    ROUND(AVG(ERROR_COUNT), 2) AS avg_errors_per_operation
FROM all_copy_history
GROUP BY STATUS
ORDER BY operation_count DESC;

/*****************************************************
 * PART 7: Historical Copy History - ACCOUNT_USAGE -- **NOTE: THIS SECTION IS OPTIONAL AND INTENDED TO PROVIDE INFORMATION ON HISTORICAL REPORTING**
 *****************************************************
 * 
 * ⏰ IMPORTANT: ACCOUNT_USAGE VIEWS HAVE LATENCY
 * 
 * Latency: 45 minutes to 3 hours (sometimes up to 90 minutes)
 * Data Retention: 365 days (1 year)
 * 
 * WHY THE LATENCY EXISTS:
 * ------------------------
 * ACCOUNT_USAGE views aggregate data from across your entire
 * Snowflake account, processing information from:
 * - Multiple databases, schemas, and warehouses
 * - All users and roles across the account
 * - Historical data requiring validation and processing
 * - Compliance and audit trail requirements
 * 
 * This comprehensive aggregation takes time but provides:
 * ✓ Long-term historical analysis (up to 1 year)
 * ✓ Account-wide trends and patterns
 * ✓ Compliance reporting and auditing
 * ✓ Cross-database analytics
 * 
 * WHEN TO USE ACCOUNT_USAGE:
 * ---------------------------
 * ✓ Analyzing trends over weeks/months
 * ✓ Historical compliance reporting
 * ✓ Long-term capacity planning
 * ✓ Identifying patterns across time periods
 * ✓ Account-wide load analysis
 * 
 * WHEN NOT TO USE ACCOUNT_USAGE:
 * -------------------------------
 * ✗ Immediate troubleshooting (use INFORMATION_SCHEMA instead)
 * ✗ Real-time monitoring (use INFORMATION_SCHEMA instead)
 * ✗ Testing/development validation (use INFORMATION_SCHEMA instead)
 * ✗ Data loaded in the last 1-3 hours (won't appear yet)
 * 
 * NOTE: If queries below return 0 rows, wait 45-90 minutes
 *       and try again, or use INFORMATION_SCHEMA queries instead.
 *****************************************************/

-- Step 26: View recent copy history from ACCOUNT_USAGE
-- This may return 0 rows if data hasn't propagated yet (45 min - 3 hour latency)
SELECT 
    TABLE_NAME,
    STAGE_LOCATION,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    FILE_SIZE,
    STATUS,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_SCHEMA_NAME = 'COPY_SCHEMA'
  AND LAST_LOAD_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY LAST_LOAD_TIME DESC;

-- Step 27: Analyze copy performance by table (ACCOUNT_USAGE)
-- Better for historical trends over days/weeks
SELECT 
    TABLE_NAME,
    COUNT(*) AS copy_operations,
    SUM(ROW_COUNT) AS total_rows_loaded,
    SUM(FILE_SIZE) AS total_bytes_loaded,
    AVG(ROW_COUNT) AS avg_rows_per_operation,
    SUM(ERROR_COUNT) AS total_errors
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_SCHEMA_NAME = 'COPY_SCHEMA'
  AND LAST_LOAD_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY TABLE_NAME
ORDER BY total_rows_loaded DESC;

-- Step 28: Identify copy operations with errors (ACCOUNT_USAGE)
SELECT 
    TABLE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUMBER,
    FIRST_ERROR_CHARACTER_POS,
    LAST_LOAD_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE ERROR_COUNT > 0
  AND TABLE_SCHEMA_NAME = 'COPY_SCHEMA'
  AND LAST_LOAD_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY LAST_LOAD_TIME DESC;

-- Step 29: Calculate load performance metrics (ACCOUNT_USAGE)
-- Ideal for analyzing trends over longer time periods
SELECT 
    TABLE_NAME,
    DATE_TRUNC('hour', LAST_LOAD_TIME) AS load_hour,
    COUNT(*) AS operations,
    SUM(ROW_COUNT) AS rows_loaded,
    SUM(FILE_SIZE) / POWER(1024, 2) AS mb_loaded,
    AVG(FILE_SIZE / NULLIF(ROW_COUNT, 0)) AS avg_bytes_per_row
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_SCHEMA_NAME = 'COPY_SCHEMA'
  AND LAST_LOAD_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY TABLE_NAME, DATE_TRUNC('hour', LAST_LOAD_TIME)
ORDER BY load_hour DESC;

-- Step 30: Monitor load patterns by status (ACCOUNT_USAGE)
-- Track success/failure rates over time
SELECT 
    STATUS,
    COUNT(*) AS operation_count,
    SUM(ROW_COUNT) AS total_rows,
    SUM(ERROR_COUNT) AS total_errors,
    ROUND(AVG(ERROR_COUNT), 2) AS avg_errors_per_operation
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_SCHEMA_NAME = 'COPY_SCHEMA'
  AND LAST_LOAD_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY STATUS
ORDER BY operation_count DESC;

-- Step 31: Weekly trend analysis (ACCOUNT_USAGE)
-- This demonstrates the value of historical data retention
SELECT 
    DATE_TRUNC('day', LAST_LOAD_TIME) AS load_date,
    COUNT(*) AS daily_operations,
    SUM(ROW_COUNT) AS daily_rows,
    SUM(FILE_SIZE) / POWER(1024, 3) AS daily_gb_loaded,
    AVG(CASE WHEN STATUS = 'Loaded' THEN 1 ELSE 0 END) * 100 AS success_rate_percent
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_SCHEMA_NAME = 'COPY_SCHEMA'
  AND LAST_LOAD_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('day', LAST_LOAD_TIME)
ORDER BY load_date DESC;

/*****************************************************
 * COMPARISON: INFORMATION_SCHEMA vs ACCOUNT_USAGE
 *****************************************************
 * 
 * | Feature              | INFORMATION_SCHEMA    | ACCOUNT_USAGE      |
 * |----------------------|-----------------------|--------------------|
 * | Latency              | Real-time (seconds)   | 45 min - 3 hours   |
 * | Data Retention       | 14 days               | 365 days           |
 * | Use Case             | Immediate monitoring  | Historical trends  |
 * | Scope                | Current schema/DB     | Account-wide       |
 * | Wildcards            | No (specific tables)  | Yes (via WHERE)    |
 * | Column for Schema    | SCHEMA_NAME           | TABLE_SCHEMA_NAME  |
 * 
 * RECOMMENDATION:
 * - Use INFORMATION_SCHEMA (Steps 20-25) for daily operations
 * - Use ACCOUNT_USAGE (Steps 26-31) for weekly/monthly reporting
 *****************************************************/

/*****************************************************
 * BEST PRACTICES FOR DATA LOADING:
 * 
 * 1. File Size: Aim for 100-250 MB compressed files
 * 2. Error Handling: Use ON_ERROR options appropriately
 *    - CONTINUE: Skip errors and continue
 *    - SKIP_FILE: Skip entire file on error
 *    - ABORT_STATEMENT: Stop on first error
 * 3. Validation: Use VALIDATION_MODE to test before loading
 * 4. Monitoring: Use the right view for your needs:
 *    - INFORMATION_SCHEMA.COPY_HISTORY: Real-time data (last 14 days)
 *    - ACCOUNT_USAGE.COPY_HISTORY: Historical data (up to 365 days, 
 *      but with 45 min to 3 hour latency)
 * 5. Incremental Loads: Use metadata to track loaded files
 * 6. Stage Organization: Organize files by date/type
 * 7. Always load FROM staged files (not subqueries) to populate 
 *    COPY_HISTORY views properly
 *****************************************************/

/*****************************************************
 * TROUBLESHOOTING TIPS:
 * 
 * If ACCOUNT_USAGE.COPY_HISTORY shows 0 rows:
 * 1. Wait 45 minutes to 3 hours for data to appear
 * 2. Use INFORMATION_SCHEMA.COPY_HISTORY instead for immediate results
 * 3. Verify you're loading FROM staged files, not FROM subqueries
 * 4. Check that files exist in the stage: LIST @data_stage;
 * 
 * If queries return errors about schema names:
 * - Use TABLE_SCHEMA_NAME (not SCHEMA_NAME) in ACCOUNT_USAGE views
 * - Use SCHEMA_NAME in INFORMATION_SCHEMA views
 *****************************************************/

/*****************************************************
 * SUMMARY: WHICH QUERIES TO RUN
 *****************************************************
 * 
 * FOR IMMEDIATE RESULTS (Run these first):
 * ----------------------------------------
 * Step 20: LIST @data_stage - Verify files exist
 * Step 21-25: INFORMATION_SCHEMA queries
 *   → Shows data immediately with no latency
 *   → Perfect for testing and troubleshooting
 * 
 * FOR HISTORICAL ANALYSIS (Wait 45-90 minutes):
 * ----------------------------------------------
 * Step 26-31: ACCOUNT_USAGE queries
 *   → Shows long-term trends (up to 1 year)
 *   → Best for compliance and capacity planning
 *   → Will return 0 rows initially due to latency
 * 
 * RECOMMENDED WORKFLOW:
 * ---------------------
 * 1. Run Steps 1-19 to create data and load files
 * 2. Run Steps 20-25 to see immediate results
 * 3. Wait 45-90 minutes
 * 4. Run Steps 26-31 to see historical data
 * 
 *****************************************************/

/*****************************************************
 * END OF PHASE 5
 * 
 * Next Step: Proceed to Phase 6 - Task History
 * Script: code/06_task_history/create_tasks.sql
 *****************************************************/

