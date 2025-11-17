/*****************************************************
 * Snowflake Trail Observability HOL
 * Phase 2: Explore Traces
 *****************************************************
 * Description: This script creates stored procedures
 * that generate trace data for observability analysis.
 *
 * Execution Time: ~5 minutes
 * Prerequisites: Phase 1 completed (telemetry enabled)
 *****************************************************/

-- Step 1: Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create a database and schema for the lab
-- Replace <YOUR_INITIALS> with your actual initials
CREATE DATABASE IF NOT EXISTS OBSERVABILITY_HOL_DB;
CREATE SCHEMA IF NOT EXISTS OBSERVABILITY_HOL_DB.TRACES_SCHEMA;

-- Step 3: Use the schema
USE SCHEMA <SCHEMA_NAME>; -- Replace with the actual schema name

-- Step 4: Create a warehouse for compute
-- Using X-SMALL size to minimize costs
CREATE WAREHOUSE IF NOT EXISTS OBSERVABILITY_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Observability HOL';

USE WAREHOUSE <WAREHOUSE_NAME>; -- Replace with the actual warehouse name

-- Step 5: Create sample tables for demonstration
CREATE OR REPLACE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    created_date DATE
);

CREATE OR REPLACE TABLE orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    order_amount DECIMAL(10,2)
);

-- Step 6: Insert sample data
INSERT INTO customers VALUES
    (1, 'John Smith', 'john.smith@email.com', '2024-01-15'),
    (2, 'Jane Doe', 'jane.doe@email.com', '2024-02-20'),
    (3, 'Bob Johnson', 'bob.johnson@email.com', '2024-03-10'),
    (4, 'Alice Williams', 'alice.williams@email.com', '2024-04-05'),
    (5, 'Charlie Brown', 'charlie.brown@email.com', '2024-05-12');

INSERT INTO orders VALUES
    (101, 1, '2024-06-01', 150.00),
    (102, 1, '2024-06-15', 225.50),
    (103, 2, '2024-06-10', 89.99),
    (104, 3, '2024-06-20', 450.00),
    (105, 2, '2024-07-01', 175.25),
    (106, 4, '2024-07-05', 320.00),
    (107, 5, '2024-07-10', 95.50);

-- Step 7: Create a stored procedure that performs multiple operations
-- This procedure will generate trace data with multiple spans
CREATE OR REPLACE PROCEDURE analyze_customer_orders()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Create a temporary results table
    CREATE OR REPLACE TEMP TABLE customer_summary AS
    SELECT 
        c.customer_id,
        c.customer_name,
        c.email,
        COUNT(o.order_id) AS total_orders,
        COALESCE(SUM(o.order_amount), 0) AS total_spent,
        COALESCE(AVG(o.order_amount), 0) AS avg_order_value
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name, c.email;
    
    -- Perform additional analysis
    LET high_value_customers INT := (
        SELECT COUNT(*) 
        FROM customer_summary 
        WHERE total_spent > 200
    );
    
    LET total_customers INT := (SELECT COUNT(*) FROM customers);
    
    -- Return summary
    RETURN 'Analysis complete. Total customers: ' || total_customers || 
           ', High-value customers: ' || high_value_customers;
END;
$$;

-- Step 8: Create a more complex procedure with nested operations
CREATE OR REPLACE PROCEDURE process_monthly_report(report_month DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    orders_count INT;
    revenue DECIMAL(10,2);
    result_msg STRING;
BEGIN
    -- Count orders for the month
    SELECT COUNT(*), COALESCE(SUM(order_amount), 0)
    INTO :orders_count, :revenue
    FROM orders
    WHERE DATE_TRUNC('MONTH', order_date) = DATE_TRUNC('MONTH', :report_month);
    
    -- Create monthly summary table
    CREATE OR REPLACE TEMP TABLE monthly_summary AS
    SELECT 
        DATE_TRUNC('MONTH', order_date) AS month,
        COUNT(order_id) AS order_count,
        SUM(order_amount) AS total_revenue,
        AVG(order_amount) AS avg_order_value
    FROM orders
    WHERE DATE_TRUNC('MONTH', order_date) = DATE_TRUNC('MONTH', :report_month)
    GROUP BY DATE_TRUNC('MONTH', order_date);
    
    result_msg := 'Monthly report processed: ' || orders_count || 
                  ' orders, $' || revenue || ' revenue';
    
    RETURN result_msg;
END;
$$;

-- Step 9: Execute the procedures to generate trace data
-- These executions will create traces visible in Snowsight
CALL analyze_customer_orders();
CALL process_monthly_report('2024-06-01');
CALL process_monthly_report('2024-07-01');

-- Step 10: Create a Python UDF that generates trace data
CREATE OR REPLACE FUNCTION calculate_customer_lifetime_value(
    total_spent DECIMAL(10,2),
    months_active INT
)
RETURNS DECIMAL(10,2)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'calculate_ltv'
AS
$$
def calculate_ltv(total_spent, months_active):
    """
    Calculate customer lifetime value with simple projection
    """
    if months_active == 0:
        return 0
    
    monthly_value = total_spent / months_active
    projected_months = 24  # 2-year projection
    ltv = monthly_value * projected_months
    
    return round(ltv, 2)
$$;

-- Step 11: Test the UDF (generates trace data)
SELECT 
    customer_name,
    total_spent,
    calculate_customer_lifetime_value(total_spent, 6) AS projected_ltv
FROM (
    SELECT 
        c.customer_name,
        COALESCE(SUM(o.order_amount), 0) AS total_spent
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_name
);

/*****************************************************
 * VIEWING TRACES IN SNOWSIGHT
 *****************************************************
 * 
 * After executing this script:
 * 
 * 1. Navigate to Monitoring » Traces and Logs
 * 2. You should see traces for the procedure executions
 * 3. Click on any trace to see detailed span information
 * 
 * Key things to observe:
 * - Trace timeline showing all spans
 * - Individual span details (duration, status)
 * - Related metrics (CPU, memory)
 * - Associated logs
 * 
 * Trace Explorer Features:
 * - Filter by Span Type (UDF, Procedures, Query)
 * - Color by Type or Query ID
 * - View execution flow and dependencies
 *****************************************************/

-- Step 12: Query to view trace data directly from event table
SELECT 
    TIMESTAMP,
    RECORD_TYPE,
    RECORD['name']::STRING AS trace_name,
    RECORD['status']::STRING AS status,
    RECORD_ATTRIBUTES['span.kind']::STRING AS span_kind,
    RESOURCE_ATTRIBUTES['snow.database.name']::STRING AS database_name
FROM SNOWFLAKE.TELEMETRY.EVENTS
WHERE RECORD_TYPE = 'SPAN'
ORDER BY TIMESTAMP DESC
LIMIT 20;

/*****************************************************
 * IMPORTANT NOTES ABOUT TRACES:
 * 
 * 1. Simple SQL statements executed directly in worksheets
 *    do NOT generate traces
 * 
 * 2. Traces are generated only for:
 *    - Stored Procedures
 *    - User-Defined Functions (UDFs)
 *    - Streamlit applications
 * 
 * 3. Each trace consists of one or more spans
 * 
 * 4. Spans represent individual operations within a trace
 * 
 * 5. Traces help identify:
 *    - Performance bottlenecks
 *    - Execution flow
 *    - Dependencies between operations
 *****************************************************/

/*****************************************************
 * END OF PHASE 2
 * 
 * Next Step: Proceed to Phase 3 - Analyze Logs
 * Script: code/03_logs/create_log_examples.sql
 *****************************************************/

