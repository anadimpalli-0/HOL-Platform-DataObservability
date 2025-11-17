/*****************************************************
 * Snowflake Trail Observability HOL
 * Phase 6: Task History
 *****************************************************
 * Description: This script creates and executes
 * Snowflake Tasks to demonstrate task monitoring,
 * task graphs, and automated pipeline observability.
 *
 * Execution Time: ~10 minutes (includes task execution time)
 * Prerequisites: Phase 1-5 completed
 *****************************************************/

-- Step 1: Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create schema for tasks
CREATE SCHEMA IF NOT EXISTS OBSERVABILITY_HOL_DB.TASK_SCHEMA;
USE SCHEMA OBSERVABILITY_HOL_DB.TASK_SCHEMA;
USE WAREHOUSE OBSERVABILITY_WH;

/*****************************************************
 * PART 1: Create Tables for Task Demonstrations
 *****************************************************/

-- Step 3: Create a table to track task executions
CREATE OR REPLACE TABLE task_execution_log (
    log_id INT AUTOINCREMENT,
    task_name VARCHAR(100),
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    status VARCHAR(20),
    message VARCHAR(500)
);

-- Step 4: Create a table for daily summary data
CREATE OR REPLACE TABLE daily_sales_summary (
    summary_date DATE,
    total_transactions INT,
    total_revenue DECIMAL(15,2),
    avg_transaction_value DECIMAL(10,2),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Step 5: Create a table for aggregated metrics
CREATE OR REPLACE TABLE sales_metrics (
    metric_date DATE,
    region VARCHAR(50),
    transaction_count INT,
    total_amount DECIMAL(15,2),
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

/*****************************************************
 * PART 2: Create Standalone Tasks
 *****************************************************/

-- Step 6: Create a simple standalone task
-- This task runs every 2 minutes and logs its execution
CREATE OR REPLACE TASK simple_logging_task
    WAREHOUSE = OBSERVABILITY_WH
    SCHEDULE = '2 MINUTE'
AS
    INSERT INTO task_execution_log (task_name, status, message)
    VALUES ('simple_logging_task', 'SUCCESS', 'Task executed successfully');

-- Step 7: Create a task that generates daily sales summary
CREATE OR REPLACE TASK daily_summary_task
    WAREHOUSE = OBSERVABILITY_WH
    SCHEDULE = '5 MINUTE'  -- In production, this would be USING CRON '0 2 * * *' for 2 AM daily
AS
    INSERT INTO daily_sales_summary (summary_date, total_transactions, total_revenue, avg_transaction_value)
    SELECT 
        CURRENT_DATE() AS summary_date,
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_transaction_value
    FROM OBSERVABILITY_HOL_DB.COPY_SCHEMA.sales_data;

-- Step 8: Create a task that calculates regional metrics
CREATE OR REPLACE TASK regional_metrics_task
    WAREHOUSE = OBSERVABILITY_WH
    SCHEDULE = '3 MINUTE'
AS
    MERGE INTO sales_metrics sm
    USING (
        SELECT 
            CURRENT_DATE() AS metric_date,
            region,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount
        FROM OBSERVABILITY_HOL_DB.COPY_SCHEMA.sales_data
        GROUP BY region
    ) src
    ON sm.metric_date = src.metric_date AND sm.region = src.region
    WHEN MATCHED THEN 
        UPDATE SET 
            sm.transaction_count = src.transaction_count,
            sm.total_amount = src.total_amount,
            sm.updated_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (metric_date, region, transaction_count, total_amount)
        VALUES (src.metric_date, src.region, src.transaction_count, src.total_amount);

/*****************************************************
 * PART 3: Create Task Graph (Parent-Child Tasks)
 *****************************************************/

-- Step 9: Create root task for data validation
CREATE OR REPLACE TASK root_validate_data
    WAREHOUSE = OBSERVABILITY_WH
    SCHEDULE = '5 MINUTE'
AS
BEGIN
    -- Validate that source data exists
    LET row_count INT := (SELECT COUNT(*) FROM OBSERVABILITY_HOL_DB.COPY_SCHEMA.sales_data);
    
    IF (row_count > 0) THEN
        INSERT INTO task_execution_log (task_name, status, message)
        VALUES ('root_validate_data', 'SUCCESS', 'Data validation passed: ' || row_count || ' rows found');
    ELSE
        INSERT INTO task_execution_log (task_name, status, message)
        VALUES ('root_validate_data', 'WARNING', 'No data found for processing');
    END IF;
END;

-- Step 10: Create child task 1 - Transform data
CREATE OR REPLACE TASK child_transform_data
    WAREHOUSE = OBSERVABILITY_WH
    AFTER root_validate_data
AS
BEGIN
    -- Create transformed view of data
    CREATE OR REPLACE TEMP TABLE transformed_sales AS
    SELECT 
        transaction_date,
        region,
        SUM(amount) AS daily_regional_total,
        COUNT(*) AS transaction_count,
        AVG(amount) AS avg_amount
    FROM OBSERVABILITY_HOL_DB.COPY_SCHEMA.sales_data
    GROUP BY transaction_date, region;
    
    INSERT INTO task_execution_log (task_name, status, message)
    VALUES ('child_transform_data', 'SUCCESS', 'Data transformation completed');
END;

-- Step 11: Create child task 2 - Calculate KPIs
CREATE OR REPLACE TASK child_calculate_kpis
    WAREHOUSE = OBSERVABILITY_WH
    AFTER root_validate_data
AS
BEGIN
    -- Calculate key performance indicators
    CREATE OR REPLACE TEMP TABLE kpi_results AS
    SELECT 
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(DISTINCT product_id) AS unique_products,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_transaction,
        MAX(amount) AS max_transaction,
        MIN(amount) AS min_transaction
    FROM OBSERVABILITY_HOL_DB.COPY_SCHEMA.sales_data;
    
    INSERT INTO task_execution_log (task_name, status, message)
    VALUES ('child_calculate_kpis', 'SUCCESS', 'KPI calculation completed');
END;

-- Step 12: Create grandchild task - Final aggregation
CREATE OR REPLACE TASK grandchild_final_aggregation
    WAREHOUSE = OBSERVABILITY_WH
    AFTER child_transform_data, child_calculate_kpis
AS
BEGIN
    -- This task runs after both child tasks complete
    INSERT INTO task_execution_log (task_name, status, message)
    VALUES ('grandchild_final_aggregation', 'SUCCESS', 'Final aggregation completed successfully');
END;

/*****************************************************
 * PART 4: Enable and Execute Tasks
 *****************************************************/

-- Step 13: Enable standalone tasks
ALTER TASK simple_logging_task RESUME;
ALTER TASK daily_summary_task RESUME;
ALTER TASK regional_metrics_task RESUME;

-- Step 14: Enable task graph (must enable from child to parent)
ALTER TASK grandchild_final_aggregation RESUME;
ALTER TASK child_calculate_kpis RESUME;
ALTER TASK child_transform_data RESUME;
ALTER TASK root_validate_data RESUME;

-- Step 15: Show all tasks in the schema
SHOW TASKS IN SCHEMA OBSERVABILITY_HOL_DB.TASK_SCHEMA;

-- Step 16: Manually execute a task for immediate results
EXECUTE TASK simple_logging_task;
EXECUTE TASK daily_summary_task;
EXECUTE TASK root_validate_data;

-- Step 17: Wait a moment for tasks to complete, then check execution log
-- Note: In a real scenario, you would wait for scheduled executions
SELECT * FROM task_execution_log ORDER BY execution_time DESC;

/*****************************************************
 * PART 5: Query Task History
 *****************************************************
 * 
 * EXECUTION GUIDE:
 * 
 * RUN NOW (Section 5A):
 * - Steps 18-19: Use SHOW TASKS for immediate results
 * - Shows current task configuration and state
 * 
 * RUN LATER (Section 5B):
 * - Steps 20-21: Use ACCOUNT_USAGE after 1-3 hours
 * - Shows historical execution data and performance metrics
 * 
 *****************************************************/

/*****************************************************
 * SECTION 5A: IMMEDIATE QUERIES (No Latency)
 * These queries use SHOW TASKS which provides
 * real-time data about task configuration and state.
 *****************************************************/

-- Step 18: Check current task states (IMMEDIATE)
SHOW TASKS IN SCHEMA OBSERVABILITY_HOL_DB.TASK_SCHEMA;

-- Step 19: View detailed task information from SHOW TASKS result (IMMEDIATE)
-- This queries the result set from the previous SHOW TASKS command
SELECT 
    "name" AS task_name,
    "database_name",
    "schema_name",
    "schedule",
    "state",
    "warehouse",
    "predecessors",
    "created_on",
    "condition"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "created_on" DESC;

/*****************************************************
 * SECTION 5B: HISTORICAL QUERIES (45 min - 3 hour latency)
 * 
 * IMPORTANT: ACCOUNT_USAGE views have a latency of 
 * 45 minutes to 3 hours. These queries will return 
 * 0 records if run immediately after task creation.
 * 
 * WHY THE LATENCY?
 * - ACCOUNT_USAGE views are populated asynchronously
 * - Data is aggregated from multiple internal sources
 * - Ensures consistency across distributed system
 * - Optimized for long-term analytics, not real-time
 * 
 * RECOMMENDATION:
 * - Run these queries 1-3 hours after task execution
 * - Use SHOW TASKS for immediate task state
 * - Use ACCOUNT_USAGE for historical trend analysis
 *****************************************************/

-- Step 20: View task run history using ACCOUNT_USAGE (DELAYED)
-- Note: Wait 1-3 hours after task execution for data to appear
SELECT 
    name AS task_name,
    database_name,
    schema_name,
    state,
    scheduled_time,
    query_start_time,
    completed_time,
    DATEDIFF(second, query_start_time, completed_time) AS duration_seconds,
    error_code,
    error_message
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE schema_name = 'TASK_SCHEMA'
  AND scheduled_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;

-- Step 21: Analyze task success/failure rates (DELAYED)
-- Note: Wait 1-3 hours after task execution for data to appear
SELECT 
    name AS task_name,
    state,
    COUNT(*) AS execution_count,
    AVG(DATEDIFF(second, query_start_time, completed_time)) AS avg_duration_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE schema_name = 'TASK_SCHEMA'
  AND scheduled_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
GROUP BY name, state
ORDER BY task_name, state;

/*****************************************************
 * VIEWING TASK HISTORY IN SNOWSIGHT
 *****************************************************
 * 
 * After executing this script:
 * 
 * 1. Navigate to Transformation » Tasks
 * 2. Use filters to narrow down by status and database
 * 
 * Task Graphs View:
 * - Groups related tasks in a DAG (directed acyclic graph)
 * - Shows root task name and schedule
 * - Displays recent run history (may have latency)
 * - Click on any task to see dependencies
 * 
 * Task Runs View:
 * - Shows individual task executions
 * - Displays status (Success, Failed, Running)
 * - Shows execution duration
 * - Provides error details for failures
 * 
 * NOTE: The Snowsight UI also uses ACCOUNT_USAGE data
 * which may have 45min-3hr latency. For immediate task
 * status, use SHOW TASKS commands (Section 5A).
 * 
 * Key monitoring activities:
 * - Track task execution success rates
 * - Identify failed tasks and error patterns
 * - Monitor task duration trends
 * - Analyze task graph dependencies
 * - Optimize task scheduling
 *****************************************************/

/*****************************************************
 * PART 6: Task Monitoring Best Practices
 *****************************************************/

-- Step 22: Create a monitoring view for task health (DELAYED - uses ACCOUNT_USAGE)
-- Note: This view uses ACCOUNT_USAGE data which has 45min-3hr latency
CREATE OR REPLACE VIEW task_health_dashboard AS
SELECT 
    name AS task_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(100.0 * SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate,
    AVG(DATEDIFF(second, query_start_time, completed_time)) AS avg_duration_seconds,
    MAX(scheduled_time) AS last_run_time
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE schema_name = 'TASK_SCHEMA'
  AND scheduled_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY name;

-- Step 23: Query the monitoring view (DELAYED)
-- Note: Wait 1-3 hours after task execution for data to appear
SELECT * FROM task_health_dashboard ORDER BY success_rate ASC, failed_runs DESC;

-- Step 24: Identify long-running tasks (DELAYED)
-- Note: Wait 1-3 hours after task execution for data to appear
SELECT 
    name AS task_name,
    scheduled_time,
    DATEDIFF(second, query_start_time, completed_time) AS duration_seconds,
    state,
    error_message
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE schema_name = 'TASK_SCHEMA'
  AND DATEDIFF(second, query_start_time, completed_time) > 60  -- Tasks running longer than 60 seconds
  AND scheduled_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY duration_seconds DESC;

/*****************************************************
 * IMPORTANT: Suspend Tasks After Lab
 * 
 * Tasks continue to run on schedule until explicitly
 * suspended. To avoid ongoing compute costs, suspend
 * all tasks when done with this phase.
 *****************************************************/

-- Step 25: Suspend all tasks (run this when done testing)
ALTER TASK regional_metrics_task SUSPEND;
ALTER TASK daily_summary_task SUSPEND;
ALTER TASK simple_logging_task SUSPEND;
ALTER TASK root_validate_data SUSPEND;
ALTER TASK child_transform_data SUSPEND;
ALTER TASK child_calculate_kpis SUSPEND;
ALTER TASK grandchild_final_aggregation SUSPEND;

/*****************************************************
 * TASK BEST PRACTICES:
 * 
 * 1. Scheduling:
 *    - Use CRON expressions for production schedules
 *    - Consider timezone implications
 *    - Avoid overlapping task executions
 * 
 * 2. Error Handling:
 *    - Implement proper error logging
 *    - Use conditional execution (WHEN clauses)
 *    - Set up alerts for task failures
 * 
 * 3. Dependencies:
 *    - Design clear task graphs
 *    - Limit DAG depth (max 1000 tasks)
 *    - Test individual tasks before chaining
 * 
 * 4. Resource Management:
 *    - Use appropriately sized warehouses
 *    - Enable auto-suspend on warehouses
 *    - Monitor task duration trends
 * 
 * 5. Monitoring:
 *    - Regularly review task history
 *    - Set up alerts for failures
 *    - Track execution duration patterns
 *****************************************************/

/*****************************************************
 * END OF PHASE 6
 * 
 * Next Step: Proceed to Phase 7 - Dynamic Tables
 * Script: code/07_dynamic_tables/create_dynamic_table.sql
 *****************************************************/

