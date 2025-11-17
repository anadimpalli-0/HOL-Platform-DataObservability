/*****************************************************
 * Snowflake Trail Observability HOL
 * Cleanup Script
 *****************************************************
 * Description: This script removes all objects created
 * during the Observability HOL to prevent ongoing costs.
 *
 * IMPORTANT: Review before executing. This will delete
 * all databases, warehouses, and other objects created
 * during this lab.
 *
 * Execution Time: ~2 minutes
 *****************************************************/

-- Step 1: Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

/*****************************************************
 * PART 1: Suspend and Drop Tasks
 * Tasks must be suspended before dropping
 *****************************************************/

-- Suspend all tasks in task schema
USE SCHEMA OBSERVABILITY_HOL_DB.TASK_SCHEMA;

ALTER TASK IF EXISTS grandchild_final_aggregation SUSPEND;
ALTER TASK IF EXISTS child_calculate_kpis SUSPEND;
ALTER TASK IF EXISTS child_transform_data SUSPEND;
ALTER TASK IF EXISTS root_validate_data SUSPEND;
ALTER TASK IF EXISTS simple_logging_task SUSPEND;
ALTER TASK IF EXISTS daily_summary_task SUSPEND;
ALTER TASK IF EXISTS regional_metrics_task SUSPEND;

-- Drop all tasks
DROP TASK IF EXISTS grandchild_final_aggregation;
DROP TASK IF EXISTS child_calculate_kpis;
DROP TASK IF EXISTS child_transform_data;
DROP TASK IF EXISTS root_validate_data;
DROP TASK IF EXISTS simple_logging_task;
DROP TASK IF EXISTS daily_summary_task;
DROP TASK IF EXISTS regional_metrics_task;

/*****************************************************
 * PART 2: Drop Dynamic Tables
 *****************************************************/

DROP DYNAMIC TABLE IF EXISTS OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA.sales_summary;
DROP DYNAMIC TABLE IF EXISTS OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA.regional_sales_analysis;
DROP DYNAMIC TABLE IF EXISTS OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA.customer_sales_summary;
DROP DYNAMIC TABLE IF EXISTS OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA.top_regions;
DROP DYNAMIC TABLE IF EXISTS OBSERVABILITY_HOL_DB.DYNAMIC_TABLES_SCHEMA.payment_method_summary;

/*****************************************************
 * PART 3: Drop Database
 * This will drop all schemas, tables, and views
 *****************************************************/

DROP DATABASE IF EXISTS OBSERVABILITY_HOL_DB CASCADE;

/*****************************************************
 * PART 4: Drop Warehouse
 *****************************************************/

DROP WAREHOUSE IF EXISTS OBSERVABILITY_WH;

/*****************************************************
 * PART 5: Reset Telemetry Settings (Optional)
 * Uncomment if you want to disable telemetry collection
 *****************************************************/

-- ALTER ACCOUNT SET LOG_LEVEL = 'OFF';
-- ALTER ACCOUNT SET METRIC_LEVEL = 'NONE';
-- ALTER ACCOUNT SET TRACE_LEVEL = 'OFF';

/*****************************************************
 * PART 6: Verify Cleanup
 *****************************************************/

-- Verify databases are removed
SHOW DATABASES LIKE 'OBSERVABILITY_HOL%';

-- Verify warehouses are removed
SHOW WAREHOUSES LIKE 'OBSERVABILITY_WH%';

-- Verify tasks are removed
SHOW TASKS LIKE '%TASK%' IN ACCOUNT;

-- Check current telemetry settings
SHOW PARAMETERS LIKE '%LEVEL' IN ACCOUNT;

/*****************************************************
 * CLEANUP COMPLETE
 * 
 * All lab resources have been removed.
 * 
 * Note: Telemetry data in SNOWFLAKE.TELEMETRY.EVENTS
 * will remain but is managed by Snowflake's retention
 * policies.
 * 
 * To review what was learned, see the main README.md
 *****************************************************/

SELECT 'Cleanup completed successfully!' AS status;

