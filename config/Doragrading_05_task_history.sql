-- =============================================================================
-- VALIDATION: PHASE 5 - TASK HISTORY
-- =============================================================================
-- Validates completion of Phase 6: Task History (code/06_task_history/create_tasks.sql)
-- Expected: 4 passing validations
-- =============================================================================

USE DATABASE OBSERVABILITY_HOL_DB;
USE SCHEMA INFORMATION_SCHEMA;

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description) AS graded_results
FROM (SELECT 'STEP20' AS step, (SELECT COUNT(*) FROM SCHEMATA WHERE SCHEMA_NAME = 'TASK_SCHEMA' AND CATALOG_NAME = 'OBSERVABILITY_HOL_DB') AS actual, 1 AS expected, 'TASK_SCHEMA schema is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP21' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'TASK_EXECUTION_LOG' AND TABLE_SCHEMA = 'TASK_SCHEMA') AS actual, 1 AS expected, 'TASK_EXECUTION_LOG table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP22' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'DAILY_SALES_SUMMARY' AND TABLE_SCHEMA = 'TASK_SCHEMA') AS actual, 1 AS expected, 'DAILY_SALES_SUMMARY table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP23' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'SALES_METRICS' AND TABLE_SCHEMA = 'TASK_SCHEMA') AS actual, 1 AS expected, 'SALES_METRICS table is created for Platform College Observability HOL' AS description)
;

