-- =============================================================================
-- VALIDATION: PHASE 4 - COPY HISTORY
-- =============================================================================
-- Validates completion of Phase 5: Copy History (code/05_copy_history/setup_data_loading.sql)
-- Expected: 4 passing validations
-- =============================================================================

USE DATABASE OBSERVABILITY_HOL_DB;
USE SCHEMA INFORMATION_SCHEMA;

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description) AS graded_results
FROM (SELECT 'STEP16' AS step, (SELECT COUNT(*) FROM SCHEMATA WHERE SCHEMA_NAME = 'COPY_SCHEMA' AND CATALOG_NAME = 'OBSERVABILITY_HOL_DB') AS actual, 1 AS expected, 'COPY_SCHEMA schema is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP17' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'SALES_DATA' AND TABLE_SCHEMA = 'COPY_SCHEMA') AS actual, 1 AS expected, 'SALES_DATA table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP18' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'CUSTOMER_FEEDBACK' AND TABLE_SCHEMA = 'COPY_SCHEMA') AS actual, 1 AS expected, 'CUSTOMER_FEEDBACK table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP19' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'INCREMENTAL_SALES' AND TABLE_SCHEMA = 'COPY_SCHEMA') AS actual, 1 AS expected, 'INCREMENTAL_SALES table is created for Platform College Observability HOL' AS description)
;

