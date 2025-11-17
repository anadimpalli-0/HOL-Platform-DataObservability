-- =============================================================================
-- VALIDATION: PHASE 3 - QUERY HISTORY
-- =============================================================================
-- Validates completion of Phase 4: Query History (code/04_query_history/sample_queries.sql)
-- Expected: 3 passing validations
-- =============================================================================

USE DATABASE OBSERVABILITY_HOL_DB;
USE SCHEMA INFORMATION_SCHEMA;

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description) AS graded_results
FROM (SELECT 'STEP13' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'PRODUCTS' AND TABLE_SCHEMA = 'TRACES_SCHEMA') AS actual, 1 AS expected, 'PRODUCTS table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP14' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'ORDER_ITEMS' AND TABLE_SCHEMA = 'TRACES_SCHEMA') AS actual, 1 AS expected, 'ORDER_ITEMS table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP15' AS step, (SELECT COUNT(*) FROM VIEWS WHERE TABLE_NAME = 'CUSTOMER_ANALYTICS' AND TABLE_SCHEMA = 'TRACES_SCHEMA') AS actual, 1 AS expected, 'CUSTOMER_ANALYTICS view is created for Platform College Observability HOL' AS description)
;

