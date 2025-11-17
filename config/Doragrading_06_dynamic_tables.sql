-- =============================================================================
-- VALIDATION: PHASE 6 - DYNAMIC TABLES
-- =============================================================================
-- Validates completion of Phase 7: Dynamic Tables (code/07_dynamic_tables/create_dynamic_table.sql)
-- Expected: 7 passing validations
-- =============================================================================

USE DATABASE OBSERVABILITY_HOL_DB;
USE SCHEMA INFORMATION_SCHEMA;

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description) AS graded_results
FROM (SELECT 'STEP24' AS step, (SELECT COUNT(*) FROM SCHEMATA WHERE SCHEMA_NAME = 'DYNAMIC_TABLES_SCHEMA' AND CATALOG_NAME = 'OBSERVABILITY_HOL_DB') AS actual, 1 AS expected, 'DYNAMIC_TABLES_SCHEMA schema is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP25' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'SALES_TRANSACTIONS' AND TABLE_SCHEMA = 'DYNAMIC_TABLES_SCHEMA') AS actual, 1 AS expected, 'SALES_TRANSACTIONS table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP26' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'CUSTOMERS_DIM' AND TABLE_SCHEMA = 'DYNAMIC_TABLES_SCHEMA') AS actual, 1 AS expected, 'CUSTOMERS_DIM table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP27' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'SALES_SUMMARY' AND TABLE_SCHEMA = 'DYNAMIC_TABLES_SCHEMA') AS actual, 1 AS expected, 'SALES_SUMMARY dynamic table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP28' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'REGIONAL_SALES_ANALYSIS' AND TABLE_SCHEMA = 'DYNAMIC_TABLES_SCHEMA') AS actual, 1 AS expected, 'REGIONAL_SALES_ANALYSIS dynamic table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP29' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'CUSTOMER_SALES_SUMMARY' AND TABLE_SCHEMA = 'DYNAMIC_TABLES_SCHEMA') AS actual, 1 AS expected, 'CUSTOMER_SALES_SUMMARY dynamic table is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP30' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'PAYMENT_METHOD_SUMMARY' AND TABLE_SCHEMA = 'DYNAMIC_TABLES_SCHEMA') AS actual, 1 AS expected, 'PAYMENT_METHOD_SUMMARY dynamic table is created for Platform College Observability HOL' AS description)
;

