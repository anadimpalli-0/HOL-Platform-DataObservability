-- =============================================================================
-- VALIDATION: PHASE 1 - TRACES SCHEMA
-- =============================================================================
-- Validates completion of Phase 2: Traces (code/02_traces/create_trace.sql)
-- Expected: 6 passing validations
-- =============================================================================

USE DATABASE OBSERVABILITY_HOL_DB;
USE SCHEMA INFORMATION_SCHEMA;

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description) AS graded_results
FROM (SELECT 'STEP01' AS step, (SELECT COUNT(*) FROM SCHEMATA WHERE SCHEMA_NAME = 'TRACES_SCHEMA' AND CATALOG_NAME = 'OBSERVABILITY_HOL_DB') AS actual, 1 AS expected, 'TRACES_SCHEMA schema is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP02' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'CUSTOMERS' AND TABLE_SCHEMA = 'TRACES_SCHEMA') AS actual, 1 AS expected, 'CUSTOMERS table is created in TRACES_SCHEMA for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP03' AS step, (SELECT COUNT(*) FROM TABLES WHERE TABLE_NAME = 'ORDERS' AND TABLE_SCHEMA = 'TRACES_SCHEMA') AS actual, 1 AS expected, 'ORDERS table is created in TRACES_SCHEMA for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP04' AS step, (SELECT COUNT(*) FROM PROCEDURES WHERE PROCEDURE_NAME = 'ANALYZE_CUSTOMER_ORDERS' AND PROCEDURE_SCHEMA = 'TRACES_SCHEMA') AS actual, 1 AS expected, 'ANALYZE_CUSTOMER_ORDERS procedure is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP05' AS step, (SELECT COUNT(*) FROM PROCEDURES WHERE PROCEDURE_NAME = 'PROCESS_MONTHLY_REPORT' AND PROCEDURE_SCHEMA = 'TRACES_SCHEMA') AS actual, 1 AS expected, 'PROCESS_MONTHLY_REPORT procedure is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP06' AS step, (SELECT COUNT(*) FROM FUNCTIONS WHERE FUNCTION_NAME = 'CALCULATE_CUSTOMER_LIFETIME_VALUE' AND FUNCTION_SCHEMA = 'TRACES_SCHEMA') AS actual, 1 AS expected, 'CALCULATE_CUSTOMER_LIFETIME_VALUE function is created for Platform College Observability HOL' AS description)
;

