-- =============================================================================
-- VALIDATION: PHASE 2 - LOGS SCHEMA
-- =============================================================================
-- Validates completion of Phase 3: Logs (code/03_logs/create_log.sql)
-- Expected: 6 passing validations
-- =============================================================================

USE DATABASE OBSERVABILITY_HOL_DB;
USE SCHEMA INFORMATION_SCHEMA;

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description) AS graded_results
FROM (SELECT 'STEP07' AS step, (SELECT COUNT(*) FROM SCHEMATA WHERE SCHEMA_NAME = 'LOGS_SCHEMA' AND CATALOG_NAME = 'OBSERVABILITY_HOL_DB') AS actual, 1 AS expected, 'LOGS_SCHEMA schema is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP08' AS step, (SELECT COUNT(*) FROM FUNCTIONS WHERE FUNCTION_NAME = 'VALIDATE_EMAIL' AND FUNCTION_SCHEMA = 'LOGS_SCHEMA') AS actual, 1 AS expected, 'VALIDATE_EMAIL function is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP09' AS step, (SELECT COUNT(*) FROM PROCEDURES WHERE PROCEDURE_NAME = 'PROCESS_CUSTOMER_DATA' AND PROCEDURE_SCHEMA = 'LOGS_SCHEMA') AS actual, 1 AS expected, 'PROCESS_CUSTOMER_DATA procedure is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP10' AS step, (SELECT COUNT(*) FROM PROCEDURES WHERE PROCEDURE_NAME = 'DEMONSTRATE_LOG_LEVELS' AND PROCEDURE_SCHEMA = 'LOGS_SCHEMA') AS actual, 1 AS expected, 'DEMONSTRATE_LOG_LEVELS procedure is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP11' AS step, (SELECT COUNT(*) FROM PROCEDURES WHERE PROCEDURE_NAME = 'SAFE_DIVIDE' AND PROCEDURE_SCHEMA = 'LOGS_SCHEMA') AS actual, 1 AS expected, 'SAFE_DIVIDE procedure is created for Platform College Observability HOL' AS description)

UNION ALL

SELECT util_db.public.se_grader(step, (actual = expected), actual, expected, description)
FROM (SELECT 'STEP12' AS step, (SELECT COUNT(*) FROM PROCEDURES WHERE PROCEDURE_NAME = 'ANALYZE_ORDER_PATTERNS' AND PROCEDURE_SCHEMA = 'LOGS_SCHEMA') AS actual, 1 AS expected, 'ANALYZE_ORDER_PATTERNS procedure is created for Platform College Observability HOL' AS description)
;

