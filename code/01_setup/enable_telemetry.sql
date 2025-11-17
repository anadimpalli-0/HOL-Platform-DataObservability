/*****************************************************
 * Snowflake Trail Observability HOL
 * Phase 1: Enable Telemetry Collection
 *****************************************************
 * Description: This script enables telemetry collection
 * at the account level by setting logging, metrics, 
 * and tracing levels.
 *
 * Execution Time: ~2 minutes
 * Prerequisites: ACCOUNTADMIN role or equivalent
 *****************************************************/

-- Step 1: Switch to ACCOUNTADMIN role
-- This role has the necessary privileges to alter account parameters
USE ROLE ACCOUNTADMIN;

-- Step 2: Set logging level to INFO
-- Valid values: TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF
-- INFO level captures informational messages and above (WARN, ERROR, FATAL)
ALTER ACCOUNT SET LOG_LEVEL = 'INFO';

-- Step 3: Set metrics level to ALL
-- Valid values: ALL, NONE
-- ALL captures all available metrics from your Snowflake workloads
ALTER ACCOUNT SET METRIC_LEVEL = 'ALL';

-- Step 4: Set trace level to ALWAYS
-- Valid values: ALWAYS, ON_EVENT, OFF
-- ALWAYS captures all trace data for procedures, UDFs, and Streamlit apps
ALTER ACCOUNT SET TRACE_LEVEL = 'ALWAYS';

-- Step 5: Verify the telemetry settings
-- This query shows the current values for all telemetry parameters
SHOW PARAMETERS LIKE '%LEVEL' IN ACCOUNT;

-- Step 6: Check the active event table
-- Snowflake uses SNOWFLAKE.TELEMETRY.EVENTS by default if no custom table is set
SHOW PARAMETERS LIKE 'EVENT_TABLE' IN ACCOUNT;

/*****************************************************
 * IMPORTANT NOTES:
 * 
 * 1. Telemetry data is stored in event tables following 
 *    the OpenTelemetry standard
 * 
 * 2. The default event table is SNOWFLAKE.TELEMETRY.EVENTS
 * 
 * 3. Simple SQL statements executed in worksheets do NOT
 *    generate traces - only handler code (procedures, UDFs)
 * 
 * 4. It may take a few minutes for telemetry data to 
 *    appear after enabling these settings
 * 
 * 5. To disable telemetry later, set levels to:
 *    - LOG_LEVEL = 'OFF'
 *    - METRIC_LEVEL = 'NONE'
 *    - TRACE_LEVEL = 'OFF'
 *****************************************************/

-- Optional: Query the event table to see if data is being collected
-- Note: This may return no results immediately after enabling
SELECT 
    TIMESTAMP,
    RECORD_TYPE,
    RECORD,
    RESOURCE_ATTRIBUTES
FROM SNOWFLAKE.TELEMETRY.EVENTS
ORDER BY TIMESTAMP DESC
LIMIT 10;

/*****************************************************
 * END OF PHASE 1
 * 
 * Next Step: Proceed to Phase 2 - Explore Traces
 * Script: code/02_traces/create_trace_examples.sql
 *****************************************************/

