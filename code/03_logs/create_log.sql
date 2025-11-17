/*****************************************************
 * Snowflake Trail Observability HOL
 * Phase 3: Analyze Logs
 *****************************************************
 * Description: This script creates functions and
 * procedures that generate log messages at various
 * severity levels for observability analysis.
 *
 * Execution Time: ~5 minutes
 * Prerequisites: Phase 1 and 2 completed
 *****************************************************/

-- Step 1: Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create schema for logs examples
CREATE SCHEMA IF NOT EXISTS OBSERVABILITY_HOL_DB.LOGS_SCHEMA;
USE SCHEMA <SCHEMA_NAME>; -- Replace with the actual schema name
USE WAREHOUSE <WAREHOUSE_NAME>; -- Replace with the actual warehouse name

-- Step 3: Create a Python UDF that generates logs
CREATE OR REPLACE FUNCTION validate_email(email STRING)
RETURNS BOOLEAN
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'validate_email_handler'
PACKAGES = ('snowflake-snowpark-python')
AS
$$
import logging
import re

# Set up logging
logger = logging.getLogger('email_validator')

def validate_email_handler(email):
    """
    Validate email format and log the validation process
    """
    logger.info(f'Starting email validation for: {email}')
    
    if not email:
        logger.warn('Empty email provided')
        return False
    
    # Simple email regex pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if re.match(pattern, email):
        logger.info(f'Email validation successful: {email}')
        return True
    else:
        logger.error(f'Invalid email format: {email}')
        return False
$$;

-- Step 4: Create a stored procedure with comprehensive logging
CREATE OR REPLACE PROCEDURE process_customer_data()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_data'
AS
$$
import logging
from snowflake.snowpark import Session

# Configure logging
logger = logging.getLogger('customer_processor')

def process_data(session: Session):
    """
    Process customer data with comprehensive logging
    """
    logger.info('=== Starting customer data processing ===')
    
    try:
        # Log the start of data retrieval
        logger.debug('Fetching customer data from database')
        
        # Query customers
        customers_df = session.sql("""
            SELECT customer_id, customer_name, email 
            FROM OBSERVABILITY_HOL_DB.TRACES_SCHEMA.CUSTOMERS
        """).collect()
        
        customer_count = len(customers_df)
        logger.info(f'Successfully retrieved {customer_count} customers')
        
        # Process each customer
        valid_emails = 0
        invalid_emails = 0
        
        for customer in customers_df:
            customer_id = customer['CUSTOMER_ID']
            email = customer['EMAIL']
            
            logger.debug(f'Processing customer ID: {customer_id}')
            
            if email and '@' in email:
                valid_emails += 1
                logger.debug(f'Valid email for customer {customer_id}')
            else:
                invalid_emails += 1
                logger.warn(f'Invalid email for customer {customer_id}: {email}')
        
        # Log summary
        logger.info(f'Processing complete. Valid emails: {valid_emails}, Invalid: {invalid_emails}')
        
        if invalid_emails > 0:
            logger.warn(f'Found {invalid_emails} customers with invalid email addresses')
        
        result = f'Processed {customer_count} customers: {valid_emails} valid emails, {invalid_emails} invalid emails'
        logger.info('=== Customer data processing completed successfully ===')
        
        return result
        
    except Exception as e:
        logger.error(f'Error during customer processing: {str(e)}')
        raise
$$;

-- Step 5: Create a procedure that demonstrates different log levels
CREATE OR REPLACE PROCEDURE demonstrate_log_levels()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'demo_logs'
AS
$$
import logging

logger = logging.getLogger('log_level_demo')

def demo_logs(session):
    """
    Demonstrate all log levels in Snowflake
    """
    # TRACE level (most verbose)
    logger.debug('TRACE/DEBUG: This is a debug message for detailed troubleshooting')
    
    # INFO level
    logger.info('INFO: This is an informational message about normal operations')
    
    # WARN level
    logger.warning('WARN: This is a warning about a potential issue')
    
    # ERROR level
    logger.error('ERROR: This is an error message indicating a problem occurred')
    
    # Simulating a process with logs
    logger.info('Starting data validation process')
    
    for i in range(1, 4):
        logger.debug(f'Processing record {i} of 3')
        
        if i == 2:
            logger.warning(f'Record {i} has missing optional field')
    
    logger.info('Data validation process completed')
    
    return 'Log level demonstration complete - check Snowsight Logs tab'
$$;

-- Step 6: Create a procedure that simulates error handling with logging
CREATE OR REPLACE PROCEDURE safe_divide(numerator FLOAT, denominator FLOAT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'divide_handler'
AS
$$
import logging

logger = logging.getLogger('safe_divide')

def divide_handler(session, numerator, denominator):
    """
    Safely perform division with comprehensive error logging
    """
    logger.info(f'Division requested: {numerator} / {denominator}')
    
    try:
        if denominator == 0:
            logger.error('Division by zero attempted - operation cannot proceed')
            return 'ERROR: Division by zero'
        
        result = numerator / denominator
        logger.info(f'Division successful: {numerator} / {denominator} = {result}')
        return f'Result: {result}'
        
    except Exception as e:
        logger.error(f'Unexpected error during division: {str(e)}')
        return f'ERROR: {str(e)}'
$$;

-- Step 7: Execute procedures to generate log data
-- These executions will create log entries visible in Snowsight
CALL demonstrate_log_levels();
CALL process_customer_data();
CALL safe_divide(100, 5);
CALL safe_divide(100, 0);  -- This will generate an error log

-- Step 8: Test the email validation UDF with various inputs
SELECT 
    email,
    validate_email(email) AS is_valid
FROM (
    SELECT 'john.doe@example.com' AS email
    UNION ALL SELECT 'invalid-email'
    UNION ALL SELECT 'test@domain.co.uk'
    UNION ALL SELECT 'bad@email@domain.com'
    UNION ALL SELECT ''
);

-- Step 9: Create a procedure with structured logging
CREATE OR REPLACE PROCEDURE analyze_order_patterns()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'analyze_orders'
AS
$$
import logging

logger = logging.getLogger('order_analyzer')

def analyze_orders(session):
    """
    Analyze order patterns with detailed logging
    """
    logger.info('Starting order pattern analysis')
    
    try:
        # Get order statistics
        logger.debug('Querying order statistics')
        result = session.sql("""
            SELECT 
                COUNT(*) as total_orders,
                SUM(order_amount) as total_revenue,
                AVG(order_amount) as avg_order_value
            FROM OBSERVABILITY_HOL_DB.TRACES_SCHEMA.ORDERS
        """).collect()
        
        if result:
            stats = result[0]
            total_orders = stats['TOTAL_ORDERS']
            total_revenue = float(stats['TOTAL_REVENUE'])
            avg_value = float(stats['AVG_ORDER_VALUE'])
            
            logger.info(f'Order statistics retrieved: {total_orders} orders')
            logger.info(f'Total revenue: ${total_revenue:.2f}')
            logger.info(f'Average order value: ${avg_value:.2f}')
            
            # Check for anomalies
            if avg_value > 300:
                logger.warning(f'High average order value detected: ${avg_value:.2f}')
            elif avg_value < 50:
                logger.warning(f'Low average order value detected: ${avg_value:.2f}')
            else:
                logger.info('Order values within expected range')
            
            return f'Analysis complete: {total_orders} orders, ${total_revenue:.2f} revenue'
        else:
            logger.error('No order data found')
            return 'No data available'
            
    except Exception as e:
        logger.error(f'Failed to analyze order patterns: {str(e)}')
        raise
$$;

-- Step 10: Execute the order analysis procedure
CALL analyze_order_patterns();

/*****************************************************
 * VIEWING LOGS IN SNOWSIGHT
 *****************************************************
 * 
 * After executing this script:
 * 
 * 1. Navigate to Monitoring » Traces & Logs
 * 2. Click on the "Logs" tab
 * 3. Use filters to find specific logs:
 *    - Time Range: Adjust to see recent logs
 *    - Severity: Filter by DEBUG, INFO, WARN, ERROR
 *    - Languages: Filter by Python
 *    - Database: Select OBSERVABILITY_HOL_DB
 * 
 * Key things to observe:
 * - Different log severity levels
 * - Log messages from different procedures
 * - Error logs from failed operations
 * - Contextual information in log messages
 *****************************************************/

-- Step 11: Query logs directly from event table
SELECT 
    TIMESTAMP,
    RECORD_TYPE,
    RECORD['severity_text']::STRING AS severity,
    RECORD['body']::STRING AS log_message,
    RESOURCE_ATTRIBUTES['snow.database.name']::STRING AS database_name,
    RESOURCE_ATTRIBUTES['snow.schema.name']::STRING AS schema_name
FROM SNOWFLAKE.TELEMETRY.EVENTS
WHERE RECORD_TYPE = 'LOG'
ORDER BY TIMESTAMP DESC
LIMIT 50;

-- Step 12: Query logs by severity level
SELECT 
    RECORD['severity_text']::STRING AS severity_level,
    COUNT(*) AS log_count
FROM SNOWFLAKE.TELEMETRY.EVENTS
WHERE RECORD_TYPE = 'LOG'
GROUP BY severity_level
ORDER BY log_count DESC;

/*****************************************************
 * LOG SEVERITY LEVELS IN SNOWFLAKE:
 * 
 * TRACE/DEBUG - Detailed diagnostic information
 * INFO        - General informational messages
 * WARN        - Warning messages for potential issues
 * ERROR       - Error messages for failures
 * FATAL       - Critical failures (rare)
 * 
 * Best Practices:
 * - Use INFO for normal operation milestones
 * - Use WARN for recoverable issues
 * - Use ERROR for actual failures
 * - Include context in log messages (IDs, values)
 * - Avoid logging sensitive data (PII, credentials)
 *****************************************************/

/*****************************************************
 * END OF PHASE 3
 * 
 * Next Step: Proceed to Phase 4 - Query History
 * Script: code/04_query_history/sample_queries.sql
 *****************************************************/

