/*****************************************************
 * Snowflake Trail Observability HOL
 * Phase 4: Query History
 *****************************************************
 * Description: This script runs various types of
 * queries to populate query history for analysis
 * and optimization demonstrations.
 *
 * Execution Time: ~5 minutes
 * Prerequisites: Phase 1-3 completed
 *****************************************************/

-- Step 1: Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Step 2: Set context
USE SCHEMA OBSERVABILITY_HOL_DB.TRACES_SCHEMA;
USE WAREHOUSE OBSERVABILITY_WH;

-- Step 3: Create additional sample data for more interesting queries
CREATE OR REPLACE TABLE products (
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2)
);

INSERT INTO products VALUES
    (1, 'Laptop', 'Electronics', 1200.00),
    (2, 'Mouse', 'Electronics', 25.00),
    (3, 'Keyboard', 'Electronics', 75.00),
    (4, 'Monitor', 'Electronics', 350.00),
    (5, 'Desk Chair', 'Furniture', 299.00),
    (6, 'Desk', 'Furniture', 450.00),
    (7, 'Notebook', 'Office Supplies', 5.99),
    (8, 'Pen Set', 'Office Supplies', 12.50);

CREATE OR REPLACE TABLE order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    item_price DECIMAL(10,2)
);

INSERT INTO order_items VALUES
    (1, 101, 1, 1, 1200.00),
    (2, 101, 2, 2, 50.00),
    (3, 102, 4, 1, 350.00),
    (4, 103, 7, 5, 29.95),
    (5, 103, 8, 2, 25.00),
    (6, 104, 5, 1, 299.00),
    (7, 104, 6, 1, 450.00),
    (8, 105, 3, 1, 75.00),
    (9, 105, 2, 1, 25.00);

/*****************************************************
 * QUERY SET 1: Simple SELECT Queries
 * Purpose: Generate basic query history entries
 *****************************************************/

-- Query 1: Simple customer list
SELECT * FROM customers;

-- Query 2: Orders with filtering
SELECT * FROM orders WHERE order_amount > 100;

-- Query 3: Product catalog
SELECT product_name, category, price FROM products ORDER BY price DESC;

/*****************************************************
 * QUERY SET 2: Aggregation Queries
 * Purpose: Demonstrate queries with grouping and aggregation
 *****************************************************/

-- Query 4: Customer order summary
SELECT 
    c.customer_name,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_amount) AS total_spent,
    AVG(o.order_amount) AS avg_order_value
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name
ORDER BY total_spent DESC;

-- Query 5: Sales by category
SELECT 
    p.category,
    COUNT(DISTINCT oi.order_id) AS orders_count,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.item_price) AS revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.category
ORDER BY revenue DESC;

-- Query 6: Monthly revenue trend
SELECT 
    DATE_TRUNC('MONTH', order_date) AS month,
    COUNT(order_id) AS orders,
    SUM(order_amount) AS revenue,
    AVG(order_amount) AS avg_order
FROM orders
GROUP BY DATE_TRUNC('MONTH', order_date)
ORDER BY month;

/*****************************************************
 * QUERY SET 3: Complex JOIN Queries
 * Purpose: Generate query history with multiple joins
 *****************************************************/

-- Query 7: Complete order details
SELECT 
    o.order_id,
    o.order_date,
    c.customer_name,
    c.email,
    p.product_name,
    p.category,
    oi.quantity,
    oi.item_price,
    (oi.quantity * p.price) AS line_total
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
ORDER BY o.order_date, o.order_id;

-- Query 8: Customer lifetime value analysis
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT oi.product_id) AS unique_products,
    SUM(oi.quantity) AS total_items,
    SUM(oi.item_price) AS total_revenue,
    AVG(o.order_amount) AS avg_order_value,
    MAX(o.order_date) AS last_order_date,
    DATEDIFF(day, c.created_date, CURRENT_DATE()) AS days_as_customer
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.customer_name, c.email, c.created_date
ORDER BY total_revenue DESC;

/*****************************************************
 * QUERY SET 4: Window Functions
 * Purpose: Demonstrate advanced analytical queries
 *****************************************************/

-- Query 9: Running total of revenue
SELECT 
    order_date,
    order_id,
    order_amount,
    SUM(order_amount) OVER (ORDER BY order_date) AS running_total,
    AVG(order_amount) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3
FROM orders
ORDER BY order_date;

-- Query 10: Customer ranking by spend
SELECT 
    c.customer_name,
    SUM(o.order_amount) AS total_spent,
    RANK() OVER (ORDER BY SUM(o.order_amount) DESC) AS spending_rank,
    PERCENT_RANK() OVER (ORDER BY SUM(o.order_amount) DESC) AS spending_percentile
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name
ORDER BY spending_rank;

/*****************************************************
 * QUERY SET 5: Subqueries and CTEs
 * Purpose: Generate query history with complex structures
 *****************************************************/

-- Query 11: Customers above average order value (subquery)
SELECT 
    c.customer_name,
    c.email,
    AVG(o.order_amount) AS avg_order_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.email
HAVING AVG(o.order_amount) > (
    SELECT AVG(order_amount) FROM orders
)
ORDER BY avg_order_value DESC;

-- Query 12: Product performance analysis (CTE)
WITH product_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.price,
        SUM(oi.quantity) AS units_sold,
        SUM(oi.item_price) AS revenue
    FROM products p
    LEFT JOIN order_items oi ON p.product_id = oi.product_id
    GROUP BY p.product_id, p.product_name, p.category, p.price
),
category_avg AS (
    SELECT 
        category,
        AVG(revenue) AS avg_category_revenue
    FROM product_sales
    GROUP BY category
)
SELECT 
    ps.product_name,
    ps.category,
    ps.units_sold,
    ps.revenue,
    ca.avg_category_revenue,
    CASE 
        WHEN ps.revenue > ca.avg_category_revenue THEN 'Above Average'
        WHEN ps.revenue = ca.avg_category_revenue THEN 'Average'
        ELSE 'Below Average'
    END AS performance
FROM product_sales ps
JOIN category_avg ca ON ps.category = ca.category
ORDER BY ps.category, ps.revenue DESC;

/*****************************************************
 * QUERY SET 6: Intentionally Inefficient Queries
 * Purpose: Create queries that will show in performance analysis
 *****************************************************/

-- Query 13: Cross join (intentionally inefficient)
-- This will show up as a performance issue in Query Profile
SELECT 
    c.customer_name,
    p.product_name
FROM customers c
CROSS JOIN products p
WHERE c.customer_id < 3;  -- Limit to prevent excessive output

-- Query 14: Query with repetitive calculations
SELECT 
    order_id,
    order_amount,
    order_amount * 1.10 AS with_10_pct_tax,
    order_amount * 1.10 * 0.95 AS after_discount,
    order_amount * 1.10 * 0.95 * 1.05 AS with_shipping,
    CASE 
        WHEN order_amount * 1.10 * 0.95 * 1.05 > 200 THEN 'High Value'
        WHEN order_amount * 1.10 * 0.95 * 1.05 > 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS order_tier
FROM orders;

/*****************************************************
 * QUERY SET 7: Create a View for Reusable Analysis
 * Purpose: Demonstrate view creation in query history
 *****************************************************/

-- Query 15: Create a view for customer analytics
CREATE OR REPLACE VIEW customer_analytics AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    c.created_date,
    COUNT(DISTINCT o.order_id) AS lifetime_orders,
    COALESCE(SUM(o.order_amount), 0) AS lifetime_value,
    COALESCE(AVG(o.order_amount), 0) AS avg_order_value,
    MAX(o.order_date) AS last_order_date,
    DATEDIFF(day, MAX(o.order_date), CURRENT_DATE()) AS days_since_last_order,
    CASE 
        WHEN COUNT(o.order_id) >= 3 THEN 'Loyal'
        WHEN COUNT(o.order_id) = 2 THEN 'Repeat'
        WHEN COUNT(o.order_id) = 1 THEN 'New'
        ELSE 'Inactive'
    END AS customer_segment
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.email, c.created_date;

-- Query 16: Query the view
SELECT * FROM customer_analytics ORDER BY lifetime_value DESC;

/*****************************************************
 * QUERY SET 8: Generate a Failed Query
 * Purpose: Demonstrate error handling in query history
 *****************************************************/

-- Query 17: Intentional error - referencing non-existent column
-- This will fail and appear in query history as failed
-- Uncomment to execute:
-- SELECT customer_id, non_existent_column FROM customers;

-- Query 18: Intentional error - division by zero
-- Uncomment to execute:
-- SELECT order_id, order_amount, order_amount / 0 AS invalid_calc FROM orders;

/*****************************************************
 * VIEWING QUERY HISTORY IN SNOWSIGHT
 *****************************************************
 * 
 * After executing this script:
 * 
 * 1. Navigate to Monitoring » Query History
 * 2. Review the "Individual Queries" tab
 * 3. Click on any query to see three main tabs:
 *    - Query Details: Status, duration, SQL text, results
 *    - Query Profile: Visual execution plan
 *    - Query Telemetry: Related traces and logs
 * 
 * 4. Switch to "Grouped Queries" tab
 * 5. Observe similar queries grouped together
 * 
 * Key analysis activities:
 * - Identify slow-running queries
 * - Find queries with high resource usage
 * - Analyze query patterns
 * - Optimize problematic queries using Query Profile
 * - Compare performance across similar queries
 *****************************************************/

-- Step 4: Query the QUERY_HISTORY view
-- This demonstrates accessing query history programmatically
SELECT 
    query_id,
    query_text,
    user_name,
    role_name,
    warehouse_name,
    warehouse_size,
    execution_status,
    start_time,
    end_time,
    total_elapsed_time / 1000 AS execution_seconds,
    rows_produced,
    bytes_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE user_name = CURRENT_USER()
  AND start_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY start_time DESC
LIMIT 20;

-- Step 5: Analyze query performance trends
SELECT 
    warehouse_name,
    execution_status,
    COUNT(*) AS query_count,
    AVG(total_elapsed_time) / 1000 AS avg_duration_seconds,
    SUM(bytes_scanned) / POWER(1024, 3) AS total_gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE user_name = CURRENT_USER()
  AND start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
GROUP BY warehouse_name, execution_status
ORDER BY query_count DESC;

/*****************************************************
 * QUERY PROFILE ANALYSIS TIPS:
 * 
 * When analyzing Query Profile, look for:
 * 
 * 1. High-cost operators (TableScan, Join)
 * 2. Large data movements between nodes
 * 3. Partition pruning effectiveness
 * 4. Join strategies and spilling to disk
 * 5. Filter selectivity
 * 
 * Optimization opportunities:
 * - Add clustering keys for large tables
 * - Rewrite inefficient joins
 * - Optimize filter predicates
 * - Consider materialized views
 * - Adjust warehouse size if needed
 *****************************************************/

/*****************************************************
 * END OF PHASE 4
 * 
 * Next Step: Proceed to Phase 5 - Copy History
 * Script: code/05_copy_history/setup_data_loading.sql
 *****************************************************/

