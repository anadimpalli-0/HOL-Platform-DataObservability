# Script Execution Order

This document provides a clear execution sequence for all scripts in the Observability HOL.

---

## 📋 Complete Execution Sequence

Execute these scripts in the exact order listed below:

### Phase 1: Enable Telemetry (REQUIRED FIRST)
**File:** `code/01_setup/enable_telemetry.sql`  
**Time:** ~2 minutes  
**Description:**  
- Enables logging at INFO level
- Enables metrics at ALL level
- Enables tracing at ALWAYS level
- Verifies telemetry configuration

**Key Actions:**
```sql
ALTER ACCOUNT SET LOG_LEVEL = 'INFO';
ALTER ACCOUNT SET METRIC_LEVEL = 'ALL';
ALTER ACCOUNT SET TRACE_LEVEL = 'ALWAYS';
```

**After Execution:** Verify settings with `SHOW PARAMETERS LIKE '%LEVEL' IN ACCOUNT;`

---

### Phase 2: Explore Traces
**File:** `code/02_traces/create_trace_examples.sql`  
**Time:** ~5 minutes  
**Description:**  
- Creates database OBSERVABILITY_HOL_DB
- Creates warehouse OBSERVABILITY_WH
- Creates sample tables (customers, orders)
- Creates stored procedures that generate traces
- Creates Python UDFs with trace data

**Key Objects Created:**
- Database: `OBSERVABILITY_HOL_DB`
- Schema: `TRACES_SCHEMA`
- Warehouse: `OBSERVABILITY_WH`
- Procedures: `analyze_customer_orders()`, `process_monthly_report()`
- Function: `calculate_customer_lifetime_value()`

**After Execution:** Navigate to **Monitoring** » **Traces and Logs** to view generated traces

---

### Phase 3: Analyze Logs
**File:** `code/03_logs/create_log_examples.sql`  
**Time:** ~5 minutes  
**Description:**  
- Creates schema LOGS_SCHEMA
- Creates Python UDFs with logging
- Creates procedures with comprehensive logging
- Demonstrates different log levels (INFO, WARN, ERROR)

**Key Objects Created:**
- Schema: `LOGS_SCHEMA`
- Function: `validate_email()`
- Procedures: `process_customer_data()`, `demonstrate_log_levels()`, `safe_divide()`, `analyze_order_patterns()`

**After Execution:** Navigate to **Monitoring** » **Traces & Logs** » **Logs tab** to view log messages

---

### Phase 4: Query History
**File:** `code/04_query_history/sample_queries.sql`  
**Time:** ~5 minutes  
**Description:**  
- Creates additional tables (products, order_items)
- Runs various query types (simple, complex, joins, window functions)
- Creates intentionally inefficient queries for analysis
- Creates views for reusable queries

**Key Objects Created:**
- Tables: `products`, `order_items`
- View: `customer_analytics`
- Multiple queries of varying complexity

**After Execution:** Navigate to **Monitoring** » **Query History** to analyze query performance

---

### Phase 5: Copy History
**File:** `code/05_copy_history/setup_data_loading.sql`  
**Time:** ~5 minutes  
**Description:**  
- Creates schema COPY_SCHEMA
- Creates tables for data loading
- Creates internal stage and file format
- Executes COPY INTO operations
- Demonstrates error handling in data loading

**Key Objects Created:**
- Schema: `COPY_SCHEMA`
- Tables: `sales_data`, `customer_feedback`, `validated_sales`, `incremental_sales`, `strict_sales`
- Stage: `data_stage`
- File Format: `csv_format`

**After Execution:** Navigate to **Ingestion** » **Copy History** to monitor data loading operations

---

### Phase 6: Task History
**File:** `code/06_task_history/create_tasks.sql`  
**Time:** ~10 minutes (includes task execution wait time)  
**Description:**  
- Creates schema TASK_SCHEMA
- Creates standalone tasks
- Creates task graph with parent-child dependencies
- Enables and executes tasks
- Demonstrates task monitoring

**Key Objects Created:**
- Schema: `TASK_SCHEMA`
- Tables: `task_execution_log`, `daily_sales_summary`, `sales_metrics`
- Standalone Tasks: `simple_logging_task`, `daily_summary_task`, `regional_metrics_task`
- Task Graph: `root_validate_data` → `child_transform_data` & `child_calculate_kpis` → `grandchild_final_aggregation`

**After Execution:** Navigate to **Transformation** » **Tasks** to view task execution history

**IMPORTANT:** Remember to suspend tasks after testing to avoid ongoing costs!

---

### Phase 7: Dynamic Tables
**File:** `code/07_dynamic_tables/create_dynamic_table.sql`  
**Time:** ~5 minutes  
**Description:**  
- Creates schema DYNAMIC_TABLES_SCHEMA
- Creates base tables with sample data
- Creates multiple dynamic tables with different target lags
- Demonstrates cascading dynamic tables
- Inserts data to trigger refreshes

**Key Objects Created:**
- Schema: `DYNAMIC_TABLES_SCHEMA`
- Base Tables: `sales_transactions`, `customers_dim`
- Dynamic Tables: `sales_summary`, `regional_sales_analysis`, `customer_sales_summary`, `top_regions`, `payment_method_summary`

**After Execution:** Navigate to **Transformation** » **Dynamic Tables** to monitor refresh patterns

---

### Phase 8: AI Observability (OPTIONAL)
**Status:** Documentation only - requires AI/ML workloads  
**Description:**  
- Concepts covered in main README.md
- Requires Cortex AI functions or ML models
- Not included as executable script in this lab

**Resources:**
- [Getting Started with AI Observability Quickstart](https://quickstarts.snowflake.com/guide/getting_started_with_ai_observability/)
- [AI Observability Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-observability)

---

### Cleanup: Remove All Objects
**File:** `code/99_cleanup/cleanup.sql`  
**Time:** ~2 minutes  
**Description:**  
- Suspends and drops all tasks
- Drops all dynamic tables
- Drops entire database (CASCADE)
- Drops warehouse
- Optionally resets telemetry settings

**Objects Removed:**
- All tasks in TASK_SCHEMA
- All dynamic tables
- Database: `OBSERVABILITY_HOL_DB` (and all schemas within)
- Warehouse: `OBSERVABILITY_WH`

**After Execution:** Verify cleanup with `SHOW DATABASES;` and `SHOW WAREHOUSES;`

---

## 🔄 Execution Flow Diagram

```
Start
  ↓
Phase 1: Enable Telemetry (REQUIRED)
  ↓
Phase 2: Create Traces (creates DB & WH)
  ↓
Phase 3: Create Logs
  ↓
Phase 4: Run Queries → Explore in Query History
  ↓
Phase 5: Load Data → Explore in Copy History
  ↓
Phase 6: Create Tasks → Explore in Task History
  ↓
Phase 7: Create Dynamic Tables → Explore in Dynamic Tables
  ↓
Phase 8: (Optional) AI Observability Concepts
  ↓
Cleanup: Remove All Objects
  ↓
End
```

---

## ⚠️ Important Notes

1. **Must execute in order** - Each phase depends on previous phases
2. **Phase 1 is mandatory** - Telemetry must be enabled first
3. **Phase 2 creates core objects** - Database and warehouse used throughout
4. **Don't skip phases** - Later phases reference objects from earlier phases
5. **Run cleanup when done** - Prevents ongoing compute costs
6. **Use ACCOUNTADMIN role** - Required for most operations

---

## 📊 Expected Resource Usage

| Resource | Type | Size | Auto-Suspend | Cost Impact |
|----------|------|------|--------------|-------------|
| OBSERVABILITY_WH | Warehouse | X-SMALL | 60 seconds | Minimal (~$1-2) |
| OBSERVABILITY_HOL_DB | Database | N/A | N/A | Storage only |
| Tasks (7 total) | Compute | Uses WH | N/A | Only when running |
| Dynamic Tables (5) | Compute | Uses WH | N/A | Refresh cycles |

**Total Estimated Cost:** $1-3 in Snowflake credits for complete lab execution

---

## 🎯 Success Criteria

After completing all phases, you should be able to:

- ✅ View traces in Snowsight Monitoring
- ✅ See log messages with different severity levels
- ✅ Analyze query performance using Query Profile
- ✅ Monitor data loading operations
- ✅ Track task execution patterns
- ✅ Observe dynamic table refreshes
- ✅ Query telemetry data directly from event tables

---

## 📖 Additional Resources

- **Main README:** `README.md`
- **Quick Start:** `lab_instructions/quick_start.md`
- **FAQ:** `troubleshooting/faq.md`
- **Snowflake Documentation:** [docs.snowflake.com](https://docs.snowflake.com)

---

**Ready to begin?** Start with Phase 1: `code/01_setup/enable_telemetry.sql`

Good luck! 🚀

