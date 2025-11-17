## 📚 Lab Instructions

### Overview of Execution Order

This lab is designed to be executed in sequential order. Each phase builds upon the previous one:

- [Phase 1:  Enable telemetry collection (foundational setup)](README.md#phase-1-enable-telemetry)
- [Phase 2: Create and Explore traces](README.md#phase-2-explore-traces)
- [Phase 3: Generate and Analyze logs](README.md#phase-3-analyze-logs)
- [Phase 4: Run queries and Explore Query History](README.md#phase-4-query-history)
- [Phase 5: Set up Data Loading and Monitor Copy History](README.md#phase-5-copy-history)
- [Phase 6: Create Tasks and Monitor Task History](README.md#phase-6-task-history)
- [Phase 7: Create Dynamic Tables and Monitor Refreshes](README.md#phase-7-dynamic-tables)
- [Phase 8: Explore AI Observability](README.md#phase-8-ai-observability-optional)

---

## Phase 1: Enable Telemetry

### Objective
Enable telemetry collection at the account level to start capturing logs, metrics, and traces.

### Understanding Observability Data Sources

Observability in Snowflake comes in two main categories:

**System Views** provide historical data about your Snowflake account through views and table functions:
- **Information Schema** (`INFORMATION_SCHEMA`) in every Snowflake database
- **Account Usage** (`ACCOUNT_USAGE` and `READER_ACCOUNT_USAGE`) in the Snowflake database
- **Organization Usage** (`ORGANIZATION_USAGE`) in the Snowflake database

**Telemetry data** is delivered exclusively through event tables:
- An event table is a special database table with predefined columns following the OpenTelemetry data model
- By default, Snowflake includes `SNOWFLAKE.TELEMETRY.EVENTS` as a predefined event table
- Telemetry data requires explicit enablement by setting levels for logging, metrics, and tracing

### Script to Execute

**File:** [ENABLE_TELEMETRY.SQL](/code/01_setup/enable_telemetry.sql)

### What This Script Does

This script enables telemetry collection at the account level by setting the appropriate levels for:
- **LOG_LEVEL:** Set to `INFO` to capture informational messages and above
- **METRIC_LEVEL:** Set to `ALL` to capture all available metrics
- **TRACE_LEVEL:** Set to `ALWAYS` to capture all trace data

### Alternative: Enable via Snowsight UI

You can also enable telemetry through the Snowsight interface:
1. Sign in to Snowsight
2. Navigate to **Monitoring** » **Traces and Logs**
3. On the Traces & Logs page, select **Set Event Level**
4. For "Set logging & tracing for", ensure **Account** is selected
5. Set your desired levels:
   - For **All Events**, select **On**
   - For **Logs**, select **INFO**
   - Ensure all other fields show as **On**
6. Click **Save**

<img src="/images/EventLevel.png" width="70%">

### Valid Telemetry Levels

| Parameter | Valid Values | Default Value |
|-----------|-------------|---------------|
| `LOG_LEVEL` | TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF | OFF |
| `METRIC_LEVEL` | ALL, NONE | NONE |
| `TRACE_LEVEL` | ALWAYS, ON_EVENT, OFF | OFF |

### Documentation References
- [Setting levels for logging, metrics, and tracing](https://docs.snowflake.com/en/developer-guide/logging-tracing/logging-tracing-overview)
- [How Snowflake determines the level in effect](https://docs.snowflake.com/en/developer-guide/logging-tracing/logging-tracing-levels)

---

## Phase 2: Explore Traces

### Objective
Understand how traces provide end-to-end visibility into execution flows and help identify performance bottlenecks.

### What Are Traces?

A **trace** represents the complete execution path of a request through your Snowflake workloads. Each trace is made up of one or more **spans**, where each span represents a single operation within the trace (like a SQL query, UDF execution, or procedure call).

**Why Traces Are Useful:**
- Understand end-to-end execution flow of complex operations
- Identify performance bottlenecks and slow operations
- Debug issues by seeing the exact sequence of operations
- Optimize query and procedure performance
- Monitor dependencies between different components

### Important Note

Simple DML/DQL SQL commands executed directly in a worksheet do not generate traces. **Traces are only generated when SQL is executed within supported handler code** (stored procedures, UDFs, Streamlit apps).

### Script to Execute

**File:** [EXPLORE TRACES.SQL](/code/02_traces/create_trace.sql)

### What This Script Does

This script creates:
1. A sample stored procedure that performs multiple operations
2. Executes the procedure to generate trace data
3. Provides instructions for viewing traces in Snowsight

### Viewing Traces in Snowsight

After executing the script:
1. Navigate to **Monitoring** » **Traces and Logs**
2. (Optional) Use filters to narrow down results
3. Click on any trace to view its spans in detail

<img src="/images/Traces1.png" width="70%">

**Trace Explorer Interface** shows:
- Date, Duration, Trace Name, Status
- Number of Spans in the trace
- Timeline visualization of all spans

<img src="/images/Traces2.png" width="70%">

**Span Details** include four tabs:
- **Details:** Info and attributes about the span (Trace ID, Span ID, Duration, Type, Warehouse)
- **Span Events:** Details of events recorded within the span
- **Related Metrics:** CPU and memory metrics related to the span
- **Logs:** Logs directly related to the trace
<img src="/images/Traces3.png" width="70%">

### Documentation References
- [Getting Started with Traces Quickstart](https://quickstarts.snowflake.com/guide/getting_started_with_traces/)
- [Viewing trace data](https://docs.snowflake.com/en/developer-guide/logging-tracing/tracing-viewing)
- [Trace events for functions and procedures](https://docs.snowflake.com/en/developer-guide/logging-tracing/tracing-procedures-udfs)
-[Adding custom spans to a trace](https://docs.snowflake.com/en/developer-guide/logging-tracing/tracing-custom-spans?_ga=2.189991725.700748310.1762956177-721365219.1748449960&_gac=1.83686756.1760630245.Cj0KCQjw3OjGBhDYARIsADd-uX48RT1ZuMoVV61EmTXTmhHaFi1o9WzaizxO77uOO6rwopK0dkH-nFkaAhvFEALw_wcB&_fsi=FSuddBv0) 

---

## Phase 3: Analyze Logs

### Objective
Learn how to generate and analyze logs for debugging issues and monitoring application behavior.

### What Are Logs?

**Logs** are structured records of events that occur during the execution of your Snowflake workloads. They provide detailed information about what happened during code execution, including informational messages, warnings, errors, and debug information.

**Why Logs Are Useful:**
- Debug issues by providing detailed error messages and stack traces
- Monitor application behavior and performance
- Audit operations and track important events
- Understand the flow of execution in complex procedures
- Identify patterns in application usage or errors

### Script to Execute

**File:** [ANALYZE LOGS.SQL](/code/03_logs/create_log.sql)

### What This Script Does

This script creates:
1. A Python UDF that generates log messages at different severity levels
2. A stored procedure that demonstrates logging from handler code
3. Executes the functions to generate log data

### Viewing Logs in Snowsight

After executing the script:
1. Navigate to **Monitoring** » **Traces & Logs**
2. Click on the **Logs** tab
3. (Optional) Use filters to find specific logs:
   - **Time Range:** Set by drop-down or clicking on the graph
   - **Severity:** Select specific log levels (DEBUG, INFO, WARN, ERROR)
   - **Languages:** Filter by handler code language (Python, Java, etc.)
   - **Database:** Filter by specific procedures, functions, or applications
   - **Record:** Select Logs, Events, or All

**Log Details:**
- By default, logs are sorted by timestamp
<img src="/images/logs1.png" width="70%">

- Click on any log entry to see full details including complete log text
<img src="/images/logs2.png" width="70%">

### Documentation References
- [Getting Started with Logging Quickstart](https://quickstarts.snowflake.com/guide/getting_started_with_logging/)
- [Logging messages from handler code](https://docs.snowflake.com/en/developer-guide/logging-tracing/logging?_ga=2.223019549.700748310.1762956177-721365219.1748449960&_gac=1.26721359.1760630245.Cj0KCQjw3OjGBhDYARIsADd-uX48RT1ZuMoVV61EmTXTmhHaFi1o9WzaizxO77uOO6rwopK0dkH-nFkaAhvFEALw_wcB&_fsi=FSuddBv0#label-logging-handler-code)
- [Viewing log messages](https://docs.snowflake.com/en/developer-guide/logging-tracing/logging-accessing-messages?_ga=2.123561741.700748310.1762956177-721365219.1748449960&_gac=1.93723119.1760630245.Cj0KCQjw3OjGBhDYARIsADd-uX48RT1ZuMoVV61EmTXTmhHaFi1o9WzaizxO77uOO6rwopK0dkH-nFkaAhvFEALw_wcB)

---

## Phase 4: Query History

### Objective
Explore Query History to monitor query performance, identify slow queries, and optimize resource usage.

### What Is Query History?

**Query History** provides a comprehensive view of all SQL queries executed in your Snowflake account. It's one of the most important tools for monitoring, troubleshooting, and optimizing database performance.

**Why Query History Is Useful:**
- Monitor query performance and identify slow-running queries
- Analyze resource usage and warehouse utilization
- Troubleshoot failed queries and understand error patterns
- Track query execution trends over time
- Optimize queries by understanding execution patterns
- Audit database activity and user behavior

### Script to Execute

**File:** [QUERY HISTORY.SQL](/code/04_query_history/sample_queries.sql)

### What This Script Does

This script runs various types of queries to populate query history:
1. Simple SELECT queries
2. Complex queries with joins and aggregations
3. Queries with different performance characteristics
4. Intentionally slow queries for demonstration

### Viewing Query History in Snowsight

#### Individual Queries View

1. Navigate to **Monitoring** » **Query History**
2. (Optional) Use filters:
   - **Status:** Filter by execution status (Success, Failed, etc.)
   - **User:** Filter by specific users
   - **Time Range:** Filter by execution time
   - **Filters:** Various other filters to find specific queries

   <img src="/images/QH1.png" width="70%">

**Query Details Tab** shows:
- Query status, duration, ID
- SQL text of the query
- Query results (for successful queries)

By default, the query text for failed queries is redacted. You can change this behavior by following the following KB article: [SQL text is showing redacted for failed queries.](https://community.snowflake.com/s/article/SQL-text-is-showing-redacted-for-failed-queries?_ga=2.132450184.700748310.1762956177-721365219.1748449960&_gac=1.58774879.1760630245.Cj0KCQjw3OjGBhDYARIsADd-uX48RT1ZuMoVV61EmTXTmhHaFi1o9WzaizxO77uOO6rwopK0dkH-nFkaAhvFEALw_wcB)

<img src="/images/QH_details.png" width="70%">

**Query Profile Tab** provides:
- Visual representation of query execution
- Critical details for debugging and optimizing
- Percentage of time spent in each step
- Data movements between nodes

<img src="/images/QP.png" width="70%">

For a list of all possible fields, see the documentation [here.](https://docs.snowflake.com/en/user-guide/ui-snowsight-activity?&_ga=2.233115288.700748310.1762956177-721365219.1748449960&_gac=1.19789130.1760630245.Cj0KCQjw3OjGBhDYARIsADd-uX48RT1ZuMoVV61EmTXTmhHaFi1o9WzaizxO77uOO6rwopK0dkH-nFkaAhvFEALw_wcB#query-profile-reference)
The Query Profile is essential for query optimization. Look for:

- Steps with the highest percentage of total time
- Large data movements between nodes
- Inefficient join strategies
- Missing or unused indexes

**Query Telemetry Tab** shows:
- Same telemetry data as Trace Explorer
- Logs and events related to the query

#### Grouped Query History

1. Navigate to **Monitoring** » **Query History**
2. Click on the **Grouped Queries** tab

**Grouped Query History** helps you:
- Identify the most frequently executed query patterns
- See aggregate performance metrics across similar queries
- Spot trends in query execution patterns
- Focus optimization efforts on the most impactful queries

<img src="/images/GQ.png" width="70%">

By clicking into a single grouped query, you can see detailed information about execution count, duration, and more.

<img src="/images/GQ1.png" width="70%">

### Documentation References
- [Monitor query activity with Query History](https://docs.snowflake.com/en/user-guide/ui-snowsight-activity#query-history)
- [Query profiling](https://docs.snowflake.com/en/user-guide/ui-query-profile)
- [QUERY_HISTORY view](https://docs.snowflake.com/en/sql-reference/account-usage/query_history)

---

## Phase 5: Copy History

### Objective
Monitor data loading activities and track the performance of COPY INTO operations.

### What Is Copy History?

**Copy History** provides comprehensive monitoring for all data loading activities in your Snowflake account. It tracks operations from COPY INTO commands, Snowpipe, and Snowpipe Streaming.

**Why Copy History Is Useful:**
- Monitor data loading performance and identify bottlenecks
- Track successful and failed data loading operations
- Analyze data ingestion patterns and volume trends
- Troubleshoot data loading errors and validation issues
- Optimize data loading strategies and warehouse sizing
- Audit data ingestion activities across your organization

### Script to Execute

**File:** [COPY HISTORY.SQL](/code/05_copy_history/setup_data_loading.sql)
### What This Script Does

This script:
1. Creates a sample table for data loading
2. Creates an internal stage
3. Generates sample data
4. Executes COPY INTO commands to load data
5. Demonstrates both successful and error scenarios

### Viewing Copy History in Snowsight

1. Navigate to **Ingestion** » **Copy History**
2. (Optional) Use filters to narrow down activity by:
   - Status
   - Database
   - Pipe (for Snowpipe operations)
   - Time range

<img src="/images/CH1.png" width="70%">

**Copy Operations View** shows:
- Status of each operation
- Target table
- Pipe name (if applicable)
- Data size loaded
- Number of rows loaded

**Detailed View:**
- Click on any operation to see individual file details
- View error messages for failed operations
- See detailed statistics for each file

<img src="/images/CH2.png" width="70%">

### Documentation References
- [Monitor data loading activity using Copy History](https://docs.snowflake.com/en/user-guide/data-load-monitor?_ga=2.220480030.700748310.1762956177-721365219.1748449960&_gac=1.184547156.1760630245.Cj0KCQjw3OjGBhDYARIsADd-uX48RT1ZuMoVV61EmTXTmhHaFi1o9WzaizxO77uOO6rwopK0dkH-nFkaAhvFEALw_wcB)
- [Getting Started with Snowpipe Quickstart](https://www.snowflake.com/en/developers/guides/getting-started-with-snowpipe/)

---

## Phase 6: Task History

### Objective
Create and monitor automated tasks and understand task execution patterns.

### What Is Task History?

**Task History** provides monitoring and observability for Snowflake Tasks, which are scheduled SQL statements or procedures that run automatically. Tasks are essential for building data pipelines, ETL processes, and automated maintenance operations.

**Why Task History Is Useful:**
- Monitor task execution success and failure rates
- Track task performance and execution duration trends
- Troubleshoot failed tasks and identify error patterns
- Analyze task scheduling and dependency execution
- Optimize task graphs and pipeline performance
- Audit automated operations across your data pipelines

### Script to Execute

**File:** [TASK HISTORY.SQL](/code/06_task_history/create_tasks.sql)

### What This Script Does

This script:
1. Creates a simple standalone task
2. Creates a task graph with parent-child dependencies
3. Executes tasks to generate history
4. Demonstrates task monitoring

### Viewing Task History in Snowsight

1. Navigate to **Transformation** » **Tasks**
2. (Optional) Use filters by status, database, and more

#### Task Graphs View

The **Task Graphs** view groups related tasks in a directed acyclic graph (DAG) showing:
- Root task name
- Schedule
- Recent run history
- Dependencies between parent and child tasks

<img src="/images/TH1.png" width="70%">

Click on any task execution to see:
- Child task names
- Status and duration
- Execution order
<img src="/images/TH2.png" width="70%">

#### Task Runs View

The **Task Runs** view shows individual task executions without grouping.
Click on any task run to view detailed run history.
<img src="/images/TH3.png" width="70%">

### Documentation References
- [Getting Started with Streams & Tasks Quickstart](https://quickstarts.snowflake.com/guide/getting_started_with_streams_and_tasks/)
- [Getting Started with Snowflake Task Graphs Quickstart](https://www.snowflake.com/en/developers/guides/getting-started-with-task-graphs/)
- [Introduction to tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)
- [Monitoring task runs](https://docs.snowflake.com/en/user-guide/tasks-monitor?_ga=2.161697466.700748310.1762956177-721365219.1748449960&_gac=1.82236132.1760630245.Cj0KCQjw3OjGBhDYARIsADd-uX48RT1ZuMoVV61EmTXTmhHaFi1o9WzaizxO77uOO6rwopK0dkH-nFkaAhvFEALw_wcB&_fsi=FSuddBv0)

---

## Phase 7: Dynamic Tables

### Objective
Create and monitor dynamic tables to understand automatic materialization and refresh patterns.

### What Are Dynamic Tables?

**Dynamic Tables** are a table type that automatically materializes the results of a query and keeps them updated as the underlying data changes. They combine the simplicity of views with the performance of materialized data.

**Why Dynamic Tables Monitoring Is Useful:**
- Track refresh performance and identify optimization opportunities
- Monitor data freshness and lag times
- Analyze resource consumption for materialized views
- Troubleshoot refresh failures and dependency issues
- Optimize refresh strategies and warehouse sizing
- Ensure data pipeline reliability and performance

### Script to Execute

**File:** [DYNAMIC TABLES.SQL](/code/07_dynamic_tables/create_dynamic_table.sql)

### What This Script Does

This script:
1. Creates a base table with sample data
2. Creates a dynamic table that materializes aggregated results
3. Inserts data to trigger refreshes
4. Demonstrates monitoring of dynamic table refreshes

### Viewing Dynamic Tables in Snowsight

1. Navigate to **Transformation** » **Dynamic Tables**
2. (Optional) Use filters by refresh status and database

**Dynamic Table View** shows:
- Status
- Target lag
- Database and schema
- Last refresh time
- Next scheduled refresh

<img src="/images/DT1.png" width="70%">

**Graph View:**
- Click on any table to see the refresh history graph
- View refresh duration over time
- Monitor lag trends

<img src="/images/DT2.png" width="70%">

### Documentation References
- [Getting Started with Snowflake Dynamic Tables Quickstart](https://quickstarts.snowflake.com/guide/getting_started_with_dynamic_tables/)
- [Dynamic tables overview](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Monitor dynamic tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-monitor?_ga=2.190598829.700748310.1762956177-721365219.1748449960&_gac=1.191182296.1760630245.Cj0KCQjw3OjGBhDYARIsADd-uX48RT1ZuMoVV61EmTXTmhHaFi1o9WzaizxO77uOO6rwopK0dkH-nFkaAhvFEALw_wcB&_fsi=FSuddBv0)

---

## Phase 8: AI Observability (Optional)

### Objective
Explore AI observability features for monitoring AI/ML workloads and Cortex AI functions.

### What Is AI Observability?

**AI Observability** in Snowflake provides monitoring and insights for AI/ML workloads, including Cortex AI functions and model inference operations.

**Key Features:**
- **Evaluations:** Systematically evaluate generative AI applications using LLM-as-a-judge technique
- **Comparison:** Compare multiple evaluations side by side across different LLMs, prompts, and configurations
- **Tracing:** Trace every step of application execution including input prompts, retrieved context, tool use, and LLM inference

**Why AI Observability Is Useful:**
- Monitor AI/ML model performance and accuracy
- Track inference latency and throughput
- Analyze cost of AI operations
- Debug AI application issues
- Optimize model selection and configuration
- Ensure AI reliability and quality

### Documentation References
- [Getting Started with AI Observability Quickstart](https://quickstarts.snowflake.com/guide/getting_started_with_ai_observability/)
- [Getting Started with ML Observability Quickstart](https://quickstarts.snowflake.com/guide/getting_started_with_ml_observability/)
- [AI Observability in Snowflake Cortex](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-observability)

---