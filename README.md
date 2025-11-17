# College of Platform - Snowflake Trail:Observability HOL
**Snowflake's suite of observability capabilities that enable its users to better monitor, troubleshoot, debug and take actions on pipelines, apps, user code and compute utilizations.**

Overview
<img src="/images/SnoflakeTrail.png" width="70%">

---

## 🛠️ Hands-On Lab Overview

In this hands-on lab, you'll step into the shoes of a **Data Engineer or Platform Administrator** 
tasked with **implementing comprehensive observability across Snowflake workloads using Snowflake Trail**.

### 📋 What You'll Do:

This lab will guide you through the complete observability stack in Snowflake, helping you monitor, troubleshoot, debug, and take action on pipelines, applications, user code, and compute utilization.

- **Task 1:** Enable **telemetry collection** (logs, metrics, traces) at the account level and understand the difference between system views and telemetry data.
- **Task 2:** Explore **traces** to understand end-to-end execution flows, identify performance bottlenecks, and debug complex operations.
- **Task 3:** Analyze **logs** to debug issues, monitor application behavior, and track important events in your workloads.
- **Task 4:** Leverage **Query History** to optimize query performance, analyze resource usage, and troubleshoot database operations.
- **Task 5:** Monitor **Copy History** for data loading activities from COPY INTO, Snowpipe, and Snowpipe Streaming operations.
- **Task 6:** Track **Task History** to monitor automated operations, task graphs, and pipeline execution patterns.
- **Task 7:** Observe **Dynamic Tables** refresh patterns, data freshness, and materialization performance.
- **Task 8:** Explore **AI Observability** for monitoring AI/ML workloads and Cortex AI functions.

**Objectives**

-Enable and configure telemetry collection in Snowflake to capture logs, metrics, and traces for account-level observability.
-Differentiate between System Views and Telemetry Data, understanding how each supports monitoring, troubleshooting, and analytics.
-Use Snowsight’s monitoring tools—such as Trace Explorer, Query History, and Logs—to diagnose performance issues and optimize workloads.
-Monitor key data operations including ingestion (Copy History), automation (Task History), and materialization (Dynamic Tables) to ensure reliability and efficiency.
-Apply AI and ML observability techniques to evaluate model performance, trace inference workflows, and monitor Cortex AI workloads for accuracy and cost efficiency.

### ⏲️ Estimated Lab Timeline

- **Phase 1 (Setup & telemetry enablement):** ~10 min
- **Phase 2 (Traces & logs exploration):** ~25 min
- **Phase 3 (Query, copy, task history):** ~25 min
- **Phase 4 (Dynamic tables & AI observability):** ~10 min
- **Phase 5 (Cleanup):** ~5 min

**Total estimated time:** ~60-90 minutes

---

## 📖 Table of Contents

- [Why this Matters](#why-this-matters)
- [Suggested Discovery Questions](#suggested-discovery-questions)
- [Repository Structure](#repository-structure)
- [Prerequisites & Setup Details](#prerequisites--setup-details)
- [Lab Instructions](#lab-instructions)
  - [Phase 1: Enable Telemetry](#phase-1-enable-telemetry)
  - [Phase 2: Explore Traces](#phase-2-explore-traces)
  - [Phase 3: Analyze Logs](#phase-3-analyze-logs)
  - [Phase 4: Query History](#phase-4-query-history)
  - [Phase 5: Copy History](#phase-5-copy-history)
  - [Phase 6: Task History](#phase-6-task-history)
  - [Phase 7: Dynamic Tables](#phase-7-dynamic-tables)
  - [Phase 8: AI Observability (Optional)](#phase-8-ai-observability-optional)
- [Placeholder & Naming Conventions](#placeholder--naming-conventions)
- [Troubleshooting & FAQ](#troubleshooting--faq)
- [Cleanup & Cost-Stewardship Procedures](#cleanup--cost-stewardship-procedures)
- [Advanced Concepts](#advanced-concepts)
- [Links to Resources & Documentation](#links-to-resources--documentation)

---

## 📌 Why this Matters

- **Business value:** Snowflake Trail provides comprehensive observability that accelerates troubleshooting time by 60-80%, reduces mean time to resolution (MTTR) for production issues, and enables proactive monitoring before problems impact business operations. Organizations can identify and resolve performance bottlenecks in minutes rather than hours.

- **Pricing impact:** Proper observability helps optimize compute costs by identifying inefficient queries, unused resources, and over-provisioned warehouses. Telemetry data storage in event tables uses standard Snowflake storage pricing. Implementing best practices from this lab can reduce compute costs by 20-40% through better resource utilization and query optimization.

- **Customer stories:** Leading enterprises across financial services, healthcare, and retail are leveraging Snowflake Trail to maintain SLAs, ensure data pipeline reliability, and provide real-time visibility into their data operations. Reference the [Snowflake Customer Stories](https://www.snowflake.com/customers/) for industry-specific use cases.

---

## ❓ Suggested Discovery Questions

Provide **5-6 open-ended questions** for customer conversations related to observability:

- "How are you currently monitoring your data pipelines and identifying performance bottlenecks in your Snowflake environment?"
- "What metrics matter most when evaluating the health and performance of your Snowflake workloads?"
- "Have you faced any challenges troubleshooting failed jobs or understanding the root cause of query performance issues?"
- "How do you currently track and audit data loading activities, task executions, and automated workflows?"
- "What's your process for identifying and optimizing expensive or long-running queries?"
- "Are you running AI/ML workloads in Snowflake, and if so, how do you monitor their performance and cost?"

---

## 📂 Repository Structure

```bash
├── README.md                          # Main entry point (this file)
├── code/                              # SQL scripts for lab execution
│   ├── 01_setup/
│   │   └── enable_telemetry.sql       # Enable logging, metrics, and tracing
│   ├── 02_traces/
│   │   └── create_trace_examples.sql  # Create procedures to generate traces
│   ├── 03_logs/
│   │   └── create_log_examples.sql    # Create functions with logging
│   ├── 04_query_history/
│   │   └── sample_queries.sql         # Sample queries for history analysis
│   ├── 05_copy_history/
│   │   └── setup_data_loading.sql     # Setup for copy operations
│   ├── 06_task_history/
│   │   └── create_tasks.sql           # Create sample tasks
│   ├── 07_dynamic_tables/
│   │   └── create_dynamic_table.sql   # Create dynamic table example
│          
├── images/                            # Diagrams and visual assets
├── lab_instructions/                  # Detailed step-by-step instructions
│   ├── phase1_setup.md
│   ├── phase2_traces.md
│   ├── phase3_logs.md
│   ├── phase4_query_history.md
│   ├── phase5_copy_history.md
│   ├── phase6_task_history.md
│   └── phase7_dynamic_tables.md
Grading/                          
│   ├── HOW_TO_GRADE.md                # Grading instructions
│   ├── Doragrading_01_traces.sql
│   ├── DoraGrading_02_logs.sql
│   ├── DoraGrading_03_query_history.sql
│   └── Doragrading_04_copy_history.sql
│   └── Doragrading_05_task_history.sql
│   └── Doragrading_06_dynamic_tables.sql
99_cleanup/
│       └── cleanup.sql       # Cleanup script
└── troubleshooting/                   # Common issues and resolutions
    └── faq.md
```
---

## ✅ Prerequisites & Setup Details

### Knowledge Prerequisites
- Basic familiarity with SQL and Snowflake concepts
- Understanding of database objects (warehouses, databases, schemas, tables)
- Familiarity with Snowsight interface (recommended)

### Account and Entitlement Checks
- **Required Role:** `ACCOUNTADMIN` role or a custom role with privileges to:
  - Set account-level parameters (`ALTER ACCOUNT`)
  - View and query event tables (`SELECT` on `SNOWFLAKE.TELEMETRY.*`)
  - Access monitoring features in Snowsight
  - Create databases, schemas, warehouses, and other objects

---

## ⚠️ Troubleshooting & FAQ

### Common Issues and Resolutions

**Issue:** No traces appearing after enabling tracing  
**Cause:** Simple SQL statements don't generate traces  
**Solution:** Execute stored procedures or UDFs to generate trace data. Traces are only created for handler code execution.

**Issue:** Cannot see telemetry data in event tables  
**Cause:** Telemetry levels not properly set or insufficient privileges  
**Solution:** Verify telemetry levels are set at account level using `SHOW PARAMETERS LIKE '%LEVEL' IN ACCOUNT;` and ensure you have `ACCOUNTADMIN` role or appropriate privileges.

**Issue:** Query history shows redacted SQL text  
**Cause:** Default security setting for failed queries  
**Solution:** Follow [KB article for redacted queries](https://community.snowflake.com/s/article/SQL-text-is-showing-redacted-for-failed-queries).

**Issue:** Event table not receiving data  
**Cause:** Active event table not properly configured  
**Solution:** Verify the active event table using `SHOW PARAMETERS LIKE 'EVENT_TABLE' IN ACCOUNT;` and ensure it points to a valid event table.

**Issue:** Insufficient privileges error  
**Cause:** Current role lacks required permissions  
**Solution:** Switch to `ACCOUNTADMIN` role or request necessary privileges from your administrator.

### Internal Support

For internal support and questions:
- Slack Channel: `#snowflake-observability`
- Snowflake Documentation: [docs.snowflake.com](https://docs.snowflake.com)

---
## ⚠️ Grading 
Detailed instruction on Grading instructions can be found here `grading/HOW_TO_GRADE.md`

---

## 🧹 Cleanup & Cost-Stewardship Procedures

### Cleanup Instructions

After completing the lab, it's important to clean up resources to avoid unnecessary costs.

**Script to Execute:** `code/99_cleanup/cleanup.sql`

### What the Cleanup Script Does

The cleanup script removes all objects created during this lab:
- Drops all databases created
- Drops all warehouses created
- Drops all tasks (suspends first, then drops)
- Cleans up all temporary objects

### Manual Verification Steps

After running the cleanup script:
1. Verify databases are removed: `SHOW DATABASES LIKE 'OBSERVABILITY_HOL%';`
2. Verify warehouses are removed: `SHOW WAREHOUSES LIKE 'OBSERVABILITY_WH%';`
3. Verify tasks are removed: `SHOW TASKS LIKE 'TASK%';`

### Cost Considerations

**Compute Costs:**
- Warehouses consume credits only when running
- Suspend warehouses immediately when not in use
- Use auto-suspend settings (recommended: 60-120 seconds)

**Storage Costs:**
- Event tables consume standard Snowflake storage
- Telemetry data retention follows standard time travel settings
- Consider data retention policies for long-term cost management

---

## 📘 Advanced Concepts

Brief callouts to deeper observability topics:

- **Custom Event Tables:** Create dedicated event tables for specific workloads or departments to isolate telemetry data and implement fine-grained access controls.

- **Integration with External Tools:** Connect Snowflake observability data to external monitoring platforms like Grafana, Datadog, or Observe for unified observability across your entire stack.

- **Automated Alerting:** Build event-driven architectures using Snowflake tasks and event tables to trigger automated alerts and remediation workflows based on telemetry data.

- **OpenTelemetry Standard:** Snowflake's event tables follow the OpenTelemetry specification, enabling standardized observability across heterogeneous systems and cloud platforms.

- **Performance Optimization Patterns:** Use trace data to identify anti-patterns like excessive small queries, inefficient UDFs, or suboptimal task scheduling that impact overall system performance.

---

## 🔗 Links to Resources & Documentation

### Core Snowflake Documentation
- [Event tables overview](https://docs.snowflake.com/en/developer-guide/logging-tracing/event-table-overview)
- [Logging, tracing, and metrics](https://docs.snowflake.com/en/developer-guide/logging-tracing/logging-tracing-overview)
- [AI Observability in Snowflake Cortex](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-observability)
- [Getting Started with ML Observability in Snowflake](https://www.snowflake.com/en/developers/guides/getting-started-with-ml-observability-in-snowflake/)
- [Platform Observability - Customer Overview Deck](https://snowflake.seismic.com/app?ContentId=146eb12e-0755-47dc-ace8-e43eb00ba5b3#/doccenter/1bd9d0ff-73e0-4b73-adc8-41c72dada8f3/doc/%252Flfc2b4291a-d17f-42e0-a7b1-18a68dcfcadb/grid/)

---

## 👤 Author & Support --Check and update

**Lab created by:** Snowflake Solutions Engineering – Data Platform Team  
**Created on:** October 2024 | **Last updated:** October 17, 2025

💬 **Need Help or Have Feedback?**  
- Slack Channel: [#snowflake-observability](https://snowflake.slack.com/archives/snowflake-observability)  
- Email: [support@snowflake.com](mailto:support@snowflake.com)

🌟 *We greatly value your feedback to continuously improve our HOL experiences!*

---

## 🎓 Conclusion

Congratulations! You have successfully explored the comprehensive observability capabilities available in Snowflake Trail. By following this lab, you've gained hands-on experience with the key components that make up Snowflake's observability platform.

### What You Learned

Through this lab, you have learned how to:
- ✅ Enable telemetry collection at the account level to capture logs, metrics, and traces
- ✅ Understand the difference between System Views and Telemetry data
- ✅ Utilize traces to gain end-to-end insight into execution flows
- ✅ Analyze logs to debug issues and monitor application behavior
- ✅ Leverage Query History to optimize query performance
- ✅ Monitor data loading activities through Copy History
- ✅ Track automated operations using Task History
- ✅ Observe Dynamic Tables refresh patterns and data freshness
- ✅ Explore AI observability concepts for AI/ML workloads
- ✅ Build a foundational understanding of Snowflake Trail observability

### Next Steps

Now that you've completed the basics, consider:
- Configure alerts and notifications based on telemetry data
- Integrate external observability tools (Grafana, Datadog, Observe)
- Build custom dashboards for specific monitoring needs
- Implement automated monitoring workflows using Tasks
- Explore advanced observability patterns for your workloads

Thank you for completing this Hands-On Lab! 🎉

