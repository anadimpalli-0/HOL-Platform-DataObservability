# How to Complete Lab Grading

This is where you can find the validation scripts you need to run to prove you completed the Snowflake Observability HOL.

Remember to edit your contact information in the SQL Statement for the [SE_GREETER.sql](/config/SE_GREETER.sql)

If all validations return ✅, you have successfully completed Data Observability HOL

## ✅ Grading Steps

After completing each lab phase or the complete lab, run the corresponding DoraGrading script:

### 1. Complete Traces validation:

**Script to Execute:** [DORAGRADING_01_TRACES.sql](/config/Doragrading_01_traces.sql)

### 2. Complete Logs validation:

**Script to Execute:** [DORAGRADING_02_LOGS.sql](/config/Doragrading_02_logs.sql)


### 3. Complete Query History validation

**Script to Execute:** [DORAGRADING_03_QUERYHISTORY.sql](/config/Doragrading_03_query_history.sql)


### 4. Complete Copy History validation

**Script to Execute:** [DORAGRADING_04_COPYHISTORY.sql](/config/Doragrading_04_copy_history.sql)


### 5. Complete Task History validation

**Script to Execute:** [DORAGRADING_05_TASKHISTORY.sql](/config/Doragrading_05_task_history.sql)


### 6. Complete Dynamic Tables validation

**Script to Execute:** [DORAGRADING_06_DYNAMICTABLES.sql](/config/Doragrading_06_dynamic_tables.sql)


## 🎉 Success!

**If all validations return ✅, you have successfully completed the Platform College Observability HOL!**

---

## ⚠️ Important Notes

- Run validation scripts **ONE AT A TIME** (do not run all at once)
- Each validation script must show all ✅ checkmarks
- Total of 30 validation steps across 6 scripts
- Use ACCOUNTADMIN role when running validations

# Next Steps

## Cleanup 

After completing the lab, it's important to clean up resources to avoid unnecessary costs.

**Script to Execute:** [CLEANUP.SQL](/code/99_cleanup/cleanup.sql)
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
