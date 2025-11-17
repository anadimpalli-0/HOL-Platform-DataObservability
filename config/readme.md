# How to Complete Lab Grading

This is where you can find the validation scripts you need to run to prove you completed the Snowflake Observability HOL.

Remember to edit your contact information in the SQL Statement for the SE_GREETER.sql
If all validations return ✅, you have successfully completed the Platform Performance Clustering HOL

## ✅ Grading Steps

After completing each lab phase or the complete lab, run the corresponding DoraGrading script:

### 1. Complete Traces validation:

```sql
@Doragrading_01_traces.sql
```

### 2. Complete Logs validation:
```sql
@Doragrading_02_logs.sql
```

### 3. Complete Query History validation
```sql
@Doragrading_03_query_history.sql
```

### 4. Complete Copy History validation
```sql
@Doragrading_04_copy_history.sql
```

### 5. Complete Task History validation
```sql
@Doragrading_05_task_history.sql
```

### 6. Complete Dynamic Tables validation
```sql
@Doragrading_06_dynamic_tables.sql
```

## 🎉 Success!

**If all validations return ✅, you have successfully completed the Platform College Observability HOL!**

---

## ⚠️ Important Notes

- Run validation scripts **ONE AT A TIME** (do not run all at once)
- Each validation script must show all ✅ checkmarks
- Total of 30 validation steps across 6 scripts
- Use ACCOUNTADMIN role when running validations

