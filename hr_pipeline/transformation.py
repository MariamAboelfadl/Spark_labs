from merges import merge_csvs,merge_tables
from pyspark.sql.functions import col, lit, to_date, datediff, current_date

def transform():
    emp_oracle=merge_tables()
    emp_csv=merge_csvs()

    emp_csv = emp_csv.withColumn("hire_date", to_date("hire_date"))
    emp_csv = emp_csv.withColumn("tenure_years", datediff(current_date(), col("hire_date")) / 365)
    emp_csv = emp_csv.withColumn("source", lit("CSV"))
    emp_csv = emp_csv.select(
    col("emp_no").alias("employee_id"), "first_name", "last_name", "gender",
    "hire_date", "dept_name".alias("department_name"), "title".alias("job_title"),
    col("salary").cast("double"), "tenure_years", "is_manager", "source"
    )

    emp_oracle = emp_oracle.withColumn("hire_date", to_date("HIRE_DATE"))
    emp_oracle = emp_oracle.withColumn("tenure_years", datediff(current_date(), col("hire_date")) / 365)
    emp_oracle = emp_oracle.withColumn("is_manager", lit(None).cast("boolean"))
    emp_oracle = emp_oracle.withColumn("source", lit("ORACLE"))

    emp_oracle = emp_oracle.select(
    col("EMPLOYEE_ID").alias("employee_id"), col("FIRST_NAME").alias("first_name"),
    col("LAST_NAME").alias("last_name"), lit(None).cast("string").alias("gender"),
    "hire_date", col("DEPARTMENT_NAME").alias("department_name"),
    col("JOB_TITLE").alias("job_title"), col("SALARY").cast("double"),
    "tenure_years", "is_manager", "source"
     )
    
    df = emp_csv.unionByName(emp_oracle)
    return df

