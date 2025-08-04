from extract_csvs import read_csvfiles
from extract_orcaledb import read_tables
from pyspark.sql.functions import col, lit, coalesce

def merge_csvs():
    dfs = read_csvfiles()

    # Assigning DataFrames from the dictionary
    df_csv_employees = dfs["df_csv_employees"]
    df_csv_department = dfs["df_csv_department"]
    df_csv_dept_emp = dfs["df_csv_dept_emp"]
    df_csv_dept_manager = dfs["df_csv_dept_manager"]
    df_csv_salaries = dfs["df_csv_salaries"]
    df_csv_titles = dfs["df_csv_titles"]

    # Joining CSV DataFrames
    emp_csv = df_csv_employees \
        .join(df_csv_dept_emp, "emp_no", "left") \
        .join(df_csv_department, "dept_no", "left") \
        .join(df_csv_titles, "emp_no", "left") \
        .join(df_csv_salaries, "emp_no", "left")

    emp_csv = emp_csv.join(
        df_csv_dept_manager.select("emp_no").withColumn("is_manager", lit(True)),
        on="emp_no", how="left"
    ).withColumn("is_manager", coalesce(col("is_manager"), lit(False)))

    return emp_csv

def merge_tables():
    tables = read_tables()

    # Assigning DataFrames from the dictionary
    employees_oracle = tables["employees"]
    departments_oracle = tables["departments"]
    jobs_oracle = tables["jobs"]

    # Joining Oracle tables
    emp_oracle = employees_oracle \
        .join(departments_oracle, "DEPARTMENT_ID", "left") \
        .join(jobs_oracle, "JOB_ID", "left")

    return emp_oracle
