import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
def read_csvfiles():
    spark = SparkSession.builder.getOrCreate()
    df_csv_employees = spark.read.format('csv').option('header',True).load("C:\\Users\\maboelfadl\\Downloads\\employees.csv")
    df_csv_department = spark.read.format('csv').option('header',True).load("C:\\Users\\maboelfadl\\Downloads\\departments.csv")
    df_csv_dept_emp = spark.read.format('csv').option('header',True).load("C:\\Users\\maboelfadl\\Downloads\\dept_emp.csv")
    df_csv_dept_manager = spark.read.format('csv').option('header',True).load("C:\\Users\\maboelfadl\\Downloads\\dept_manager.csv")
    df_csv_salaries = spark.read.format('csv').option('header',True).load("C:\\Users\\maboelfadl\\Downloads\\salaries.csv")
    df_csv_titles = spark.read.format('csv').option('header',True).load("C:\\Users\\maboelfadl\\Downloads\\titles.csv")
    dfs={
        "df_csv_employees": df_csv_employees,
        "df_csv_department":df_csv_department,
        "df_csv_dept_emp":  df_csv_dept_emp,
        "df_csv_dept_manager": df_csv_dept_manager,
        "df_csv_salaries": df_csv_salaries,
        "df_csv_titles": df_csv_titles
    }
    return dfs