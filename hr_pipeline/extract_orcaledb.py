import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
def read_tables():
    spark = SparkSession.builder.config("spark.jars","C:\\Users\\maboelfadl\\Downloads\\sqldeveloper-20.4.1.407.0006-x64\\sqldeveloper\\jdbc\\lib\\ojdbc8.jar").getOrCreate()
    oracle_props = {
        "user": "hr",
        "password": "hr",
        "driver": "oracle.jdbc.driver.OracleDriver"
    }
    oracle_url = "jdbc:oracle:thin:@localhost:1521:xe"

    employees_oracle = spark.read.jdbc(oracle_url, "EMPLOYEES", properties=oracle_props)
    departments_oracle = spark.read.jdbc(oracle_url, "DEPARTMENTS", properties=oracle_props)
    jobs_oracle = spark.read.jdbc(oracle_url, "JOBS", properties=oracle_props)
    tables={
        "employees_oracle": employees_oracle,
        "departments_oracle": departments_oracle,
        "jobs_oracle": jobs_oracle
    }
    return tables