-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Overview
-- MAGIC
-- MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
-- MAGIC
-- MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/diamonds-1.csv"
-- MAGIC file_type = "csv"
-- MAGIC
-- MAGIC # CSV options
-- MAGIC infer_schema = "false"
-- MAGIC first_row_is_header = "false"
-- MAGIC delimiter = ","
-- MAGIC
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SQLContext
-- MAGIC sqlContext = SQLContext(sc)

-- COMMAND ----------



-- COMMAND ----------

-- Create a temporary view for the CSV file
CREATE OR REPLACE TEMPORARY VIEW diamonds_view
USING CSV
OPTIONS (
  path "dbfs:/FileStore/tables/diamonds-1.csv",
  header "true"
)

-- COMMAND ----------

-- Query the data from the temporary view


SELECT *
FROM diamonds_view where _c0=3038

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Creating widgets for leveraging parameters, and printing the parameters
-- MAGIC
-- MAGIC # dbutils.widgets.text("input", "","")
-- MAGIC # y = dbutils.widgets.get("input")
-- MAGIC # print ("Param -\'input':")
-- MAGIC # print (y)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(y)

-- COMMAND ----------

-- %python
-- # y=int(y)
-- type(y)


-- COMMAND ----------

-- Query the data from the temporary view


-- SELECT *
-- FROM diamonds_view where carat =y

-- COMMAND ----------

-- create procedure spGetEmp
-- @car int
-- as
-- begin
-- select * from  diamonds_view  where _c0=@car;







-- COMMAND ----------



CREATE OR REPLACE TEMPORARY FUNCTION GetEmployeesByDepartme(c INT)
RETURNS TABLE (_c0 INT)
LANGUAGE SQL
AS
'SELECT * FROM diamonds_view WHERE _c0 = c';


-- COMMAND ----------

SELECT * FROM GetEmployeesByDepartment (3038);

-- COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ExampleApp").getOrCreate()

# Sample data to create DataFrame
data = [
    (1, 'Diamond1', 5000),
    (2, 'Diamond2', 3000),
    (3, 'Diamond3', 4000),
    (4, 'Diamond4', 2500),
    (5, 'Diamond5', 3500)
]

# Define schema
schema = ["_c0", "name", "price"]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Register DataFrame as a temporary view
df.createOrReplaceTempView("diamonds_view")

# Define the function to query the view
def get_employees_by_departme(c):
    query = f"SELECT * FROM diamonds_view WHERE _c0 = {c}"
    result_df = spark.sql(query)
    return result_df

# Example usage
result = get_employees_by_departme(3)
result.show()


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Initialize Spark session
-- MAGIC spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
-- MAGIC
-- MAGIC # Sample data to create DataFrame
-- MAGIC data = [
-- MAGIC     (1, 'Diamond1', 5000),
-- MAGIC     (2, 'Diamond2', 3000),
-- MAGIC     (3, 'Diamond3', 4000),
-- MAGIC     (4, 'Diamond4', 2500),
-- MAGIC     (5, 'Diamond5', 3500)
-- MAGIC ]
-- MAGIC
-- MAGIC # Define schema
-- MAGIC schema = ["_c0", "name", "price"]
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC df = spark.createDataFrame(data, schema)
-- MAGIC
-- MAGIC # Register DataFrame as a temporary view
-- MAGIC df.createOrReplaceTempView("diamonds_view")
-- MAGIC
-- MAGIC # Define the function to query the view
-- MAGIC def get_employees_by_departme(c):
-- MAGIC     query = f"SELECT * FROM diamonds_view WHERE _c0 = {c}"
-- MAGIC     result_df = spark.sql(query)
-- MAGIC     return result_df
-- MAGIC
-- MAGIC # Example usage
-- MAGIC result = get_employees_by_departme(3)
-- MAGIC result.show()
-- MAGIC

-- COMMAND ----------


