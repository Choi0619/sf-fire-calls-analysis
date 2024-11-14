import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_date, weekofyear, corr, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType

# Initialize Spark session
spark = SparkSession.builder.appName("FireCallsAnalysis").getOrCreate()

# Ensure dataset is provided as a command-line argument
if len(sys.argv) != 2:
    print("Usage: spark-submit module-05.py <file_path>")
    sys.exit(1)

# Read the dataset path from the command line
file_path = sys.argv[1]

# Define the schema based on the textbook
fire_schema = StructType([
    StructField('CallNumber', IntegerType(), True),
    StructField('UnitID', StringType(), True),
    StructField('IncidentNumber', IntegerType(), True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', IntegerType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', IntegerType(), True),
    StructField('ALSUnit', BooleanType(), True),
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)
])

# Read the CSV file into a DataFrame
fire_df = spark.read.csv(file_path, header=True, schema=fire_schema)

# 1. What were all the different types of fire calls in 2018?
fire_df = fire_df.withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy"))
fire_df_2018 = fire_df.filter(year(col("CallDate")) == 2018)
fire_df_2018.select("CallType").where(col("CallType").isNotNull()).distinct().show(truncate=False)

# 2. What months within the year 2018 saw the highest number of fire calls?
fire_df_2018.groupBy(month(col("CallDate")).alias("Month")).count().orderBy("count", ascending=False).show()

# 3. Which neighborhood in San Francisco generated the most fire calls in 2018?
fire_df_2018.groupBy("Neighborhood").count().orderBy("count", ascending=False).show(1)

# 4. Which neighborhoods had the worst response times to fire calls in 2018?
fire_df_2018.groupBy("Neighborhood").agg({"Delay": "avg"}).orderBy("avg(Delay)", ascending=False).show(1)

# 5. Which week in the year 2018 had the most fire calls?
fire_df_2018.groupBy(weekofyear(col("CallDate")).alias("Week")).count().orderBy("count", ascending=False).show(1)

# 6. Is there a correlation between neighborhood, zip code, and number of fire calls?
neighborhood_zip_count = fire_df.groupBy("Neighborhood", "Zipcode").agg(count("CallNumber").alias("CallCount")).orderBy("CallCount", ascending=False)
neighborhood_zip_count.show()
neighborhood_zip_count.select(corr("Zipcode", "CallCount")).show()

# 7. How can we use Parquet files or SQL tables to store this data and read it back?

# Parquet File
parquet_path = "sf_fire_calls.parquet"
fire_df.write.mode("overwrite").parquet(parquet_path)
fire_df_parquet = spark.read.parquet(parquet_path)
fire_df_parquet.show(5)

# SQL Table
fire_df.write.mode("overwrite").saveAsTable("FireCallsSQLTable")
sql_result = spark.sql("SELECT * FROM FireCallsSQLTable")
sql_result.show(5)