from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Step 1: Create Spark session
spark = SparkSession.builder.appName("telecom-etl").getOrCreate()

# Step 2: Input data
calls_data = [
    (1, 1, "2022-01-01", 100),
    (2, 2, "2022-01-01", 200),
    (3, 1, "2022-01-02", 150),
    (4, 3, "2022-01-02", 300),
    (5, 2, "2022-01-03", 50),
]

customers_data = [
    (1, "Alice", "NY", 10, "doctor"),
    (2, "Bob", "CA", 12, "lawyer"),
    (3, "Charlie", "TX", 6, "engineer"),
]

calls_df = spark.createDataFrame(calls_data, ["call_id", "cust_id", "date", "duration"])
customers_df = spark.createDataFrame(customers_data, ["cust_id", "name", "state", "tenure", "occupation"])

# Step 3: ETL function
def etl(calls_df, customers_df):
    transformed_df = calls_df.groupBy("date").agg(
        F.count_distinct("cust_id").alias("num_customers"),
        F.sum("duration").alias("total_duration")
    )
    return transformed_df

# Step 4: Call the function
output_df = etl(calls_df, customers_df)

# Step 5: Show the transformed data
output_df.display()
