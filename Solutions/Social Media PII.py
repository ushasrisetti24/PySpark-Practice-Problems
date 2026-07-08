from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Step 1: Create Spark session
spark = SparkSession.builder.appName("social-media-etl").getOrCreate()

# Step 2: Input data
data = [
    (1, "alice@example.com", 5551234567),
    (2, "bob@domain.net", 5559876543),
    (3, "carol@email.org", 5551239876),
    (4, "dave@site.com", 5554567890),
    (5, "eve@platform.io", 5559871234),
]

columns = ["user_id", "email", "phone"]

input_df = spark.createDataFrame(data, columns)

# Step 3: ETL function
def etl(input_df):
    formatted_df = input_df.select(
        # Mask first 6 digits of phone, keep last 4
        F.concat(
            F.lit("******"),
            F.substring(F.col("phone").cast("string"), -4, 4)
        ).alias("anon_phone"),
        
        # Extract domain part after '@'
        F.substring_index(F.col("email"), "@", -1).alias("email_domain"),
        
        # Keep user_id
        F.col("user_id")
    )

    return formatted_df

# Step 4: Call the function
output_df = etl(input_df)

# Step 5: Show the transformed data
output_df.show(truncate=False)
