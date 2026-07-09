from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Step 1: Create Spark session
spark = SparkSession.builder.appName("video-streaming-etl").getOrCreate()

# Step 2: Input data
videos_data = [
    (1, "Amazing Adventure", "Action", 2020, 120, 2500000),
    (2, "Sci-fi World", "Sci-fi", 2018, 140, 800000),
    (3, "Mysterious Island", "Drama", 2022, 115, 1500000),
    (4, "Uncharted Realms", "Action", 2019, 134, 3200000),
    (5, "Journey to the Stars", "Sci-fi", 2021, 128, 1100000),
]

columns = ["video_id", "title", "genre", "release_year", "duration", "view_count"]

input_df = spark.createDataFrame(videos_data, columns)

# Step 3: ETL function
def etl(input_df):
    # Current year = 2024, so last 5 years means release_year >= 2019
    cutoff_year = 2024 - 5 + 1  # = 2020
    df_trans = input_df.filter(
        (F.col("view_count") > 1000000) & (F.col("release_year") >= cutoff_year)
    )
    return df_trans

# Step 4: Call the function
output_df = etl(input_df)

# Step 5: Show the transformed data
output_df.show(truncate=False)
