from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create Spark session
spark = SparkSession.builder.appName("social-media-etl").getOrCreate()

# Sample data
data = [
    (1, "This is a Python post.", "2022-03-01", 10, 3, 2, "Twitter"),
    (2, "Another post about Python.", "2022-03-02", 20, 5, 3, "Instagram"),
    (3, "Python is great for data analysis.", "2022-03-03", 30, 2, 4, "Facebook"),
    (4, "I'm learning Python for machine learning.", "2022-03-04", 40, 7, 5, "Twitter"),
    (5, "Python vs. R for data science.", "2022-03-05", 50, 9, 6, "Instagram"),
    (6, "Python web development is awesome.", "2022-03-06", 60, 1, 1, "Facebook"),
    (7, "Python for finance.", "2022-03-07", 70, 4, 3, "Twitter"),
    (8, "Python libraries for data visualization.", "2022-03-08", 80, 6, 2, "Instagram"),
    (9, "Why Python is the best programming language.", "2022-03-09", 90, 3, 1, "Facebook"),
    (10, "Python for data engineering.", "2022-03-10", 100, 8, 7, "Twitter"),
]

# Define schema
columns = ["id", "text", "date", "likes", "comments", "shares", "platform"]

# Create DataFrame
social_media = spark.createDataFrame(data, columns)

# ETL function
def etl(social_media):
    # Replace "Python" with "PySpark" in the text column
    replaced_text = F.regexp_replace(F.col("text"), r"Python", "PySpark")
    social_media = social_media.withColumn("text", replaced_text)
    return social_media

# Apply transformation
output_df = etl(social_media)

# Show result
output_df.show(truncate=False)
