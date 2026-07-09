from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

# Step 1: Create Spark session
spark = SparkSession.builder.appName("ai-research-etl").getOrCreate()

# Step 2: Input data
research_papers_data = [
    ("P1", "Deep Learning Techniques in AI", 2019),
    ("P2", "Reinforcement Learning for Robotics", 2020),
    ("P3", "Natural Language Processing Advances", 2021),
]

authors_data = [
    ("P1", "A1", "Alice Smith"),
    ("P1", "A2", "Bob Johnson"),
    ("P2", "A3", "Carol Williams"),
    ("P2", "A4", "David Brown"),
    ("P2", "A5", "Eva Davis"),
    ("P3", "A6", "Frank Wilson"),
    ("P3", "A7", "Grace Lee"),
]

research_papers_df = spark.createDataFrame(
    research_papers_data, ["paper_id", "title", "year"]
)

authors_df = spark.createDataFrame(
    authors_data, ["paper_id", "author_id", "name"]
)

# Step 3: ETL function
def etl(research_papers, authors):
    # Define window partitioned by paper_id and ordered by author_id
    windowfunc = W.partitionBy("paper_id").orderBy("author_id")
    
    # Assign row_number per author within each paper
    authors_with_rownum = authors.withColumn("row_number", F.row_number().over(windowfunc))
    
    return authors_with_rownum

# Step 4: Call the function
output_df = etl(research_papers_df, authors_df)

# Step 5: Show the transformed data
output_df.display()
