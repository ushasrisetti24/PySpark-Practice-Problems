from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

# Step 1: Create Spark session
spark = SparkSession.builder.appName("manufacturing-etl").getOrCreate()

# Step 2: Input data
products_data = [
    (1, "A", "Product1"),
    (2, "A", "Product2"),
    (3, "A", "Product3"),
    (4, "B", "Product4"),
    (5, "B", "Product5"),
    (6, "B", "Product6"),
    (7, "C", "Product7"),
    (8, "C", "Product8"),
    (9, "C", "Product9"),
]

sales_data = [
    (1, 1, 10, 100.0),
    (2, 2, 8, 120.0),
    (3, 3, 12, 180.0),
    (4, 4, 5, 50.0),
    (5, 5, 3, 30.0),
    (6, 6, 7, 70.0),
    (7, 7, 15, 150.0),
    (8, 8, 10, 100.0),
    (9, 9, 8, 80.0),
]

products_df = spark.createDataFrame(products_data, ["product_id", "category", "product_name"])
sales_df = spark.createDataFrame(sales_data, ["sale_id", "product_id", "quantity", "revenue"])

# Step 3: ETL function
def etl(products, sales):
    # Aggregate revenue per product
    sales_agg = sales.groupBy("product_id").agg(F.sum("revenue").alias("revenue"))
    
    # Join with products
    joined_df = products.join(sales_agg, "product_id", "inner")
    
    # Define window for ranking within each category
    indexingrows = W.partitionBy("category").orderBy(F.desc("revenue"))
    
    # Apply row_number for ranking
    output_df = joined_df.select(
        "category",
        "product_name",
        F.row_number().over(indexingrows).alias("rank"),
        "revenue"
    )
    
    # Keep only top 3 per category
    return output_df.filter(F.col("rank") <= 3)

# Step 4: Call the function
output_df = etl(products_df, sales_df)

# Step 5: Show the transformed data
output_df.show()
