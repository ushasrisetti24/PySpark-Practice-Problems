from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Step 1: Create Spark session
spark = SparkSession.builder.appName("ecommerce-etl").getOrCreate()

# Step 2: Input data
products_data = [
    (1, "Apparel", 25.99),
    (2, "Apparel", 35.99),
    (3, "Footwear", 50.00),
    (4, "Footwear", 75.00),
    (5, "Apparel", 20.00),
]

orders_data = [
    (101, 1, 2),
    (102, 2, 1),
    (103, 1, 3),
    (104, 3, 1),
    (105, 4, 2),
]

products_df = spark.createDataFrame(products_data, ["product_id", "category", "price"])
orders_df = spark.createDataFrame(orders_data, ["order_id", "product_id", "quantity"])

# Step 3: ETL function
def etl(products_df, orders_df):
    # Join products with orders
    joined_df = products_df.join(orders_df, on="product_id", how="inner")
    
    # Group by category and calculate average price and total order count
    output_df = joined_df.groupBy("category").agg(
        F.avg("price").alias("avg_price"),
        F.count("order_id").alias("total_orders_count")
    )
    
    return output_df

# Step 4: Call the function
result_df = etl(products_df, orders_df)

# Step 5: Show the transformed data
result_df.show()
