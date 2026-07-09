from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Step 1: Create Spark session
spark = SparkSession.builder.appName("crm-saas-etl").getOrCreate()

# Step 2: Input data
customers_data = [
    (1, "John", "Doe", "john.doe@email.com"),
    (2, "Jane", "Smith", "jane.smith@email.com"),
]

orders_data = [
    (1001, 1, 101, "2023-01-10"),
    (1002, 2, 102, "2023-01-11"),
]

products_data = [
    (101, "Product A", "Category1"),
    (102, "Product B", "Category2"),
]

customers_df = spark.createDataFrame(
    customers_data, ["customer_id", "first_name", "last_name", "email"]
)

orders_df = spark.createDataFrame(
    orders_data, ["order_id", "customer_id", "product_id", "order_date"]
)

products_df = spark.createDataFrame(
    products_data, ["product_id", "product_name", "category"]
)

# Step 3: ETL function
def etl(customers, orders, products):
    joined_df = customers.join(
        orders, on="customer_id", how="inner"
    ).join(products, on="product_id", how="inner")
    
    result_df = joined_df.select(
        "order_id",
        F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
        F.col("email").alias("customer_email"),
        "product_name",
        F.col("category").alias("product_category"),
        "order_date",
    )
    return result_df

# Step 4: Call the function
output_df = etl(customers_df, orders_df, products_df)

# Step 5: Show the transformed data
output_df.show(truncate=False)
