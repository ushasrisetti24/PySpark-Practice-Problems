from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Step 1: Create Spark session
spark = SparkSession.builder.appName("real-estate-etl").getOrCreate()

# Step 2: Input data
properties_data = [
    (1, 101, "Apartment", 1500.0, 1000, "Seattle"),
    (2, 101, "Condo", 1200.0, 800, "Seattle"),
    (3, 102, "House", 2000.0, 1500, "Bellevue"),
    (4, 103, "Apartment", 1800.0, 1200, "Redmond"),
    (5, 103, "Condo", 1000.0, 700, "Redmond"),
]

landlords_data = [
    (101, "John", "Smith", "john.smith@example.com", "555-123-4567"),
    (102, "Jane", "Doe", "jane.doe@example.com", "555-234-5678"),
    (103, "Bob", "Johnson", "bob.johnson@example.com", "555-345-6789"),
    (104, "Mary", "Williams", "mary.williams@example.com", "555-456-7890"),
    (105, "Jack", "Brown", "jack.brown@example.com", "555-567-8901"),
]

properties_df = spark.createDataFrame(
    properties_data,
    ["property_id", "landlord_id", "property_type", "rent", "square_feet", "city"]
)

landlords_df = spark.createDataFrame(
    landlords_data,
    ["landlord_id", "first_name", "last_name", "email", "phone"]
)

# Step 3: ETL function
def etl(properties_df, landlords_df):
    # Pivot property types and sum rents per landlord
    property_pivot_df = properties_df.groupBy("landlord_id").pivot("property_type").agg(F.sum("rent"))

    # Join with landlords and calculate total rental income
    rental_income_df = property_pivot_df.join(landlords_df, "landlord_id").select(
        "landlord_id",
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("landlord_name"),
        (
            F.coalesce(F.col("Apartment"), F.lit(0)) +
            F.coalesce(F.col("Condo"), F.lit(0)) +
            F.coalesce(F.col("House"), F.lit(0))
        ).cast("float").alias("total_rental_income")
    )

    return rental_income_df.sort("landlord_id")

# Step 4: Call the function
output_df = etl(properties_df, landlords_df)

# Step 5: Show the transformed data
output_df.show()
