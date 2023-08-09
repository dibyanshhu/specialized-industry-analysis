import logging
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sum, split, avg
from datetime import timedelta

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a SparkSession
spark = SparkSession.builder.appName("Assignment").getOrCreate()

def create_schema():
    """
    Create the database schema if it doesn't exist.
    """
    spark.sql("create database if not exists dal.assignment")
    logger.info("Database schema 'dal.assignment' created.")
    return True

def read_data():
    """
    Read data from MinIO and save as Hive tables.
    """
    config = configparser.ConfigParser()
    config.read("./conf/config.ini")
    minio_access_key = config.get("MinIO", "access_key")
    minio_secret_key = config.get("MinIO", "secret_key")

    spark.conf.set("spark.hadoop.fs.s3a.access.key", minio_access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", minio_secret_key)

    minio_endpoint = "http://minio:9000" 
    minio_bucket = "demo-data"
    hist_data = "historical_orders.json"
    recent_data = "recent_orders.json"
    cust_data = "Customers.csv"

    # Read data from MinIO
    hist_data_path = f"s3a://{minio_bucket}/{hist_data}"
    df_hist = spark.read.format("json").load(hist_data_path)

    recent_data_path = f"s3a://{minio_bucket}/{recent_data}"
    df_recent = spark.read.format("json").load(recent_data_path)

    cust_data_path = f"s3a://{minio_bucket}/{cust_data}"
    df_cust = spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load(cust_data_path)

    df_hist.write.mode("overwrite").saveAsTable("assignment.historical_orders")
    df_recent.write.mode("overwrite").saveAsTable("assignment.recent_orders")
    df_cust.write.mode("overwrite").saveAsTable("assignment.customers")

    logger.info("Data loaded successfully.")

def solution():
    """
    Main solution logic.
    """
    logger.info("Starting the solution")

    # Step 1: Read data from Hive tables
    historical_df = spark.sql("select * from dal.assignment.historical_orders")
    recent_df = spark.sql("select * from dal.assignment.recent_orders")

    # Step 2: Combine historical and recent order data into a single DataFrame
    combined_orig_df = historical_df.union(recent_df)
    combined_df = combined_orig_df.dropDuplicates(["order_id"])

    # Step 3: Load customer data from customer.csv and de-duplicate it based on customer_id
    customer_orig_df = spark.sql("select * from dal.assignment.customers")
    customer_df = customer_orig_df.dropDuplicates(["customer_id"])

    combined_date_cast_df = combined_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

    # Calculate date ranges
    max_date = combined_date_cast_df.select(col("timestamp")).agg({"timestamp": "max"}).collect()[0][0]
    one_day_before = max_date - timedelta(days=1)
    month_before = max_date - timedelta(days=30)

    # Step 4: Join customer data with order data and de-duplicate it based on order_id
    order_with_customer_df = combined_date_cast_df.join(customer_df, on="customer_id")

    # Step 5: Explode the specialized_industries array to get individual rows for each industry
    customer_industries_df = order_with_customer_df.withColumn("specialized_industry", explode(split(col("specialized_industries"), ";"))).drop("specialized_industries")

    # Step 6: Calculate Industry-Wise Daily Revenue for the last 24 hours
    last_24_hours_df = customer_industries_df.filter(col("timestamp") >= one_day_before)
    industry_daily_revenue_df = last_24_hours_df.groupBy("specialized_industry").agg(sum(col("amount")).alias("daily_revenue"))

    # Step 7: Calculate Industry-Wise Monthly Average Revenue for the last 30 days
    last_30_days_df = customer_industries_df.filter(col("timestamp") >= month_before)
    industry_monthly_revenue_df = last_30_days_df.groupBy("specialized_industry").agg(avg(col("amount")).alias("monthly_avg_revenue"))

    # Step 8: Join daily and monthly revenue data
    industry_fluctuation_df = industry_daily_revenue_df.join(industry_monthly_revenue_df, on="specialized_industry")

    # Step 9: Calculate fluctuations
    industry_fluctuation_cal_df = industry_fluctuation_df.withColumn("fluctuation", col("daily_revenue") - col("monthly_avg_revenue"))

    # Step 10: Identify Top 3 Industries with the Highest Fluctuations (Positive or Negative)
    top_3_industries_df = industry_fluctuation_cal_df.orderBy(col("fluctuation").desc()).limit(3)

    # Step 11: Show the results
    top_3_industries_df.show()

    logger.info("Solution completed successfully.")

if __name__ == "__main__":
    # Call functions
    create_schema()
    read_data()
    solution()
