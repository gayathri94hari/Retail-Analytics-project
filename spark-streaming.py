# Import libraries and set variables
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")

def is_order(order):
    return 1 if (order == "ORDER") else 0

def is_return(order):
    return 1 if (order == "RETURN") else 0

def total_items(items):
    return len(items)

def total_cost(items, order_type):
    total_cost = 0
    for item in items:
        total_cost = total_cost + item.unit_price * item.quantity if (order_type == "ORDER") else (
                total_cost - (item.unit_price * item.quantity))
    return total_cost


# Code begins
if __name__ == "__main__":

    # Spark Session
    spark = SparkSession \
        .builder \
        .appName("RetailDataAnalytics") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Read Input From Kafka
    order_Raw = spark \
		.readStream  \
		.format("kafka")  \
		.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
		.option("startingOffsets","latest") \
		.option("subscribe","real-time-project")  \
        .load() \
        .selectExpr("CAST(value AS STRING) as data")

    # Creating the JSON Schema
    jsonSchema = StructType() \
        .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", DoubleType()),
        StructField("quantity", IntegerType())]))
             ) \
        .add("type", StringType()) \
        .add("country", StringType()) \
        .add("invoice_no", LongType()) \
        .add("timestamp", TimestampType())

    # Create dataframe from input with only required columns.
    OrderStream = order_Raw.select(from_json(col("data"), jsonSchema).alias("order")).select("order.*")

    # Convert Functions to UDFs
    isOrderUDF = udf(is_order, IntegerType())
    isReturnUDF = udf(is_return, IntegerType())
    totalItemsUDF = udf(total_items, IntegerType())
    totalCostUDF = udf(total_cost, DoubleType())

    # Add new calculated attributes
    OrderStream = OrderStream.withColumn("is_order", isOrderUDF(OrderStream.type)) \
        .withColumn("is_return", isReturnUDF(OrderStream.type)) \
        .withColumn("total_items", totalItemsUDF(OrderStream.items)) \
        .withColumn("total_cost", totalCostUDF(OrderStream.items, OrderStream.type))

    # Calculate time-based KPIs
    timeBasedKPIs = OrderStream.withWatermark("timestamp", "1 minute") \
        .groupby(window("timestamp", "1 minute")) \
        .agg(count("invoice_no").alias("OPM"),
             sum("total_cost").alias("total_sale_volume"),
             sum("is_return").alias("total_returns"),
             sum("is_order").alias("total_orders")) \
        .withColumn("rate_of_return", col("total_returns") / (col("total_returns") + col("total_orders"))) \
        .withColumn("average_transaction_size", col("total_sale_volume") / (col("total_returns") + col("total_orders")))

    # Calculate time-and-country-based KPIs
    timeAndCountryBasedKPIs = OrderStream.withWatermark("timestamp", "1 minute") \
        .groupby(window("timestamp", "1 minute"), "country") \
        .agg(count("invoice_no").alias("OPM"),
             sum("total_cost").alias("total_sale_volume"),
             sum("is_return").alias("total_returns"),
             sum("is_order").alias("total_orders")) \
        .withColumn("rate_of_return", col("total_returns") / (col("total_returns") + col("total_orders")))

    # Extract required columns to Console
    OrderStream = OrderStream.selectExpr("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order",
                                         "is_return")
    # Write the output to Console
    orderOpStream = OrderStream \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "False") \
        .trigger(processingTime="1 minute") \
        .start()

    # Extract required columns
    timeBasedKPIs = timeBasedKPIs.selectExpr("window", "OPM", "total_sale_volume", "average_transaction_size",
                                             "rate_of_return")
    # Write time-based KPIs into JSON files
    timeBasedKPI = timeBasedKPIs \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "/tmp/RetailOp/op1") \
        .option("checkpointLocation", "/tmp/RetailOp/CheckPoints/op1Chckpt") \
        .option("truncate", "False") \
        .trigger(processingTime="1 minute") \
        .start()

    # Extract required columns
    timeAndCountryBasedKPIs = timeAndCountryBasedKPIs.selectExpr("window", "country", "OPM", "total_sale_volume",
                                                                 "rate_of_return")

    # Write time and country-based KPIs into JSON files
    timeAndCountryBasedKPI = timeAndCountryBasedKPIs \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "/tmp/RetailOp/op2") \
        .option("checkpointLocation", "/tmp/RetailOp/CheckPoints/op2Chckpt") \
        .option("truncate", "False") \
        .trigger(processingTime="1 minute") \
        .start()

    # Wait for termination
    orderOpStream.awaitTermination()
    timeBasedKPIsOutput.awaitTermination()
    timeAndCountryBasedKPIsOutput.awaitTermination()