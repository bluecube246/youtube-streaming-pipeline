from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.protobuf.functions import from_protobuf
import pyspark.sql.functions as F

ss: SparkSession = SparkSession.builder. \
    master("local"). \
    appName("ecommerce ex"). \
    getOrCreate()

input_root_path = "/Users/leo/Documents/GitHub/youtube-kafka-spark-cassandra"

# 1.1 products
products_df = ss.read.json(f"{input_root_path}/sample_video_fields.json", multiLine=True)

products_df = products_df.select("*", explode("items").alias("data_items"))\
    .select("data_items.snippet.*", "*")\
    .select("data_items.contentDetails.*", "*")\
    .select("data_items.statistics.*", "*")\
    .drop("items", "data_items", "pageInfo")

products_df.show()

products_df.printSchema()