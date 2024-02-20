from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import from_protobuf
import pyspark.sql.functions as F
from pyspark.sql.functions import from_json, col, explode
import os

#os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.kafka:kafka-clients:3.4.0,org.apache.spark:spark-streaming_2.12:3.4.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.1,org.apache.commons:commons-pool2:2.11.1,org.apache.spark:spark-protobuf_2.12:3.4.1"
# "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
#.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1")\

#os.environ['PYSPARK_SUBMIT_ARGS'] = "--master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, IntegerType, StructField, StringType, ArrayType, TimestampType, MapType, \
    BooleanType, FloatType
from textblob import TextBlob

# 1.2 items
items_schema = StructType([
    StructField("kind", StringType(), False),
    StructField("items", ArrayType(StructType([
        StructField("id", StringType(), False),
        StructField("snippet", StructType([
            StructField("publishedAt", StringType(), False),
            StructField("channelId", StringType(), False),
            StructField("title", StringType(), False),
            StructField("description", StringType(), False),
            StructField("channelTitle", StringType(), False),
        ]), False),
        StructField("contentDetails", StructType([
            StructField("duration", StringType(), False),
        ]), False),
        StructField("statistics", StructType([
            StructField("viewCount", StringType(), False),
            StructField("likeCount", StringType(), False),
            StructField("favoriteCount", StringType(), False),
            StructField("commentCount", StringType(), False),
        ]), False),
    ])), False),
    StructField("pageInfo", StructType([
        StructField("totalResults", StringType(), False),
        StructField("resultsPerPage", StringType(), False),
    ]), False),
])

comment_schema = StructType([
    StructField("id", StringType(), False),
    StructField("snippet", StructType([
        StructField("channelId", StringType(), False),
        StructField("videoId", StringType(), False),
        StructField("topLevelComment", StructType([
            StructField("snippet", StructType([
                StructField("textOriginal", StringType(), False)
            ]))
        ]), False),
    ]))
])


def get_sentiment(comment):
    score = TextBlob(comment).sentiment.polarity
    if score > 0:
        return "positive"
    elif score < 0:
        return "negative"
    else:
        return "neutral"


sentiment_udf = udf(get_sentiment)


if __name__ == '__main__':
    ss: SparkSession = SparkSession.builder\
        .master("local") \
        .appName("youtube_streaming") \
        .getOrCreate()

    spark_df = ss.readStream \
        .format('kafka')\
        .option("kafka.bootstrap.servers", "localhost:29092")\
        .option("subscribe", "video")\
        .option("startingOffsets", "earliest")\
        .load()\
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
        .select(from_json(col("value"), items_schema).alias("data"))\
        .select("*", explode("data.items").alias("data_items")) \
        .select("data_items.snippet.*", "*") \
        .select("data_items.contentDetails.*", "*") \
        .select("data_items.statistics.*", "*") \
        .drop("data", "data_items")
        #.withColumn("data_items", col("data.items"))

    comment_df = ss.readStream\
        .format('kafka') \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "comment") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
        .select(from_json(col("value"), comment_schema).alias("data"))\
        .select("data.snippet.*", "*")\
        .select("topLevelComment.*", "*")\
        .select("snippet.*", "*")\
        .select("data.id", "*")\
        .withColumn("Sentiment", sentiment_udf(col('textOriginal')))\
        .drop("data", "topLevelComment", "snippet")

    comment_df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .start() \

    spark_df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .start() \

    comment_df.writeStream.format("org.apache.spark.sql.cassandra")\
            .option('checkpointLocation', '/tmp/checkpoint')\
            .option("keyspace", "fc_catalog")\
            .option("table", "comment")\
            .start()

    ss.streams.awaitAnyTermination()

