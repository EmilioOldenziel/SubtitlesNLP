
# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, count
from os import environ

environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 pyspark-shell' 
spark = SparkSession.builder.appName("Python Spark wordcount").getOrCreate()

take_first = udf(lambda l: l[0]) # take first from list

# read xml files as df
df_xml = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "s") \
    .option("rootTag", "document") \
    .load("./subtitles/af/*/*/*/*/*/*.xml.gz") # spark does not support recursive load

df_wc = df_xml \
    .withColumn("words", explode(col("w"))) \
    .drop("_emphasis", "_id", "time", "w") \
    .withColumn("word", take_first(col("words"))) \
    .groupBy("word").agg(count(col("word"))) \
    .sort(col("count(word)").desc())

# write word count dict to json
df_wc.write.mode('append').json("words_sorted.json")