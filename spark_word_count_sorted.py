
# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, count
from os import environ, path

# warehouse_location points to the default location for managed databases and tables
warehouse_location = path.abspath('spark-warehouse')

environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 pyspark-shell' 
spark = SparkSession \
    .builder \
    .appName("Python Spark wordcount") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

take_first = udf(lambda l: l[0]) # take first from list

language = "af"

# read xml files as df
df_xml = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "s") \
    .option("rootTag", "document") \
    .load("./subtitles/"+ language + "/*/*/*/*/*/*.xml.gz") # spark does not support recursive load

df_wc = df_xml \
    .withColumn("words", explode(col("w"))) \
    .drop("_emphasis", "_id", "time", "w") \
    .withColumn("word", take_first(col("words"))) \
    .groupBy("word").agg(count(col("word"))) \
    .select(col("word"),col("count(word)").alias("frequency")) \
    .sort(col("frequency").desc())

print(df_wc.count(), "words found")
print("saving dataframe...")
df_wc.write.saveAsTable(language)
print("saved, language")

spark.sparkContext.stop()