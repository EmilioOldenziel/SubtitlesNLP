import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, udf, lower
from pyspark.sql.types import  ArrayType, StringType
from pyspark.ml.feature import NGram, StopWordsRemover
from os import environ, path

parser = argparse.ArgumentParser()
parser.add_argument('--language', help='give language to parse', required=True)
args = parser.parse_args()

# warehouse_location points to the default location for managed databases and tables
warehouse_location = path.abspath('spark-warehouse')

# set driver memory
submit_args = [
    '--driver-memory 15g',
    'pyspark-shell'
]
environ['PYSPARK_SUBMIT_ARGS'] =  " ".join(submit_args)

spark = SparkSession \
    .builder \
    .appName("Python Spark ngrams") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

language = args.language

df_wc = spark.sql("select * from wc_" + language)
df_sg = spark.sql("select * from sg_" + language)

df = df_sg \
    .join(df_wc.select(col('frequency').alias("frequency_word_1"), 'word'), [df_sg.skipgrams[0] == df_wc.word], 'left').drop('word') \
    .join(df_wc.select(col('frequency').alias("frequency_word_2"), 'word'), [df_sg.skipgrams[1] == df_wc.word], 'left').drop('word') \
    .withColumn('frequency_normalised', col('frequency')/(col('frequency_word_1')+col('frequency_word_2'))) \
    .withColumn('balance', (col('frequency_word_1')/col('frequency_word_2'))) \
    .sort(col("frequency_normalised").desc())

print(df.count(), "normalised grams for", language)
print("saving dataframe...")
df.write.mode("overwrite").saveAsTable("nsg_"+language)
print("saved", language)

spark.sparkContext.stop()