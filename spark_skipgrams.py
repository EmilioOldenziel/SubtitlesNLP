# coding: utf-8

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

# print spark webinterface url
print("Web URL:", spark.sparkContext.uiWebUrl)

# set subtitle language to make ngram of
language = args.language

symbols = [".", ",", "'", '?', '-', '"', '#', '...', '~', "[", "]", "{", "}", ")", "(", "_"]

# init stopword remover
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=symbols) 

# skipgram udf
def skipgram(sentence):
    length = len(sentence)
    return [list(zip(len(sentence[i+1:length])*[n],sentence[i+1:length])) for i, n in enumerate(sentence[:length-1])]
    
skipgrams_udf = udf(skipgram, ArrayType(ArrayType(ArrayType(StringType()))))

lowercase_udf = udf(lambda sentence: [w.lower() for w in sentence], ArrayType(StringType()))

# check if table exists for language, if not: quit
if language not in [table.name for  table in spark.catalog.listTables()]:
    print("Hive tables for language", language, "was not found")
    exit(-1)

#read table
df_subtitles = spark.sql("SELECT * FROM " + language) \
    .select(col('w').alias('words')) \
    .dropna()

#to lowercase
df_subtitles = df_subtitles.withColumn("words", lowercase_udf(col("words")))

# remove stopwords
df_words_clean = stopwords_remover.transform(df_subtitles)
df_words_clean = df_words_clean.drop("words").dropna()

# make skipgrams
df_skipgrams = df_words_clean \
    .withColumn("skipgrams", skipgrams_udf(col("filtered_words"))) \
    .drop("filtered_words") \
    .dropna() \
    .withColumn("skipgrams", explode(col("skipgrams"))) \
    .dropna() \
    .withColumn("skipgrams", explode(col("skipgrams"))) \
    .groupBy("skipgrams").agg(count(col("skipgrams"))) \
    .select(col("skipgrams"),col("count(skipgrams)").alias("frequency")) \
    .filter("frequency > 1") \

# save ngrams and stop spark
print("saving dataframe...")
df_skipgrams.write.mode("overwrite").saveAsTable("sg_"+language)
print("saved", language)

spark.sparkContext.stop()