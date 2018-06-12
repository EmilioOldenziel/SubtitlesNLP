# coding: utf-8

import argparse
import json

from spark import Spark
from pyspark.sql.functions import col, explode, count, udf, lower, collect_list, sum
from pyspark.sql.types import  ArrayType, StringType, IntegerType
from pyspark.ml.feature import NGram, StopWordsRemover

parser = argparse.ArgumentParser()
parser.add_argument('--language', help='give language to parse', required=True)
args = parser.parse_args()

sp = Spark()
spark = sp.make_spark_session()

# set subtitle language to make ngram of
language = args.language

symbols = [".", ",", "'", '?', '-', '"', '#', '...', '~', "[", "]", "{", "}", ")", "(", "_", "!", "/", "\\", ":", "..", "$"]

# init stopword remover
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=symbols) 

# skipgram udf
def skipgram(sentence):
    enumerated_skips = []
    length = len(sentence)
    for i, w in enumerate(sentence[:length-1]):
        skips = list(zip(len(sentence[i+1:length])*[w],sentence[i+1:length]))
        skips = [(x,y,i) for x, y in skips]
        enumerated_skips += skips
    return enumerated_skips

def histogram(skips):
    hist = {}
    for skip in skips:
        skip = int(skip)
        if skip in hist:
            hist[skip] += 1
        else:
            hist[skip] = 1
    return [[k,v] for k,v in hist.items()]
    
skipgrams_udf = udf(skipgram, ArrayType(ArrayType(StringType())))
histogram_udf = udf(histogram, ArrayType(ArrayType(IntegerType())))

lowercase_udf = udf(lambda sentence: [w.lower() for w in sentence], ArrayType(StringType()))

# check if language table exists
sp.table_exists(spark, language)

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
    .withColumn("word1", col("skipgrams")[0]) \
    .withColumn("word2", col("skipgrams")[1]) \
    .withColumn("skip", col("skipgrams")[2]) \
    .drop("skipgrams") \
    .groupby("word1", "word2").agg(count(col("skip")), collect_list(col("skip"))) \
    .withColumn('skips', histogram_udf(col("collect_list(skip)"))) \
    .drop("collect_list(skip)") \
    .select(col("word1"), col("word2"), col("count(skip)").alias("frequency"), col("skips")) \
    .filter("frequency > 1")

sp.save_table(df_skipgrams, "sg_" + language)

spark.sparkContext.stop()