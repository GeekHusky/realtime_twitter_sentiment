from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys
import requests
import re

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark instance with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create sparksession
spark = SparkSession(sc)

# creat the Streaming Context from the above spark context with window size 2 seconds
ssc = StreamingContext(sc, 5)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)

def pre_process(text_df):
    

    userPattern       = '@[^\s]+'
    alphaPattern      = "[^a-zA-Z0-9]"
    sequencePattern   = r"(.)\1\1+"
    seqReplacePattern = r"\1\1"
    urlPattern        = r"((http://)[^ ]*|(https://)[^ ]*|( www\.)[^ ]*)"


    text_df = text_df.withColumn('text',regexp_replace('tweet_text','[\n]+',''))
    text_df = text_df.withColumn('text',regexp_replace('text','[\t]+',''))
    text_df = text_df.withColumn("text", lower(col("text")))
    text_df = text_df.withColumn('text',regexp_replace('text',userPattern,'USER'))
    text_df = text_df.withColumn('text',regexp_replace('text',alphaPattern,' '))
    test_rdd = text_df.rdd.map(lambda l: (l[0],re.sub(sequencePattern, seqReplacePattern, l[1])))
    text_df = test_rdd.map(lambda w: Row(tweet_text=w[0],text=w[1])).toDF()
    text_df = text_df.withColumn('text',regexp_replace('text',urlPattern,'URL'))
    
    # Creating a UDF to remove emojis
    string_remove_emoji = udf(lambda line: line.encode('ascii', 'ignore').decode('ascii'))
    text_df = text_df.withColumn('text',string_remove_emoji(col('text')))

    return text_df
def get_prediction(tweet_text):
    try:
        if (tweet_text.isEmpty() != True):
            # Get spark sql singleton context from the current context
            rowRDD = tweet_text.map(lambda w: Row(tweet_text=w))
            wordsDF = rowRDD.toDF()
            
            wordsDF = pre_process(wordsDF)
            
            svm_model = PipelineModel.load('model/svm_model_3')
            svm_model.transform(wordsDF).select("tweet_text","text","predictionSVC").show(n=5, truncate=False, vertical=True)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

dataStream.foreachRDD(get_prediction)

ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()



