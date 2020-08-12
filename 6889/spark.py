from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

import os
os.environ["PYSPARK_PYTHON"]="/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# interval is 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    # try:
    sql_context = get_sql_context_instance(rdd.context)
    print(1)
    row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
    print(row_rdd.collect)
    hashtags_df = sql_context.createDataFrame(row_rdd)

    hashtags_df.registerTempTable("hashtags")
    print(4)

    hashtag_counts_df = sql_context.sql(
        "select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
    hashtag_counts_df.show()
    send_df_to_dashboard(hashtag_counts_df)
    # except:
    #     e = sys.exc_info()[0]
    #     print("Error: %s" % e)


def send_df_to_dashboard(df):
    # extract the hashtags from dataframe and convert them into array
    top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
    # extract the counts from dataframe and convert them into array
    tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
    # initialize and send the data through REST API
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)

def send_sentence(time, rdd):
    print("----------- %s -----------" % str(time))
    res = rdd.collect()
    url = 'http://localhost:5001/updateSentence'
    request_data = {'sentences':str(res)}
    requests.post(url, data=request_data)


sentences = dataStream.filter(lambda line: '#' in line)
sentences.foreachRDD(send_sentence)

words = dataStream.flatMap(lambda line: line.split(" "))
words.pprint()
# filter hashtag mark
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)

# do processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()