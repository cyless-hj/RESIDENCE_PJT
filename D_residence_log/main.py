from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from datetime import date, datetime
from pyspark.sql import Row
from pyspark.sql.types import *
import json

# foreachBatch : Spark Streaming 배치마다 수행할 작업을 콜백함수로 넘겨줄 수있다.
# https://spark.apache.org/docs/3.3.1/structured-streaming-programming-guide.html#foreachbatch

kafka_bootstrap_servers = 'localhost:9092'


def foreach_batch_function(df, epoch_id):
    global kafka_bootstrap_servers

    row = df.select(df.value).collect()
    if not row :
        print('no data to push')
        return 

    json_data = row[0]['value']
    dict = json.loads(json_data)

    if dict.get('session_id') == 'None':
        return

    dict = {
        'value': json.dumps({
            'user'      :dict['user'],
            'url'       :dict['url'],
            'session_id':dict['session_id'],
            'timestamp':dict['timestamp']
            })
        }

    temp = get_spark_session().createDataFrame([dict])
    temp.select("*") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", 'project-session') \
        .save() \

if __name__ == "__main__":
    topic = 'project-log'
    dfs = get_spark_session() \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "False") \
        .option("subscribe", topic) \
        .load()

    dfs = dfs.selectExpr("CAST(value as STRING)").alias("log")
    
     #.foreachBatch(foreach_batch_function) \
    query = dfs.writeStream \
        .format("json") \
        .outputMode("append") \
        .option('checkpointLocation', 's3a://residencebucket/tmp/log/checkpoint/') \
        .option("path", "s3a://residencebucket/log/request/") \
        .trigger(processingTime='20 seconds')  \
        .start() \
        
    query.awaitTermination()




