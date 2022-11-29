import findspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
findspark.init()


SPARK=None
def get_spark_session():

    global SPARK

    if SPARK:
        return SPARK
    
    spark_version = '3.2.2'
    findspark.add_packages(['org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2'
                            ,'org.apache.kafka:kafka-clients:3.2.1'
                            ,'com.amazonaws:aws-java-sdk:1.11.563'
                            ,'org.apache.hadoop:hadoop-aws:3.2.2'])

    S3_ACCESS_KEY=''
    S3_SECRET_KEY=''

    conf = SparkConf().set("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
                    .set("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
                    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                    .set("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true") \
                    .setAppName('corona_log')

    sc=SparkContext(conf=conf)
    SPARK = SparkSession(sc).builder.getOrCreate()
    return SPARK
