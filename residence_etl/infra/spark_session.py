import findspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf



def get_spark_session():
    findspark.init()
    return SparkSession.builder.getOrCreate()

def spark_session():
    conf = SparkConf()

    conf.set("spark.hadoop.fs.s3a.access.key", "")
    conf.set("spark.hadoop.fs.s3a.secret.key", "")

    # S3 REGION 설정 ( V4 때문에 필요 )
    conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")

    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("Learning_Spark") \
        .getOrCreate()

    # Signature V4 설정
    spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

    return spark
