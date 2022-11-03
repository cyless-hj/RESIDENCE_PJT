from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from pyspark.sql.functions import isnan,when,count
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import split,isnull,lit

from infra.util import std_day

class MiddleShcTransformer:

    @classmethod
    def transform(cls):
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/MIDDLE/MIDDLE.csv")
        #주소 반환 안된 값 넣어주기
        df_spark=df.select('MID_SCH_NAME','ADD_OLD','ADD_STR','LAT','LON')                 
        # df_spark=df_spark.replace(['서울특별시？구로구？개봉동 273-16','서울특별시 강남구 동 617','서울특별시 강남구 동 736'
        # , '서울특별시 강남구 동 712','서울특별시 강남구 동 691', '서울특별시 강남구 동 692']
        # ,['서울특별시 구로구 개봉동 273-16','서울특별시 강남구 일원동 617','서울특별시 강남구 일원동 736','서울특별시 강남구 일원동 712'
        # ,'서울특별시 강남구 일원동 691','서울특별시 강남구 일원동 692']
        #             , 'ADD_OLD')

        
        #구, 동 나누기
        df_split=df_spark.withColumn('SI_DO', split(df_spark['ADD_OLD'],' ').getItem(0)) \
                .withColumn('GU', split(df_spark['ADD_OLD'],' ').getItem(1)) \
                .withColumn('DONG', split(df_spark['ADD_OLD'],' ').getItem(2))
        
        # df_split = df_split.filter((col("DONG") != ''))  
        #데이터프레임의 모든 결측치 세기
        df_split.select([count(when(isnull(c), c)).alias(c) for c in df_split.columns]).show()

        #std_day 추가
        df3= df_split.withColumn('STD_DAY', F.current_date().cast("string"))
        df4=df3.distinct()
        df4.show()


        #category 추가
        df5=df4.withColumn('CATE_CODE',lit("G113"))

        #LOC 추가
        df_loc = find_data(DataWarehouse, 'LOC')
        # df6 = df5.join(df_loc, (df5.GU == df_loc.GU)&(df5.DONG == df_loc.DONG)&(df5.SI_DO == df_loc.SI_DO))
        df6 = df5.join(df_loc, on=['GU', 'DONG', 'SI_DO'])
        
        df6 = df6.drop(df6.SI_DO_CODE) \
                         .drop(df6.SI_DO) \
                         .drop(df6.GU_CODE) \
                         .drop(df6.GU) \
                         .drop(df6.DONG) \
                         .drop(df6.DONG_CODE) \
                         .drop(df6.ADD_OLD)
        df6.show()
        save_data(DataWarehouse, df6, 'MIDDLE')
