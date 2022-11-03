from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from pyspark.sql.types import *
from pyspark.sql.functions import col, monotonically_increasing_id, isnan,when,count, isnull, lit
from pyspark.sql import Row
from infra.util import std_day
import pandas as pd
from pyspark.sql import functions as F


class PopuMZTransformer:

    @classmethod
    def transform(cls):

        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/POPU_MZ/POPU_MZ.csv", encoding='cp949')

        #LOC 추가
        df_loc = find_data(DataWarehouse, 'LOC')
        

        df2 = df.join(df_loc, on=['GU', 'DONG'])
        
        df2=df2.drop(df2.DONG_CODE).drop(df2.DONG).drop(df2.SI_DO_CODE) \
                .drop(df2.SI_DO).drop(df2.GU_CODE)
        df2=df2.withColumnRenamed("20~24세","AGE2024").withColumnRenamed("25~29세","AGE2529").withColumnRenamed("30~34세","AGE3034") \
                .withColumnRenamed("35~39세","AGE3539").withColumnRenamed("인구수","NUM_MZ")
        
        df2.show()

        #POPU_DONG 불러오기
        popu_admin_dong = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/POPU_ADMIN_DONG/POPU_ADMIN_DONG.csv", encoding='utf-8')
        popu_admin_dong=popu_admin_dong.withColumnRenamed("DONG","DONG_ADMIN")
        popu_admin_dong.show()

        # df2 = df2.filter(col("STD_DAY") == '2022-10-27')
        df_join = df2.join(popu_admin_dong, on=['GU','DONG_ADMIN'])


        #STD_DAY 추가
        df_join= df_join.withColumn('STD_DAY', F.current_date().cast("string"))
        df_join=df_join.drop(df_join.LOC_IDX).drop(df_join.NUM_POPU)
        df_join.show()

        
        save_data(DataWarehouse, df_join, 'POPU_MZ')