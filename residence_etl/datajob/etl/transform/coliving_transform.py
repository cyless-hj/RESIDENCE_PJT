from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from pyspark.sql.types import *
from pyspark.sql.functions import col, monotonically_increasing_id, isnan,when,count, isnull, lit
from pyspark.sql import Row
from infra.util import std_day
import pandas as pd
from pyspark.sql import functions as F


class ColivingTransformer:

    @classmethod
    def transform(cls):

        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/COLIVING/COLIVING.csv", encoding='cp949')

        #LOC 추가
        df_loc = find_data(DataWarehouse, 'LOC')
        df_loc.show()        

        df2 = df.join(df_loc, on=['GU', 'DONG','DONG_CODE'])

        df2=df2.drop(df2.DONG_ADMIN).drop(df2.DONG_ADMIN_CODE).drop(df2.DONG_CODE).drop(df2.DONG).drop(df2.SI_DO_CODE) \
                .drop(df2.SI_DO).drop(df2.GU_CODE).drop(df2.GU)

        df2.show()

        #STD_DAY
        df2= df2.withColumn('STD_DAY', F.current_date().cast("string"))
        #카테고리코드 채우기
        df2=df2.withColumn('CATE_CODE',lit("J111"))
        df2.show()
        save_data(DataWarehouse, df2, 'COLIVING')