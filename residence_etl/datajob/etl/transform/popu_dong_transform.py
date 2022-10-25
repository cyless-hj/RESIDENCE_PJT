from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from pyspark.sql.types import *
from pyspark.sql.functions import col, monotonically_increasing_id, isnan,when,count, isnull
from pyspark.sql import Row
from infra.util import std_day
import pandas as pd


class PopuDongTransformer:

    @classmethod
    def transform(cls):
        schema = StructType([
            StructField("DONG_CODE", IntegerType(), False),
            StructField("SI_DO", StringType(), False),
            StructField("GU", StringType(), False),
            StructField("DONG", StringType(), False),
            StructField("NUM_POPU", IntegerType(), False),
        ])
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .schema(schema) \
                                 .option("encoding", "UTF-8") \
                                 .csv("s3a://residencebucket/raw_data/POPU_DONG/POPU_DONG.csv")

        df_popu_dong = cls._add_loc_idx(df)
        df_popu_dong = spark_session().createDataFrame(df_popu_dong)

        df_popu_dong = cls._refact_df(df_popu_dong)

        save_data(DataWarehouse, df_popu_dong, 'POPU_DONG')

    @classmethod
    def _add_loc_idx(cls, df_popu_dong):
        dong_list = df_popu_dong.select('DONG').rdd.flatMap(lambda x: x).collect()

        day = Row(STD_DAY=std_day())
        rows = []
        for g in range(len(dong_list)):
            rows.append(day)
        day_df = spark_session().createDataFrame(rows)
        pd_day_df = day_df.toPandas()
        pd_popu_dong = df_popu_dong.toPandas()
        df_popu_dong = pd.concat([pd_day_df, pd_popu_dong], axis=1)
        return df_popu_dong


    @classmethod
    def _refact_df(cls, df):
        df_loc = find_data(DataWarehouse, 'LOC')

        df = df.drop(df.DONG_CODE)
        df = df.drop(df.SI_DO)

        df_popu_dong = df.join(df_loc, on=['GU', 'DONG'])
        df_popu_dong = df_popu_dong.drop(df_popu_dong.SI_DO_CODE) \
                         .drop(df_popu_dong.SI_DO) \
                         .drop(df_popu_dong.GU_CODE) \
                         .drop(df_popu_dong.GU) \
                         .drop(df_popu_dong.DONG_CODE) \
                         .drop(df_popu_dong.DONG)
                                   
        return df_popu_dong
