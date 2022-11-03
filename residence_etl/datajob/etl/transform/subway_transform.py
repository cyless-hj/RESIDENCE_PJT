from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql import Row
import pandas as pd
from infra.util import cal_std_day, std_day
from pyspark.sql.types import *
import requests

class SubwayTransformer:
    @classmethod
    def transform(cls):
        df_sub = cls._load_csv()

        # name_list = []
        # sub_name_list = df_sub.select('SUBWAY_NAME').rdd.flatMap(lambda x: x).collect()
        # for i in sub_name_list:
        #     name = i.split('(')[0]
        #     name_list.append(name)
        # df_sub = df_sub.drop(df_sub.SUBWAY_NAME)
        # schema = StructType([
        #     StructField("SUBWAY_NAME", StringType(), False)
        # ])
        # rows = []
        # for g in name_list:
        #     rows.append(Row(SUBWAY_NAME=g))
        # name_df = spark_session().createDataFrame(rows, schema=schema)
        # pd_sub = df_sub.toPandas()
        # pd_name = name_df.toPandas()
        # pd_df = pd.concat([pd_sub, pd_name], axis=1)

        # df_sub = spark_session().createDataFrame(pd_df)
        
        df_sub = cls._add_gu_dong_cate_day(df_sub)
        df_sub = cls._add_loc_idx(df_sub)

        df_sub_line = df_sub.select('SUBWAY_NAME', 'SUBWAY_LINE', 'SUBWAY_CODE', 'STD_DAY')
        df_sub = df_sub.select('SUBWAY_NAME', 'SUBWAY_LINE', 'ADD_STR', 'SUBWAY_CODE', 'STD_DAY', 'LAT', 'LON', 'CATE_CODE', 'LOC_IDX')

        save_data(DataWarehouse, df_sub, 'SUBWAY')
        #save_data(DataWarehouse, df_sub_line, 'SUBWAY_LINE')

    @classmethod
    def _add_loc_idx(cls, df):
        df_loc = find_data(DataWarehouse, 'LOC')

        df = df.join(df_loc, on=['GU', 'DONG'])

        df = df.drop(df.SI_DO_CODE) \
                         .drop(df.SI_DO) \
                         .drop(df.GU_CODE) \
                         .drop(df.GU) \
                         .drop(df.DONG) \
                         .drop(df.DONG_CODE)
                         
        return df

    @classmethod
    def _add_gu_dong_cate_day(cls, df_cafe):
        gu_list = []
        dong_list = []
        old_add_list = df_cafe.select('OLD_ADD').rdd.flatMap(lambda x: x).collect()

        for i in old_add_list:
            gu = i.split(' ')[1]
            dong = i.split(' ')[2]
            gu_list.append(gu)
            dong_list.append(dong)
        
        cate_day = Row(CATE_CODE='A112', STD_DAY=std_day())
        rows = []
        for g in range(len(old_add_list)):
            rows.append(cate_day)
        cate_day_df = spark_session().createDataFrame(rows)

        schema = StructType([
            StructField("GU", StringType(), False)
        ])
        rows = []
        for g in gu_list:
            rows.append(Row(GU=g))
        gu_df = spark_session().createDataFrame(rows, schema=schema)

        schema = StructType([
            StructField("DONG", StringType(), False)
        ])
        rows = []
        for g in dong_list:
            rows.append(Row(DONG=g))
        dong_df = spark_session().createDataFrame(rows, schema=schema)

        pd_cafe = df_cafe.toPandas()
        pd_gu = gu_df.toPandas()
        pd_dong = dong_df.toPandas()
        pd_cate_day = cate_day_df.toPandas()
        pd_df = pd.concat([pd_cafe, pd_gu, pd_dong, pd_cate_day], axis=1)

        df_cafe = spark_session().createDataFrame(pd_df)

        df_cafe = df_cafe.drop(df_cafe.OLD_ADD)
        return df_cafe

    @classmethod
    def _load_csv(cls):
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/SUBWAY/SUBWAY.csv")
                                 
        return df
