from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql import Row
import pandas as pd
from infra.util import cal_std_day, std_day
from pyspark.sql.types import *
import requests

class LeisureTransformer:
    @classmethod
    def transform(cls):
        df_lei = cls._load_csv()
        df_lei = cls._add_gu_dong(df_lei)
        df_lei = cls._add_cate_day(df_lei)
        df_lei = cls._add_loc_idx(df_lei)

        save_data(DataWarehouse, df_lei, 'LEISURE')
        print((df_lei.count(), len(df_lei.columns)))

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

    @classmethod
    def _add_cate_day(cls, df):
        nm_list = df.select('GU').rdd.flatMap(lambda x: x).collect()
        cate_day = Row(CATE_CODE='F124', STD_DAY=std_day())
        rows = []
        for g in range(len(nm_list)):
            rows.append(cate_day)
        cate_day_df = spark_session().createDataFrame(rows)

        pd_df = df.toPandas()
        pd_cate_day = cate_day_df.toPandas()
        df = pd.concat([pd_df, pd_cate_day], axis=1)
        
        df = spark_session().createDataFrame(df)
        return df

    @classmethod
    def _add_gu_dong(cls, df):
        addr_split_list = []
        add_list = df.select('ADD_STR').rdd.flatMap(lambda x: x).collect()
        for i in add_list:
            sp_add = str(i).split()[0:4]
            sp_add = ' '.join(sp_add)
            addr_split_list.append(sp_add)
        
        gu_list = []
        dong_list = []
        client_id = ""
        client_secret = ""
        for addr in addr_split_list:
            try:
                url = f"https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode?query={addr}"
                headers = {'X-NCP-APIGW-API-KEY-ID': client_id,
                        'X-NCP-APIGW-API-KEY': client_secret}

                r = requests.get(url, headers=headers)

                if r.status_code == 200:
                    try:
                        data = r.json()
                        old_add = data['addresses'][0]['jibunAddress']
                        dong = old_add.split()[0:4][2]
                        dong_list.append(dong)
                    except:
                        dong_list.append('-')
                else :
                    dong_list.append('-')
            except:
                dong_list.append('-')
                continue

        for i in add_list:
            gu = i.split(' ')[1]
            gu_list.append(str(gu))

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

        pd_df = df.toPandas()
        pd_gu = gu_df.toPandas()
        pd_dong = dong_df.toPandas()
        pd_df = pd.concat([pd_df, pd_gu, pd_dong], axis=1)

        df = spark_session().createDataFrame(pd_df)
        df = df.filter((col("ADD_STR") != '-') & (col("GU") != '') & (col("DONG") != '-'))
        return df

    @classmethod
    def _add_loc_idx(cls, df_cafe):
        df_loc = find_data(DataWarehouse, 'LOC')

        df_cafe = df_cafe.join(df_loc, on=['GU', 'DONG'])

        df_cafe = df_cafe.drop(df_cafe.SI_DO_CODE) \
                         .drop(df_cafe.SI_DO) \
                         .drop(df_cafe.GU_CODE) \
                         .drop(df_cafe.GU) \
                         .drop(df_cafe.DONG) \
                         .drop(df_cafe.DONG_CODE)
                         
        return df_cafe

    @classmethod
    def _add_gu_dong_cate_day(cls, df_cafe):
        gu_list = []
        dong_list = []
        old_add_list = df_cafe.select('ADD_OLD').rdd.flatMap(lambda x: x).collect()

        for i in old_add_list:
            gu = i.split(' ')[1]
            dong = i.split(' ')[2]
            gu_list.append(gu)
            dong_list.append(dong)
        
        cate_day = Row(CATE_CODE='H111', STD_DAY=std_day())
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

        df_cafe = df_cafe.drop(df_cafe.ADD_OLD)
        return df_cafe

    @classmethod
    def _load_csv(cls):
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/LEISURE/LEISURE.csv")
                                 
        return df
