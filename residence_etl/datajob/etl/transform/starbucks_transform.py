from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql import Row
import pandas as pd
from infra.util import std_day
from pyspark.sql.types import *
import requests

class StarbucksTransformer:
    @classmethod
    def transform(cls):
        df_star = cls._load_csv()
        df_star = cls._add_gu_dong_cate_day(df_star)
        df_star = cls._add_loc_idx(df_star)
        df_star = cls._add_lat_lon(df_star)

        save_data(DataWarehouse, df_star, 'STARBUCKS')

    @classmethod
    def _add_lat_lon(cls, df_cafe):
        addr_split_list = []
        add_list = df_cafe.select('ADD_STR').rdd.flatMap(lambda x: x).collect()
        for i in add_list:
            sp_add = str(i).split()[0:4]
            sp_add = ' '.join(sp_add)
            addr_split_list.append(sp_add)
        
        lat_list = []
        lon_list = []
        client_id = ""
        client_secret = ""
        for addr in addr_split_list:
            try:
                url = f"https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode?query={addr}"
                headers = {'X-NCP-APIGW-API-KEY-ID': client_id,
                        'X-NCP-APIGW-API-KEY': client_secret
                        }

                r = requests.get(url, headers=headers)

                if r.status_code == 200:
                    try:
                        data = r.json()
                        lat = data['addresses'][0]['y']#위도
                        lon = data['addresses'][0]['x']#경도
                        lat_list.append(float(lat))
                        lon_list.append(float(lon))
                    except:
                        lat_list.append(0.0)
                        lon_list.append(0.0)
                else :
                    lat_list.append(0.0)
                    lon_list.append(0.0)
            except:
                lat_list.append(0.0)
                lon_list.append(0.0)
                continue
        
        schema = StructType([
            StructField("LAT", DoubleType(), False)
        ])
        rows = []
        for g in lat_list:
            rows.append(Row(LAT=g))
        lat_df = spark_session().createDataFrame(rows, schema=schema)

        schema = StructType([
            StructField("LON", DoubleType(), False)
        ])
        rows = []
        for g in lon_list:
            rows.append(Row(LON=g))
        lon_df = spark_session().createDataFrame(rows, schema=schema)

        pd_cafe = df_cafe.toPandas()
        pd_lat = lat_df.toPandas()
        pd_lon = lon_df.toPandas()
        pd_coord = pd.concat([pd_cafe, pd_lat, pd_lon], axis=1)

        df_cafe = spark_session().createDataFrame(pd_coord)
        return df_cafe

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
        
        cate_day = Row(CATE_CODE='H112', STD_DAY=std_day())
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
                                 .csv("s3a://residencebucket/raw_data/STARBUCKS/STARBUCKS.csv")
        
        df = df.filter((col('ADD_STR') != '') | col('ADD_STR').contains('None') | col('ADD_STR').contains('NULL'))
        df = df.filter(~col('ADD_STR').contains('?'))
                                 
        return df
