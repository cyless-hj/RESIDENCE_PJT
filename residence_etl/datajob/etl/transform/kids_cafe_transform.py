from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql.functions import isnan,when,count
from pyspark.sql import Row
from pyspark.sql.types import *
import pandas as pd
from infra.util import std_day
import requests

class KidsCafeTransformer:


    @classmethod
    def transform(cls):
        kidsc_json = cls._load_json(1)
        data_len = kidsc_json.select('LOCALDATA_030708.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

        df_kidsc = cls._select_columns(kidsc_json)
        df_kidsc = cls._add_gu_dong_cate_day(df_kidsc)
        df_kidsc = cls._add_lat_lon(df_kidsc)
        df_kidsc = cls._add_loc_idx(df_kidsc)
        df_kidsc = cls._refact_df(df_kidsc)

        tmp_df = df_kidsc

        for i in range(2, page_len + 1):
            kidsc_json = cls._load_json(i)
            df_kidsc = cls._select_columns(kidsc_json)
            df_kidsc = cls._add_gu_dong_cate_day(df_kidsc)
            df_kidsc = cls._add_lat_lon(df_kidsc)
            df_kidsc = cls._add_loc_idx(df_kidsc)
            df_kidsc = cls._refact_df(df_kidsc)

            tmp_df = tmp_df.union(df_kidsc)

        save_data(DataWarehouse, tmp_df, 'KIDS_CAFE')
    
    @classmethod
    def _refact_df(cls, df):
        df = df.withColumnRenamed('BPLCNM', 'KIDS_CAFE_NAME') \
               .withColumnRenamed('RDNWHLADDR', 'ADD_STR')

        return df

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
    def _add_lat_lon(cls, df):
        addr_split_list = []
        add_list = df.select('RDNWHLADDR').rdd.flatMap(lambda x: x).collect()
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
                        lat = data['addresses'][0]['y']#??????
                        lon = data['addresses'][0]['x']#??????
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

        pd_gym = df.toPandas()
        pd_lat = lat_df.toPandas()
        pd_lon = lon_df.toPandas()
        pd_coord = pd.concat([pd_gym, pd_lat, pd_lon], axis=1)

        df = spark_session().createDataFrame(pd_coord)
        return df

    @classmethod
    def _add_gu_dong_cate_day(cls, df):
        gu_list = []
        dong_list = []
        old_add_list = df.select('SITEWHLADDR').rdd.flatMap(lambda x: x).collect()

        for i in old_add_list:
            gu = i.split(' ')[1]
            dong = i.split(' ')[2]
            gu_list.append(gu)
            dong_list.append(dong)
        
        cate_day = Row(CATE_CODE='G131', STD_DAY=std_day())
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

        pd_golf = df.toPandas()
        pd_gu = gu_df.toPandas()
        pd_dong = dong_df.toPandas()
        pd_cate_day = cate_day_df.toPandas()
        pd_df = pd.concat([pd_golf, pd_gu, pd_dong, pd_cate_day], axis=1)

        df = spark_session().createDataFrame(pd_df)

        df = df.drop(df.SITEWHLADDR)
        return df

    @classmethod
    def _select_columns(cls, cd_json):
        tmp = cd_json.select('LOCALDATA_030708.row').first()[0]

        df = spark_session().createDataFrame(tmp)
        df = df.select('BPLCNM', 'RDNWHLADDR', 'SITEWHLADDR', 'TRDSTATENM')
        df = df.select('*').where(df.TRDSTATENM == '??????/??????')
        df = df.drop(df.TRDSTATENM)
        df = df.na.drop(how='any')
        df = df.filter((col('BPLCNM') != '') | col('BPLCNM').contains('None') | col('BPLCNM').contains('NULL'))
        df = df.filter((col('RDNWHLADDR') != '') | col('RDNWHLADDR').contains('None') | col('RDNWHLADDR').contains('NULL'))
        df = df.filter((col('SITEWHLADDR') != '') | col('SITEWHLADDR').contains('None') | col('SITEWHLADDR').contains('NULL'))
        df = df.filter((col('TRDSTATENM') != '') | col('TRDSTATENM').contains('None') | col('TRDSTATENM').contains('NULL'))
        return df

    @classmethod
    def _load_json(cls, i):
        cd_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/KIDS_CAFE/KIDS_CAFE_" + std_day() + "_" + str(i) + ".json")
        return cd_json

