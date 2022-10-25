from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql.functions import isnan,when,count
from pyspark.sql import Row
from pyspark.sql.types import *
import requests
from infra.util import std_day
import pandas as pd


class KinderTransformer:

    @classmethod
    def transform(cls):
        kinder_json = cls._load_json(1)
        data_len = kinder_json.select('ChildCareInfo.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

        df_kinder = cls._select_columns(kinder_json)
        df_kinder = cls._add_lat_lon_old_addr(df_kinder)
        df_kinder = cls._add_gu_dong_cate_day(df_kinder)
        df_kinder = cls._add_loc_idx(df_kinder)
        df_kinder = cls._refact_df(df_kinder)
        tmp_df = df_kinder

        for i in range(2, page_len + 1):
            kinder_json = cls._load_json(1)
            df_kinder = cls._select_columns(kinder_json)
            df_kinder = cls._add_lat_lon_old_addr(df_kinder)
            df_kinder = cls._add_gu_dong_cate_day(df_kinder)
            df_kinder = cls._add_loc_idx(df_kinder)
            df_kinder = cls._refact_df(df_kinder)

            tmp_df = tmp_df.union(df_kinder)

        save_data(DataWarehouse, tmp_df, 'KINDERGARTEN')

    @classmethod
    def _refact_df(cls, df_kinder):
        df_kinder = df_kinder.withColumnRenamed('CRNAME', 'KIN_NAME') \
                             .withColumnRenamed('CRTYPENAME', 'KIN_TYPE') \
                             .withColumnRenamed('CRADDR', 'ADD_STR') \
                             .withColumnRenamed('EM_CNT_0Y', '1YEAR_SRVC') \
                             .withColumnRenamed('EM_CNT_1Y', '1_2YEAR_SRVC') \
                             .withColumnRenamed('EM_CNT_2Y', '2_4YEAR_SRVC') \
                             .withColumnRenamed('EM_CNT_4Y', '4_6YEAR_SRVC') \
                             .withColumnRenamed('EM_CNT_6Y', '6YEAR_SRVC') \
                             .withColumnRenamed('EM_CNT_TOT', 'NUM_TCHR')
                             
        return df_kinder

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
    def _select_columns(cls, cd_json):
        tmp = cd_json.select('ChildCareInfo.row').first()[0]

        df = spark_session().createDataFrame(tmp)
        df = df.select('CRNAME', 'CRTYPENAME', 'CRSTATUSNAME', 'CRADDR', 'EM_CNT_0Y', 'EM_CNT_1Y', 'EM_CNT_2Y', 'EM_CNT_4Y', 'EM_CNT_6Y', 'EM_CNT_TOT')
        df = df.select('*').where(df.CRSTATUSNAME == '정상')
        df = df.drop(df.CRSTATUSNAME)
        df = df.na.drop(how='any')
        # df = df.filter((col('SIGUNNAME') != '') | col('SIGUNNAME').contains('None') | col('SIGUNNAME').contains('NULL'))
        # df = df.filter((col('CRNAME') != '') | col('CRNAME').contains('None') | col('CRNAME').contains('NULL'))
        # df = df.filter((col('CRTYPENAME') != '') | col('CRTYPENAME').contains('None') | col('CRTYPENAME').contains('NULL'))
        # df = df.filter((col('CRADDR') != '') | col('CRADDR').contains('None') | col('CRADDR').contains('NULL'))
        # df = df.filter((col('EM_CNT_0Y') != '') | col('EM_CNT_0Y').contains('None') | col('EM_CNT_0Y').contains('NULL'))
        # df = df.filter((col('EM_CNT_1Y') != '') | col('EM_CNT_1Y').contains('None') | col('EM_CNT_1Y').contains('NULL'))
        # df = df.filter((col('EM_CNT_2Y') != '') | col('EM_CNT_2Y').contains('None') | col('EM_CNT_2Y').contains('NULL'))
        # df = df.filter((col('EM_CNT_4Y') != '') | col('EM_CNT_4Y').contains('None') | col('EM_CNT_4Y').contains('NULL'))
        # df = df.filter((col('EM_CNT_6Y') != '') | col('EM_CNT_6Y').contains('None') | col('EM_CNT_6Y').contains('NULL'))
        return df

    @classmethod
    def _add_gu_dong_cate_day(cls, df):
        gu_list = []
        dong_list = []
        old_add_list = df.select('OLD_ADD').rdd.flatMap(lambda x: x).collect()

        for i in old_add_list:
            gu = i.split(' ')[1]
            dong = i.split(' ')[2]
            gu_list.append(gu)
            dong_list.append(dong)
        
        cate_day = Row(CATE_CODE='G121', STD_DAY=std_day())
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

        pd_cafe = df.toPandas()
        pd_gu = gu_df.toPandas()
        pd_dong = dong_df.toPandas()
        pd_cate_day = cate_day_df.toPandas()
        pd_df = pd.concat([pd_cafe, pd_gu, pd_dong, pd_cate_day], axis=1)

        df = spark_session().createDataFrame(pd_df)

        df = df.drop(df.OLD_ADD)
        return df

    @classmethod
    def _add_lat_lon_old_addr(cls, df):
        addr_split_list = []
        add_list = df.select('CRADDR').rdd.flatMap(lambda x: x).collect()
        for i in add_list:
            sp_add = str(i).split()[0:4]
            sp_add = ' '.join(sp_add)
            addr_split_list.append(sp_add)
        
        old_add_list = []
        lat_list = []
        lon_list = []
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
                        lat = data['addresses'][0]['y']#위도
                        lon = data['addresses'][0]['x']#경도
                        old_add_list.append(old_add)
                        lat_list.append(float(lat))
                        lon_list.append(float(lon))
                    except:
                        old_add_list.append('-')
                        lat_list.append(0.0)
                        lon_list.append(0.0)
                else :
                    old_add_list.append('-')
                    lat_list.append(0.0)
                    lon_list.append(0.0)
            except:
                old_add_list.append('-')
                lat_list.append(0.0)
                lon_list.append(0.0)
                continue

        schema = StructType([
            StructField("OLD_ADD", StringType(), False)
        ])

        rows = []
        for g in old_add_list:
            rows.append(Row(OLD_ADD=g))
        old_add_df = spark_session().createDataFrame(rows, schema=schema)
        
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

        pd_kinder = df.toPandas()
        pd_old_add = old_add_df.toPandas()
        pd_lat = lat_df.toPandas()
        pd_lon = lon_df.toPandas()
        pd_coord = pd.concat([pd_kinder, pd_old_add, pd_lat, pd_lon], axis=1)

        df = spark_session().createDataFrame(pd_coord)
        df = df.filter((col("OLD_ADD") != '-'))
        return df

    @classmethod
    def _load_json(cls, i):
        cd_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/KINDERGARTEN/KINDERGARTEN_" + std_day() + "_" + str(i) + ".json")
        return cd_json

