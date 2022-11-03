from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql import Row
import pandas as pd
from infra.util import std_day
from pyspark.sql.types import *
import requests

class VeganTransformer:
    @classmethod
    def transform(cls):
        vegan_json = cls._load_json(1)
        data_len = vegan_json.select('CrtfcUpsoInfo.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

        df_vegan = cls._select_columns(vegan_json)
        df_vegan = cls._add_cate_day_addr(df_vegan)
        df_vegan = cls._add_dong(df_vegan)
        df_vegan = cls._add_loc_idx(df_vegan)
        df_vegan = cls._refact_df(df_vegan)

        tmp_df = df_vegan

        for i in range(2, page_len + 1):
            vegan_json = cls._load_json(i)
            df_vegan = cls._select_columns(vegan_json)
            df_vegan = cls._add_cate_day_addr(df_vegan)
            df_vegan = cls._add_dong(df_vegan)
            df_vegan = cls._add_loc_idx(df_vegan)
            df_vegan = cls._refact_df(df_vegan)

            tmp_df = tmp_df.union(df_vegan)

        save_data(DataWarehouse, tmp_df, 'VEGAN')

    @classmethod
    def _refact_df(cls, df_vegan):
        df_vegan = df_vegan.withColumnRenamed('UPSO_NM', 'VEGAN_NAME') \
                           .withColumnRenamed('Y_DNTS', 'LAT') \
                           .withColumnRenamed('X_CNTS', 'LON')
                           
        return df_vegan

    @classmethod
    def _add_cate_day_addr(cls, df_vegan):
        gu_code_list = df_vegan.select('CGG_CODE').rdd.flatMap(lambda x: x).collect()
        gu_list = df_vegan.select('CGG_CODE_NM').rdd.flatMap(lambda x: x).collect()
        str_addr_list = df_vegan.select('RDN_CODE_NM').rdd.flatMap(lambda x: x).collect()

        for i in range(len(str_addr_list)):
            gu_code_list = list(map(str, gu_code_list))
            if gu_code_list[i] in str_addr_list[i]:
                a = str_addr_list[i].split()
                a[1] = gu_list[i]
                a = list(map(str, a))
                str_addr_list[i] = ' '.join(a)
        
        cate_day = Row(CATE_CODE='H113', STD_DAY=std_day())
        rows = []
        for g in range(len(str_addr_list)):
            rows.append(cate_day)
        cate_day_df = spark_session().createDataFrame(rows)

        schema = StructType([
            StructField("ADD_STR", StringType(), False)
        ])
        rows = []
        for g in str_addr_list:
            rows.append(Row(ADD_STR=g))
        str_add_df = spark_session().createDataFrame(rows, schema=schema)

        pd_vegan = df_vegan.toPandas()
        pd_cate_day = cate_day_df.toPandas()
        pd_str_add = str_add_df.toPandas()

        pd_df = pd.concat([pd_vegan, pd_cate_day, pd_str_add], axis=1)

        df_vegan = spark_session().createDataFrame(pd_df)
        df_vegan = df_vegan.drop(df_vegan.RDN_CODE_NM) \
                           .drop(df_vegan.CGG_CODE)
        return df_vegan

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
    def _add_dong(cls, df):
        add_list = []
        str_addr_list = df.select('ADD_STR').rdd.flatMap(lambda x: x).collect()
        for i in str_addr_list:
            sp_add = str(i).split()[0:4]
            sp_add = ' '.join(sp_add)
            sp_add = sp_add.split('(')[0]
            add_list.append(sp_add)
        
        dong_list = []
        client_id = ""
        client_secret = ""
        for addr in add_list:
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

        schema = StructType([
            StructField("DONG", StringType(), False)
        ])
        rows = []
        for g in dong_list:
            rows.append(Row(DONG=g))
        dong_df = spark_session().createDataFrame(rows, schema=schema)

        pd_df = df.toPandas()
        pd_dong = dong_df.toPandas()
        pd_df = pd.concat([pd_df, pd_dong], axis=1)

        df = spark_session().createDataFrame(pd_df)
        df = df.withColumnRenamed('CGG_CODE_NM', 'GU')
        df = df.filter((col("ADD_STR") != '-') & (col("GU") != '-') & (col("DONG") != '-'))
        #df = df.drop(df.HALF_ADD)
        return df

    @classmethod
    def _load_csv(cls):
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/VEGAN/VEGAN.csv")
                                 
        #df = df.filter((col('ADD_STR') != '') | col('ADD_STR').contains('None') | col('ADD_STR').contains('NULL'))
        #df = df.filter(~col('ADD_STR').contains('?'))
        return df
    
    @classmethod
    def _select_columns(cls, vegan_json):
        tmp = vegan_json.select('CrtfcUpsoInfo.row').first()[0]

        df_vegan = spark_session().createDataFrame(tmp)
        df_vegan = df_vegan.select('UPSO_NM', 'CGG_CODE', 'CGG_CODE_NM', 'RDN_CODE_NM', 'Y_DNTS', 'X_CNTS')
        return df_vegan

    @classmethod
    def _load_json(cls, i):
        vegan_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/VEGAN/VEGAN_" + std_day() + "_" + str(i) + ".json")
        return vegan_json
