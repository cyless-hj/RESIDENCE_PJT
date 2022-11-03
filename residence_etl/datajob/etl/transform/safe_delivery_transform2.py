from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql import Row
import pandas as pd
from infra.util import std_day
from pyspark.sql.types import *
import requests

class SafeTransformer2:
    @classmethod
    def transform(cls):
        safe_json = cls._load_json(1)
        data_len = safe_json.select('safeOpenBox.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

        safe_df = cls._select_columns(safe_json)
        safe_df = cls._add_addr_gu_dong(safe_df)
        safe_df = cls._add_cate_day(safe_df)
        safe_df = cls._add_loc_idx(safe_df)

        tmp_df = safe_df

        for i in range(2, page_len + 1):
            safe_json = cls._load_json(i)

            safe_df = cls._select_columns(safe_json)
            safe_df = cls._add_addr_gu_dong(safe_df)
            safe_df = cls._add_cate_day(safe_df)
            safe_df = cls._add_loc_idx(safe_df)

            tmp_df = tmp_df.union(safe_df)

        print((tmp_df.count(), len(tmp_df.columns)))
        tmp_df.show()
        #save_data(DataWarehouse, tmp_df, 'SAFE_DELIVERY')

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
    def _select_columns(cls, safe_json):
        tmp = safe_json.select('safeOpenBox.row').first()[0]

        df = spark_session().createDataFrame(tmp)
        df = df.select('ANSIMINM', 'ADDRDETAIL', 'ANSIMIADDR', 'WGSXPT', 'WGSYPT')
        df = df.na.drop(how='any')
        df = df.filter((col('ANSIMINM') != '') | col('ANSIMINM').contains('None') | col('ANSIMINM').contains('NULL'))
        df = df.filter((col('ADDRDETAIL') != '') | col('ADDRDETAIL').contains('None') | col('ADDRDETAIL').contains('NULL'))
        df = df.filter((col('ANSIMIADDR') != '') | col('ANSIMIADDR').contains('None') | col('ANSIMIADDR').contains('NULL'))
        df = df.filter((col('WGSXPT') != '') | col('WGSXPT').contains('None') | col('WGSXPT').contains('NULL'))
        df = df.filter((col('WGSYPT') != '') | col('WGSYPT').contains('None') | col('WGSYPT').contains('NULL'))
        df = df.withColumnRenamed('ANSIMINM', 'SAFE_DLVR_NAME') \
               .withColumnRenamed('ADDRDETAIL', 'GU') \
               .withColumnRenamed('ANSIMIADDR', 'HALF_ADD') \
               .withColumnRenamed('WGSXPT', 'LAT') \
               .withColumnRenamed('WGSYPT', 'LON')
        return df

    @classmethod
    def _load_json(cls, i):
        safe_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/SAFE_DELIVERY/SAFE_DELIVERY_" + std_day() + "_" + str(i) + ".json")
        return safe_json

    @classmethod
    def _add_cate_day(cls, df):
        nm_list = df.select('GU').rdd.flatMap(lambda x: x).collect()
        cate_day = Row(CATE_CODE='B111', STD_DAY=std_day())
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
    def _add_addr_gu_dong(cls, df):
        str_add_list = []
        gu_list = df.select('GU').rdd.flatMap(lambda x: x).collect()
        half_add_list = df.select('HALF_ADD').rdd.flatMap(lambda x: x).collect()
        for i in range(len(gu_list)):
            str_add = ' '.join(['서울특별시', gu_list[i], half_add_list[i]])
            str_add_list.append(str_add)
        
        dong_list = []
        client_id = ""
        client_secret = ""
        for addr in str_add_list:
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
            StructField("ADD_STR", StringType(), False)
        ])
        rows = []
        for g in str_add_list:
            rows.append(Row(ADD_STR=g))
        add_str_df = spark_session().createDataFrame(rows, schema=schema)

        schema = StructType([
            StructField("DONG", StringType(), False)
        ])
        rows = []
        for g in dong_list:
            rows.append(Row(DONG=g))
        dong_df = spark_session().createDataFrame(rows, schema=schema)

        pd_df = df.toPandas()
        pd_add_str = add_str_df.toPandas()
        pd_dong = dong_df.toPandas()
        pd_df = pd.concat([pd_df, pd_add_str, pd_dong], axis=1)

        df = spark_session().createDataFrame(pd_df)
        df = df.filter((col("ADD_STR") != '-') & (col("GU") != '-') & (col("DONG") != '-'))
        df = df.drop(df.HALF_ADD)
        return df
