from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql import Row
import pandas as pd
from infra.util import std_day
from pyspark.sql.types import *
import requests

class ChildMedTransformer:
    @classmethod
    def transform(cls):
        cd_json = cls._load_json(1)
        data_len = cd_json.select('TnFcltySttusInfo10074.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

        cd_df = cls._select_columns(cd_json)
        cd_df = cls._add_dong(cd_df)
        cd_df = cls._add_cate_day(cd_df)
        cd_df = cls._add_loc_idx(cd_df)

        tmp_df = cd_df

        for i in range(2, page_len + 1):
            cd_json = cls._load_json(i)

            cd_df = cls._select_columns(cd_json)
            cd_df = cls._add_dong(cd_df)
            cd_df = cls._add_cate_day(cd_df)
            cd_df = cls._add_loc_idx(cd_df)

            tmp_df = tmp_df.union(cd_df)


        save_data(DataWarehouse, tmp_df, 'CHILD_MED')

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
        tmp = cd_json.select('TnFcltySttusInfo10074.row').first()[0]

        df = spark_session().createDataFrame(tmp)
        df = df.select('CLTUR_EVENT_ETC_NM', 'ATDRC_NM', 'Y_CRDNT_VALUE', 'X_CRDNT_VALUE', 'BASS_ADRES')
        df = df.na.drop(how='any')
        df = df.filter((col('CLTUR_EVENT_ETC_NM') != '') | col('CLTUR_EVENT_ETC_NM').contains('None') | col('CLTUR_EVENT_ETC_NM').contains('NULL'))
        df = df.filter((col('ATDRC_NM') != '') | col('ATDRC_NM').contains('None') | col('ATDRC_NM').contains('NULL'))
        df = df.filter((col('Y_CRDNT_VALUE') != '') | col('Y_CRDNT_VALUE').contains('None') | col('Y_CRDNT_VALUE').contains('NULL'))
        df = df.filter((col('X_CRDNT_VALUE') != '') | col('X_CRDNT_VALUE').contains('None') | col('X_CRDNT_VALUE').contains('NULL'))
        df = df.filter((col('BASS_ADRES') != '') | col('BASS_ADRES').contains('None') | col('BASS_ADRES').contains('NULL'))
        df = df.withColumnRenamed('CLTUR_EVENT_ETC_NM', 'C_MED_NAME') \
               .withColumnRenamed('ATDRC_NM', 'GU') \
               .withColumnRenamed('Y_CRDNT_VALUE', 'LAT') \
               .withColumnRenamed('X_CRDNT_VALUE', 'LON') \
               .withColumnRenamed('BASS_ADRES', 'ADD_STR')
        return df

    @classmethod
    def _load_json(cls, i):
        cd_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/CHILD_MED/CHILD_MED_" + std_day() + "_" + str(i) + ".json")
        return cd_json

    @classmethod
    def _add_cate_day(cls, df):
        nm_list = df.select('GU').rdd.flatMap(lambda x: x).collect()
        cate_day = Row(CATE_CODE='G122', STD_DAY=std_day())
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
    def _add_dong(cls, df):
        coord_list = []
        lat_list = df.select('LAT').rdd.flatMap(lambda x: x).collect()
        lon_list = df.select('LON').rdd.flatMap(lambda x: x).collect()
        for i in range(len(lat_list)):
            coord = ', '.join([str(lon_list[i]), str(lat_list[i])])
            coord_list.append(coord)

        str_addr_list = []
        gu_list = []
        dong_list = []
        client_id = ""
        client_secret = ""
        output = "json"
        orders = 'roadaddr'
        for c in coord_list:
            try:
                url = f"https://naveropenapi.apigw.ntruss.com/map-reversegeocode/v2/gc?coords={c}&output={output}&orders={orders}"
                headers = {'X-NCP-APIGW-API-KEY-ID': client_id,
                        'X-NCP-APIGW-API-KEY': client_secret
                        }

                r = requests.get(url, headers=headers)

                if r.status_code == 200:
                    try:
                        data = r.json()
                        dong = data['results'][0]['region']['area3']['name']
                        dong_list.append(str(dong))
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
        df = df.filter((col("ADD_STR") != '-') & (col("GU") != '-') & (col("DONG") != '-'))
        return df
