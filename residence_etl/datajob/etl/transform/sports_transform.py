from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql import Row
import pandas as pd
from infra.util import std_day
from pyspark.sql.types import *
import requests

class SportsTransformer:
    @classmethod
    def transform(cls):
        df_sport = cls._load_csv()
        df_sport = cls._add_addr_gu_dong(df_sport)
        df_sport = cls._add_cate_day(df_sport)
        df_sport = cls._add_loc_idx(df_sport)
        
        df_sport = df_sport.withColumn('SPORT_CODE', monotonically_increasing_id())
        df_sport_name = df_sport.select('SPORT_CODE', 'SPORT_NAME', 'STD_DAY')
        df_sport = df_sport.select('SPORT_CODE', 'FIELD_NAME', 'STD_DAY', 'CATE_CODE', 'LAT', 'LON', 'ADD_STR', 'LOC_IDX')
        
        save_data(DataWarehouse, df_sport, 'SPORT_FACILITY')
        save_data(DataWarehouse, df_sport_name, 'SPORT_NAME')

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
    def _add_cate_day(cls, df):
        nm_list = df.select('GU').rdd.flatMap(lambda x: x).collect()
        cate_day = Row(CATE_CODE='F123', STD_DAY=std_day())
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
                        sido = data['results'][0]['region']['area1']['name']
                        gu = data['results'][0]['region']['area2']['name']
                        dong = data['results'][0]['region']['area3']['name']
                        ro = data['results'][0]['land']['name']
                        ro_num = data['results'][0]['land']['number1']
                        str_add = ' '.join([str(sido), str(gu), str(ro), str(ro_num)])
                        str_addr_list.append(str(str_add))
                        gu_list.append(str(gu))
                        dong_list.append(str(dong))
                    except:
                        str_addr_list.append('-')
                        gu_list.append('-')
                        dong_list.append('-')
                else :
                    str_addr_list.append('-')
                    gu_list.append('-')
                    dong_list.append('-')
            except:
                str_addr_list.append('-')
                gu_list.append('-')
                dong_list.append('-')
                continue

        schema = StructType([
            StructField("ADD_STR", StringType(), False)
        ])
        rows = []
        for g in str_addr_list:
            rows.append(Row(ADD_STR=g))
        add_str_df = spark_session().createDataFrame(rows, schema=schema)

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
        pd_add_str = add_str_df.toPandas()
        pd_gu = gu_df.toPandas()
        pd_dong = dong_df.toPandas()
        pd_df = pd.concat([pd_df, pd_add_str, pd_gu, pd_dong], axis=1)

        df = spark_session().createDataFrame(pd_df)
        df = df.filter((col("ADD_STR") != '-') & (col("GU") != '-') & (col("DONG") != '-'))
        return df

    @classmethod
    def _load_csv(cls):
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/SPORT_FACILITY/SPORT_FACILITY.csv")
                                 
        return df
