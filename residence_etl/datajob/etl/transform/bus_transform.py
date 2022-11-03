import json
from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql import Row
import pandas as pd
from infra.util import std_day
from pyspark.sql.types import *
import requests

class BusTransformer:
    @classmethod
    def transform(cls):
        bus_json = cls._load_json(1)
        data_len = bus_json.select('busStopLocationXyInfo.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

        df_bus = cls._select_columns(bus_json)
        df_bus = cls._add_cate_day(df_bus)
        df_bus = cls._add_addr_gu_dong(df_bus)
        df_bus = cls._add_loc_idx(df_bus)

        tmp_df = df_bus

        for i in range(2, page_len + 1):
            bus_json = cls._load_json(i)
            df_bus = cls._select_columns(bus_json)
            df_bus = cls._add_cate_day(df_bus)
            df_bus = cls._add_addr_gu_dong(df_bus)
            df_bus = cls._add_loc_idx(df_bus)

            tmp_df = tmp_df.union(df_bus)

        save_data(DataWarehouse, tmp_df, 'BUS')

    @classmethod
    def _add_loc_idx(cls, df_bus):
        df_loc = find_data(DataWarehouse, 'LOC')

        df_bus = df_bus.join(df_loc, on=['GU', 'DONG'])

        df_bus = df_bus.drop(df_bus.SI_DO_CODE) \
                         .drop(df_bus.SI_DO) \
                         .drop(df_bus.GU_CODE) \
                         .drop(df_bus.GU) \
                         .drop(df_bus.DONG) \
                         .drop(df_bus.DONG_CODE)

        df_bus = df_bus.withColumnRenamed('STOP_NM', 'BUS_NAME') \
                         .withColumnRenamed('YCODE', 'LAT') \
                         .withColumnRenamed('XCODE', 'LON')
                         
        return df_bus

    @classmethod
    def _add_addr_gu_dong(cls, df_bus):
        coord_list = []
        lat_list = df_bus.select('YCODE').rdd.flatMap(lambda x: x).collect()
        lon_list = df_bus.select('XCODE').rdd.flatMap(lambda x: x).collect()
        for i in range(len(lat_list)):
            coord = ', '.join([lon_list[i], lat_list[i]])
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

        pd_bus = df_bus.toPandas()
        pd_add_str = add_str_df.toPandas()
        pd_gu = gu_df.toPandas()
        pd_dong = dong_df.toPandas()
        pd_df = pd.concat([pd_bus, pd_add_str, pd_gu, pd_dong], axis=1)

        df_bus = spark_session().createDataFrame(pd_df)
        df1 = df_bus.filter((col("ADD_STR") == '-') | (col("GU") == '-') | (col("DONG") == '-'))
        df1.show(100)
        print((df1.count(), len(df1.columns)))
        df_bus = df_bus.filter((col("ADD_STR") != '-') & (col("GU") != '-') & (col("DONG") != '-'))
        return df_bus

    @classmethod
    def _add_cate_day(cls, df_bus):
        nm_list = df_bus.select('STOP_NM').rdd.flatMap(lambda x: x).collect()
        cate_day = Row(CATE_CODE='A113', STD_DAY=std_day())
        rows = []
        for g in range(len(nm_list)):
            rows.append(cate_day)
        cate_day_df = spark_session().createDataFrame(rows)

        pd_bus = df_bus.toPandas()
        pd_cate_day = cate_day_df.toPandas()
        df_bus = pd.concat([pd_bus, pd_cate_day], axis=1)
        
        df_bus = spark_session().createDataFrame(df_bus)
        return df_bus

    @classmethod
    def _select_columns(cls, json):
        tmp = json.select('busStopLocationXyInfo.row').first()[0]

        df = spark_session().createDataFrame(tmp)
        df = df.select('STOP_NM', 'YCODE', 'XCODE')
        return df

    @classmethod
    def _load_json(cls, i):
        df_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/BUS/BUS_" + std_day() + "_" + str(i) + ".json")
        return df_json
