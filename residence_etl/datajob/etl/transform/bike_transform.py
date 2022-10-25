from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql import Row
import pandas as pd
from infra.util import std_day
from pyspark.sql.types import *
import requests

class BikeTransformer:
    @classmethod
    def transform(cls):
        bike_json = cls._load_json(1)
        data_len = bike_json.select('bikeStationMaster.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

        df_bike = cls._select_columns(bike_json)
        df_bike = cls._add_cate_day(df_bike)
        df_bike = cls._add_old_addr(df_bike)
        df_bike = cls._add_dong_gu(df_bike)
        df_bike = cls._add_loc_idx(df_bike)

        tmp_df = df_bike

        for i in range(2, page_len + 1):
            bike_json = cls._load_json(i)
            df_bike = cls._select_columns(bike_json)
            df_bike = cls._add_cate_day(df_bike)
            df_bike = cls._add_old_addr(df_bike)
            df_bike = cls._add_dong_gu(df_bike)
            df_bike = cls._add_loc_idx(df_bike)

            tmp_df = tmp_df.union(df_bike)

        save_data(DataWarehouse, tmp_df, 'BIKE')

    @classmethod
    def _add_loc_idx(cls, df_bike):
        df_loc = find_data(DataWarehouse, 'LOC')

        df_bike = df_bike.join(df_loc, on=['GU', 'DONG'])

        df_bike = df_bike.drop(df_bike.SI_DO_CODE) \
                         .drop(df_bike.SI_DO) \
                         .drop(df_bike.GU_CODE) \
                         .drop(df_bike.GU) \
                         .drop(df_bike.DONG) \
                         .drop(df_bike.DONG_CODE) \
                         .drop(df_bike.OLD_ADDR)

        df_bike = df_bike.withColumnRenamed('STATN_ADDR1', 'ADD_STR') \
                         .withColumnRenamed('STATN_LAT', 'LAT') \
                         .withColumnRenamed('STATN_LNT', 'LON')

        return df_bike

    @classmethod
    def _add_dong_gu(cls, df_bike):
        gu_list = []
        dong_list = []
        old_addr_list = df_bike.select('OLD_ADDR').rdd.flatMap(lambda x: x).collect()
        for i in range(len(old_addr_list)):
            gu_list.append(old_addr_list[i].split()[1])
            dong_list.append(old_addr_list[i].split()[2])

        rows = []
        for g in gu_list:
            rows.append(Row(GU=g))
        gu_df = spark_session().createDataFrame(rows)

        rows = []
        for g in dong_list:
            rows.append(Row(DONG=g))
        dong_df = spark_session().createDataFrame(rows)

        pd_bike = df_bike.toPandas()
        pd_gu = gu_df.toPandas()
        pd_dong = dong_df.toPandas()
        pd_gu_dong = pd.concat([pd_bike, pd_gu, pd_dong], axis=1)

        df_bike = spark_session().createDataFrame(pd_gu_dong)
        return df_bike

    @classmethod
    def _add_old_addr(cls, df_bike):
        addr_list = df_bike.select('STATN_ADDR1').rdd.flatMap(lambda x: x).collect()
        old_add_list = []
        client_id = ""
        client_secret = ""
        for addr in addr_list:
            try:
                url = f"https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode?query={addr}"
                headers = {'X-NCP-APIGW-API-KEY-ID': client_id,
                        'X-NCP-APIGW-API-KEY': client_secret
                        }

                r = requests.get(url, headers=headers)

                if r.status_code == 200:
                    try:
                        data = r.json()
                        Address = data['addresses'][0]['jibunAddress'] # 지번 주소
                        old_add_list.append(str(Address))
                    except:
                        old_add_list.append('-')
                else :
                    old_add_list.append('-')
            except:
                old_add_list.append('-')
                continue

        schema = StructType([
            StructField("OLD_ADDR", StringType(), False)
        ])
        rows = []
        for g in old_add_list:
            rows.append(Row(OLD_ADDR=g))
        old_add_df = spark_session().createDataFrame(rows, schema=schema)

        pd_bike = df_bike.toPandas()
        pd_old_add = old_add_df.toPandas()
        pd_old = pd.concat([pd_bike, pd_old_add], axis=1)

        df_bike = spark_session().createDataFrame(pd_old)
        df_bike = df_bike.filter(col("OLD_ADDR") != '-')
        return df_bike

    @classmethod
    def _add_cate_day(cls, df_bike):

        nm_list = df_bike.select('STATN_ADDR1').rdd.flatMap(lambda x: x).collect()
        cate_day = Row(CATE_CODE='A111', STD_DAY=std_day())
        rows = []
        for g in range(len(nm_list)):
            rows.append(cate_day)
        cate_day_df = spark_session().createDataFrame(rows)

        pd_bike = df_bike.toPandas()
        pd_cate_day = cate_day_df.toPandas()
        df_bike = pd.concat([pd_bike, pd_cate_day], axis=1)

        df_bike = spark_session().createDataFrame(df_bike)
        return df_bike

    @classmethod
    def _select_columns(cls, bike_json):
        tmp = bike_json.select('bikeStationMaster.row').first()[0]

        df_bike = spark_session().createDataFrame(tmp)
        df_bike = df_bike.select('STATN_ADDR1', 'STATN_LAT', 'STATN_LNT')
        return df_bike

    @classmethod
    def _load_json(cls, i):
        df_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/BIKE/BIKE_" + std_day() + "_" + str(i) + ".json")
        return df_json
