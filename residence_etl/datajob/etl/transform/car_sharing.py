import json
from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql.functions import isnan,when,count
from pyspark.sql import Row
from pyspark.sql.types import *
import requests
from infra.util import std_day


class CarSharingTransformer:


    @classmethod
    def transform(cls):

        car_json = cls._load_json(1)
        data_len = car_json.select('NanumcarSpotList.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

        df_car = cls._generate_df(car_json)
        gu_list, dong_list = cls._generate_gu_dong_list(df_car)
        df_car = cls._add_columns(df_car, gu_list, dong_list)
        df_car = cls._refact_df(df_car)
        tmp_df = df_car

        for i in range(2, page_len + 1):
            car_json = cls._load_json(i)

            df_car = cls._generate_df(car_json)
            gu_list, dong_list = cls._generate_gu_dong_list(df_car)
            df_car = cls._add_columns(df_car, gu_list, dong_list)
            df_car = cls._refact_df(df_car)

            tmp_df = tmp_df.union(df_car)

        save_data(DataWarehouse, tmp_df, 'CAR_SHARING')

    @classmethod
    def _refact_df(cls, df_car):
        df_car = df_car.drop(df_car.SI_DO_CODE) \
                       .drop(df_car.SI_DO) \
                       .drop(df_car.GU_CODE) \
                       .drop(df_car.DONG_CODE) \
                       .drop(df_car.GU) \
                       .drop(df_car.DONG)

        df_car = df_car.withColumnRenamed('LA', 'LAT') \
                       .withColumnRenamed('LO', 'LON') \
                       .withColumnRenamed('POSITN_NM', 'CAR_SHR_NAME') \
                       .withColumnRenamed('ELCTYVHCLE_AT', 'CAR_SHR_TYPE') \
                       .withColumnRenamed('ADRES', 'ADD_STR')
                       
        return df_car

    @classmethod
    def _add_columns(cls, df_car, gu_list, dong_list):
        rows = []
        for g in gu_list:
            rows.append(Row(GU=g))
        gu_df = spark_session().createDataFrame(rows)

        rows = []
        for g in dong_list:
            rows.append(Row(DONG=g))
        dong_df = spark_session().createDataFrame(rows)

        cate_day = Row(CATE_CODE='F121', STD_DAY=std_day())
        rows = []
        for g in range(len(dong_list)):
            rows.append(cate_day)
        cate_day_df = spark_session().createDataFrame(rows)

        df_car = df_car.withColumn('idx', monotonically_increasing_id())
        gu_df = gu_df.withColumn('idx', monotonically_increasing_id())
        dong_df = dong_df.withColumn('idx', monotonically_increasing_id())
        cate_day_df = cate_day_df.withColumn('idx', monotonically_increasing_id())

        merge_tmp = df_car.join(gu_df, on='idx')
        merge_tmp = merge_tmp.join(dong_df, on='idx')
        merge_tmp = merge_tmp.join(cate_day_df, on='idx')
        df_car = merge_tmp.drop(merge_tmp.idx)

        df_loc = find_data(DataWarehouse, 'LOC')

        df_car = df_car.join(df_loc, on=['GU', 'DONG'])
        return df_car

    @classmethod
    def _generate_df(cls, car_json):
        tmp = car_json.select('NanumcarSpotList.row').first()[0]

        df_car = spark_session().createDataFrame(tmp)
        df_car = df_car.drop(df_car.POSITN_CD)
        df_car = df_car.na.drop(how='any')
        df_car = df_car.filter((col('ADRES') != '') | col('ADRES').contains('None') | col('ADRES').contains('NULL'))
        df_car = df_car.filter((col('ELCTYVHCLE_AT') != '') | col('ELCTYVHCLE_AT').contains('None') | col('ELCTYVHCLE_AT').contains('NULL'))
        df_car = df_car.filter((col('LA') != '') | col('LA').contains('None') | col('LA').contains('NULL'))
        df_car = df_car.filter((col('LO') != '') | col('LO').contains('None') | col('LO').contains('NULL'))
        df_car = df_car.filter((col('POSITN_NM') != '') | col('POSITN_NM').contains('None') | col('POSITN_NM').contains('NULL'))
        return df_car



    @classmethod
    def _generate_gu_dong_list(cls, df_car):
        old_addr_list = df_car.select('ADRES').rdd.flatMap(lambda x: x).collect()
        gu_list = []
        dong_list = []
        for i in range(len(old_addr_list)):
            a = old_addr_list[i].split(' ')
            gu_list.append(a[1])
            dong_list.append(a[2])
        return gu_list, dong_list

    @classmethod
    def _load_json(cls, i):
        car_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/CAR_SHARING/CAR_SHARING_" + std_day() + "_" + str(i) + ".json")
        return car_json