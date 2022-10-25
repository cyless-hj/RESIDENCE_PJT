from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from geopy.geocoders import Nominatim
from pyspark.sql import Row
import pandas as pd
from infra.util import std_day
from pyspark.sql.types import *
import requests

class AcademyTransformer:

    GEO_LOCAL = Nominatim(user_agent='South Korea')

    @classmethod
    def transform(cls):
        academy_json = cls._load_json(1)

        data_len = academy_json.select('neisAcademyInfo.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

        df_academy = cls._select_columns(academy_json)
        df_academy = cls._add_dong(df_academy)
        df_academy = cls._add_cate_day_loc(df_academy)
        df_academy = cls._add_coord(df_academy)

        tmp_df = df_academy

        for i in range(2, page_len + 1):
            academy_json = cls._load_json(i)

            df_academy = cls._select_columns(academy_json)
            df_academy = cls._add_dong(df_academy)
            df_academy = cls._add_cate_day_loc(df_academy)
            df_academy = cls._add_coord(df_academy)

            tmp_df = tmp_df.union(df_academy)

        save_data(DataWarehouse, tmp_df, 'ACADEMY')

    @classmethod
    def _add_coord(cls, df_academy):
        addr_list = df_academy.select('ADD_STR').rdd.flatMap(lambda x: x).collect()
        lat_list = []
        lon_list = []
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

        pd_academy = df_academy.toPandas()
        pd_lat = lat_df.toPandas()
        pd_lon = lon_df.toPandas()
        pd_coord = pd.concat([pd_academy, pd_lat, pd_lon], axis=1)

        df_academy = spark_session().createDataFrame(pd_coord)
        return df_academy

    @classmethod
    def _add_cate_day_loc(cls, df_academy):
        nm_list = df_academy.select('ACA_NM').rdd.flatMap(lambda x: x).collect()
        cate_day = Row(CATE_CODE='G111', STD_DAY=std_day())
        rows = []
        for g in range(len(nm_list)):
            rows.append(cate_day)
        cate_day_df = spark_session().createDataFrame(rows)

        pd_academy = df_academy.toPandas()
        pd_cate_day = cate_day_df.toPandas()
        df_academy = pd.concat([pd_academy, pd_cate_day], axis=1)
        
        df_academy = spark_session().createDataFrame(df_academy)

        df_academy = df_academy.withColumnRenamed('ADMST_ZONE_NM', 'GU') \
                               .withColumnRenamed('ACA_NM', 'ACA_NAME') \
                               .withColumnRenamed('FA_RDNMA', 'ADD_STR')

        df_loc = find_data(DataWarehouse, 'LOC')

        df_academy = df_academy.join(df_loc, on=['GU', 'DONG'])

        df_academy = df_academy.drop(df_loc.SI_DO_CODE) \
                               .drop(df_academy.SI_DO) \
                               .drop(df_academy.GU_CODE) \
                               .drop(df_academy.GU) \
                               .drop(df_academy.DONG) \
                               .drop(df_academy.DONG_CODE)
                               
        return df_academy

    @classmethod
    def _add_dong(cls, df_academy):
        dong_list = []
        addr_list = df_academy.select('FA_RDNDA').rdd.flatMap(lambda x: x).collect()
        for i in range(len(addr_list)):
            dong = addr_list[i].split('(')[-1]
            dong = dong.split(')')[0]
            dong = dong.split('/')[0]
            dong = dong.split(' ')[-1]
            dong_list.append(dong)
            if (dong_list[i].strip()[-1:] != '동') & (dong_list[i].strip()[-1:] != '가'):
                dong_list[i] = '-'

        rows = []
        for g in dong_list:
            rows.append(Row(DONG=g))
        dong_df = spark_session().createDataFrame(rows)

        df_academy = df_academy.withColumn('idx', monotonically_increasing_id())
        dong_df = dong_df.withColumn('idx', monotonically_increasing_id())

        merge_tmp = df_academy.join(dong_df, on='idx')
        df_academy = merge_tmp.drop(merge_tmp.idx)
        df_academy = df_academy.drop(df_academy.FA_RDNDA)

        df_academy = df_academy.filter(col("DONG") != '-')
        return df_academy

    @classmethod
    def _select_columns(cls, academy_json):
        tmp = academy_json.select('neisAcademyInfo.row').first()[0]

        df_academy = spark_session().createDataFrame(tmp)
        df_academy = df_academy.select('ADMST_ZONE_NM', 'ACA_NM', 'FA_RDNMA', 'FA_RDNDA')
        return df_academy
    
    @classmethod
    def _load_json(cls, i):
        academy_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/ACADEMY/ACADEMY_" + std_day() + "_" + str(i) + ".json")
        return academy_json
