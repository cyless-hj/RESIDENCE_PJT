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


class AnimalTransformer:

    GEO_LOCAL = Nominatim(user_agent='South Korea')

    @classmethod
    def transform(cls):
        animal_json = cls._load_json(1)
        data_len = animal_json.select('LOCALDATA_020301.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

        df_animal = cls._select_columns(animal_json)
        old_addr_list = cls._generate_old_addr_list(df_animal)
        latitude, longitude = cls._generate_lat_lon_list(df_animal)
        gu_list, dong_list = cls._generate_gu_dong_list(old_addr_list)
        df_animal = cls._add_columns(df_animal, latitude, longitude, gu_list, dong_list)
        df_animal = cls._refact_df(df_animal)
        tmp_df = df_animal

        for i in range(2, page_len + 1):
            animal_json = cls._load_json(i)
            df_animal = cls._select_columns(animal_json)
            old_addr_list = cls._generate_old_addr_list(df_animal)
            latitude, longitude = cls._generate_lat_lon_list(df_animal)
            gu_list, dong_list = cls._generate_gu_dong_list(old_addr_list)
            df_animal = cls._add_columns(df_animal, latitude, longitude, gu_list, dong_list)
            df_animal = cls._refact_df(df_animal)

            tmp_df = tmp_df.union(df_animal)

        save_data(DataWarehouse, tmp_df, 'ANIMAL_HSPT')

    @classmethod
    def _refact_df(cls, df_animal):
        df_animal = df_animal.drop(df_animal.SI_DO_CODE) \
                             .drop(df_animal.SI_DO) \
                             .drop(df_animal.GU_CODE) \
                             .drop(df_animal.DONG_CODE) \
                             .drop(df_animal.GU) \
                             .drop(df_animal.DONG) \
                             .drop(df_animal.SITEWHLADDR)

        df_animal = df_animal.withColumnRenamed('BPLCNM', 'ANI_HSPT_NAME') \
                             .withColumnRenamed('RDNWHLADDR', 'ADD_STR')

        return df_animal

    @classmethod
    def _generate_lat_lon_list(cls, df):
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
        return lat_list, lon_list

    @classmethod
    def _add_columns(cls, df, lat_list, lon_list, gu_list, dong_list):
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

        rows = []
        for g in gu_list:
            rows.append(Row(GU=g))
        gu_df = spark_session().createDataFrame(rows)

        rows = []
        for g in dong_list:
            rows.append(Row(DONG=g))
        dong_df = spark_session().createDataFrame(rows)

        cate_day = Row(CATE_CODE='F111', STD_DAY=std_day())
        rows = []
        for g in range(len(dong_list)):
            rows.append(cate_day)
        cate_day_df = spark_session().createDataFrame(rows)

        pd_df = df.toPandas()
        pd_gu = gu_df.toPandas()
        pd_dong = dong_df.toPandas()
        pd_lat = lat_df.toPandas()
        pd_lon = lon_df.toPandas()
        pd_cate_day = cate_day_df.toPandas()
        pd_df = pd.concat([pd_df, pd_gu, pd_dong, pd_lat, pd_lon, pd_cate_day], axis=1)

        df = spark_session().createDataFrame(pd_df)
        df = df.filter((col("RDNWHLADDR") != '-') & (col("GU") != '-') & (col("DONG") != '-'))

        df_loc = find_data(DataWarehouse, 'LOC')

        df = df.join(df_loc, on=['GU', 'DONG'])
        return df

    @classmethod
    def _generate_gu_dong_list(cls, old_addr_list):
        gu_list = []
        dong_list = []
        for i in range(len(old_addr_list)):
            gu_list.append(old_addr_list[i].split()[1])
            dong_list.append(old_addr_list[i].split()[2])
        return gu_list, dong_list

    @classmethod
    def _generate_old_addr_list(cls, df_animal):
        old_addr_list = df_animal.select('SITEWHLADDR').rdd.flatMap(lambda x: x).collect()
        address = []
        for i in range(len(old_addr_list)):
            a = old_addr_list[i].split(' ')
            a = " ".join(a[0:4])
            a = a.split(',')[0]
            address.append(a)
        old_addr_list = address
        return old_addr_list

    @classmethod
    def _select_columns(cls, animal_json):
        tmp = animal_json.select('LOCALDATA_020301.row').first()[0]

        df_animal = spark_session().createDataFrame(tmp)
        df_animal = df_animal.select('TRDSTATENM', 'SITEWHLADDR', 'RDNWHLADDR', 'BPLCNM')
        df_animal = df_animal.select('*').where(df_animal.TRDSTATENM == '영업/정상')
        df_animal = df_animal.drop(df_animal.TRDSTATENM)
        df_animal = df_animal.na.drop(how='any')
        df_animal = df_animal.filter((col('SITEWHLADDR') != '') | col('SITEWHLADDR').contains('None') | col('SITEWHLADDR').contains('NULL'))
        df_animal = df_animal.filter((col('RDNWHLADDR') != '') | col('RDNWHLADDR').contains('None') | col('RDNWHLADDR').contains('NULL'))
        df_animal = df_animal.filter((col('BPLCNM') != '') | col('BPLCNM').contains('None') | col('BPLCNM').contains('NULL'))
        return df_animal

    @classmethod
    def _load_json(cls, i):
        animal_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/ANIMAL_HSPT/ANIMAL_HSPT_" + std_day() + "_" + str(i) + ".json")
        return animal_json

    @classmethod
    def geocoding(cls, address):
        try:
            geo = cls.GEO_LOCAL.geocode(address)
            x_y = [geo.latitude, geo.longitude]
            return x_y

        except Exception as e:
            return [0.0, 0.0]
