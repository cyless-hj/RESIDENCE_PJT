from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from pyspark.sql.functions import isnan,when,count
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import split,isnull,lit
import numpy as np
from infra.util import std_day
import pandas as pd
import io
import numpy as np
import requests
import time



class HospitalTransformer:

    @classmethod
    def transform(cls):
        #1.파일 불러오기
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/HOSPITAL/HOSPITAL.csv", encoding='cp949')

        #ADD_STR DROP 하기
        df = df.drop(df.ADD_STR)
                    

        #2.네이버 api 불러오기                         
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
        
        dong_df.show()

        pd_df = df.toPandas()
        pd_add_str = add_str_df.toPandas()
        pd_gu = gu_df.toPandas()
        pd_dong2 = dong_df.toPandas()

        
        pd_df3 = pd.concat([pd_df, pd_add_str, pd_gu, pd_dong2], axis=1)
        
        
        df2_2 = spark_session().createDataFrame(pd_df3)
        df3 = df2_2.filter((col("ADD_STR") != '-') & (col("GU") != '-') & (col("DONG") != '-'))
        df3.show()

        df3.select([count(when(isnull(c), c)).alias(c) for c in df3.columns]).show()
   


        #3.카테고리 추가
        nm_list = df3.select('GU').rdd.flatMap(lambda x: x).collect()
        cate_day = Row(CATE_CODE='D111', STD_DAY=std_day())
        

        rows = []
        for g in range(len(nm_list)):
            rows.append(cate_day)
        cate_day_df = spark_session().createDataFrame(rows)
        
        pd_df3 = df3.toPandas()
        pd_cate_day = cate_day_df.toPandas()
        df4 = pd.concat([pd_df3, pd_cate_day], axis=1)

        # df4
        
        df5 = spark_session().createDataFrame(df4)
        # return df5

        df5.show()


        #4.loc 추가
        df_loc = find_data(DataWarehouse, 'LOC')

        df5 = df5.join(df_loc, on=['GU', 'DONG'])

        df6 = df5.drop(df5.SI_DO_CODE) \
                         .drop(df5.SI_DO) \
                         .drop(df5.GU_CODE) \
                         .drop(df5.GU) \
                         .drop(df5.DONG) \
                         .drop(df5.DONG_CODE)

        #Hospital code 추가
        df6 = df6.withColumn('HOSPITAL_CODE', monotonically_increasing_id())

        #5.HOSPITAL_WEEK, HOSPITAL_HOL 나누기
        df_hos=df6.select('HOSPITAL_CODE','HOSPITAL_NAME','HOSPITAL_TYPE','STD_DAY','CATE_CODE','LAT','LON','ADD_STR','LOC_IDX')
        
        df_week=df6.select('HOSPITAL_CODE','HOSPITAL_NAME','START_MON','START_TUE','START_WED','START_TUR','START_FRI','END_MON'
                            ,'END_TUE','END_WED','END_TUR','END_FRI','STD_DAY')
        df_hol=df6.select('HOSPITAL_CODE','HOSPITAL_NAME','START_SAT','START_SUN','START_HOL','END_SAT','END_SUN','END_HOL'
                            ,'STD_DAY')


        save_data(DataWarehouse, df_hos, 'HOSPITAL')
        save_data(DataWarehouse, df_week, 'HOSPITAL_WEEK')
        save_data(DataWarehouse, df_hol, 'HOSPITAL_HOL')