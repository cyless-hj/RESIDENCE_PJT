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
import requests
from infra.util import std_day

class ParkTransformer:

    @classmethod
    def transform(cls):

        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/PARK/PARK.csv", encoding='cp949')
        df=df.drop(df.GU)
        df.show()
        

        #주소 변환 API에 넣을 도로명 주소만 추출
        addrs_s=df.select('ADD_STR').collect()
        addrs_list=[]
        for row in addrs_s:
            rows=row['ADD_STR']
            # print(row['ADD_STR'])
            addrs_list.append(rows)


        addrs_list

        #주소 변환 API
        locations = []
        for addr in addrs_list:
            url = 'https://dapi.kakao.com/v2/local/search/address.json?query={}'.format(addr)
            headers = {
            "Authorization": "KakaoAK "}
            place = requests.get(url, headers = headers).json()['documents']
            locations.append(place)
        
        
        # locations
       
        city = [] ## 시, 군
        town = [] ## 동, 읍, 면
        for i in range(len(locations)):
            
            try:
                city.append(locations[i][0].get('address').get('region_2depth_name'))
                town.append(locations[i][0].get('address').get('region_3depth_name'))
                
            except IndexError:
                print(i,'번째 주소 못가져옴', end ='')
                print()
                city.append('없음')
                town.append('없음')
            
            except AttributeError:
                city.append(locations[i][0].get('road_address').get('region_2depth_name'))
                town.append(locations[i][0].get('road_address').get('region_3depth_name'))
            city_town = np.array([city,town]).T
            df_temp = pd.DataFrame(city_town, columns = ['region_2depth_name','region_3depth_name'])

        #df를 판다스로
        df=df.toPandas()

        print(df)

        # df_temp(API에서 불러온 구,동)와 pandas_df2(원본 df) concat 하기
        # 구, 동 없는 값을 넣기
        df2=pd.concat([df_temp,df], axis=1)

    

        df2.rename(columns={'region_2depth_name':'GU','region_3depth_name':'DONG'}, inplace=True)
        # df2['STD_DAY'] = std_day()
        df2['STD_DAY']='2022-10-27'
        print(df2)

        
        #df2 다시 spark로 되돌리기
        df3=spark_session().createDataFrame(df2)
        df3.show()
        df3.select([count(when(isnull(c), c)).alias(c) for c in df3.columns]).show()
        #LOC 추가
        df_loc = find_data(DataWarehouse, 'LOC')
        df_loc.show()

        df3 = df3.join(df_loc, on=['GU', 'DONG'])
        df3.show()
        df3 = df3.drop(df3.SI_DO_CODE) \
                        .drop(df3.GU_CODE) \
                        .drop(df3.GU) \
                        .drop(df3.DONG) \
                        .drop(df3.DONG_CODE) \
                        .drop(df3.SI_DO)

        df3.select([count(when(isnull(c), c)).alias(c) for c in df3.columns]).show()
        

        df3=df3.withColumn('CATE_CODE',lit("I111"))
        df4=df3.distinct()

        df4.show()
        df4 = df4.na.drop()

        save_data(DataWarehouse, df4, 'PARK')