from pyspark.sql.functions import col, monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from pyspark.sql.functions import isnan,when,count
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import split, isnull, lit, coalesce
import numpy as np
from infra.util import std_day
import pandas as pd
import io
import numpy as np
import requests
from infra.util import std_day
from io import StringIO
import boto3

class RetailTransformer:

    @classmethod
    def transform(cls):

        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/RETAIL/RETAIL.csv", encoding='cp949')
        
        
        df = df.filter((col('OPENCLOSE') == '정상영업'))

        df=df.drop(df.OPENCLOSE)
        df.show()
        
        #백화점은 원본에서 거름
        
        #지번주소 구, 동 나누기
        df_split=df.withColumn('SI_DO_OLD', split(df['ADD_OLD'],' ').getItem(0)) \
                .withColumn('GU_OLD', split(df['ADD_OLD'],' ').getItem(1)) \
                .withColumn('DONG_OLD', split(df['ADD_OLD'],' ').getItem(2))
        
        ## 도로명주소 구, 동 추출
        # #주소 변환 API에 넣을 도로명 주소만 추출
        addrs_s=df.select('ADD_STR').collect()
        addrs_list=[]
        for row in addrs_s:
            rows=row['ADD_STR']
            # print(row['ADD_STR'])
            addrs_list.append(rows)
        

        # #주소 변환 API
        locations = []
        for addr in addrs_list:
            url = 'https://dapi.kakao.com/v2/local/search/address.json?query={}'.format(addr)
            headers = {
            "Authorization": "KakaoAK "}
            place = requests.get(url, headers = headers).json()['documents']
            locations.append(place)
        
        
        # # locations
       
        city = [] ## 시, 군
        town = [] ## 동, 읍, 면
        for i in range(len(locations)):
            
            try:
                city.append(locations[i][0].get('address').get('region_2depth_name'))
                town.append(locations[i][0].get('address').get('region_3depth_name'))
                
            except IndexError:
                print(i,'번째 주소 못가져옴', end ='')
                print()
                city.append('')
                town.append('')
            
            except AttributeError:
                city.append(locations[i][0].get('road_address').get('region_2depth_name'))
                town.append(locations[i][0].get('road_address').get('region_3depth_name'))
            city_town = np.array([city,town]).T
            df_temp = pd.DataFrame(city_town, columns = ['region_2depth_name','region_3depth_name'])

        df_split_pd=df_split.toPandas()
        df2=pd.concat([df_split_pd,df_temp], axis=1)

        df2.rename(columns={'region_2depth_name':'GU_STR','region_3depth_name':'DONG_STR'}, inplace=True)


        print(df2)
        #nan 값 모두 null 로 바꿔줘야 함
        print(df2.isnull().sum())


        #위경도값 가져오기
        lat = [] ## 위도
        lon = [] ## 경도
        for i in range(len(locations)):
            
            try:
                lat.append(locations[i][0].get('address').get('x'))
                lon.append(locations[i][0].get('address').get('y'))
                
            except IndexError:
                print(i,'번째 위경도 못가져옴', end ='')
                print()
                lat.append(0.0)
                lon.append(0.0)
                continue

        lat_lon = np.array([lat,lon]).T
        df_temp2 = pd.DataFrame(lat_lon, columns = ['LAT','LON'])
        
        df_temp2.tail()

        #df와 위경도값 concat 하기
        df3=pd.concat([df2,df_temp2], axis=1)
        df3['STD_DAY'] = std_day()


        df3=spark_session().createDataFrame(df3)
        df3.show()

        #NAN 값 모두 NULL 으로 
        df3 = df3.replace(float('nan'), None)
        df3=df3.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df3.columns])

        df3.show(50)


        #COALESCE 함수로 구, 동 채워주기
        df4 = df3.withColumn("GU", F.coalesce(df3.GU_STR,df3.GU_OLD))
        df5 = df4.withColumn("DONG", F.coalesce(df4.DONG_STR,df4.DONG_OLD))

        # df3=F.coalesce(df3['GU'],df3['GU_OLD'])
        # df3=F.coalesce(df3['DONG'],df3['DONG_OLD'] )
    
        df5.show()

        #도로명주소&지번주소
        df5 = df5.withColumn("ADD_STROLD",F.coalesce(df5['ADD_STR'],df5['ADD_OLD'] ))
        df5.show()
        #LOC 추가
        df_loc = find_data(DataWarehouse, 'LOC')
        

        df5 = df5.join(df_loc, on=['GU', 'DONG'])

        #데이터프레임의 모든 결측치 세기
        df5.select([count(when(isnull(c), c)).alias(c) for c in df5.columns]).show()
        
        df5 = df5.drop(df5.SI_DO_CODE) \
                        .drop(df5.GU_CODE) \
                        .drop(df5.GU) \
                        .drop(df5.DONG) \
                        .drop(df5.DONG_CODE) \
                        .drop(df5.SI_DO) \
                        .drop(df5.ADD_OLD) \
                        .drop(df5.ADD_STR) \
                        .drop(df5.SI_DO_OLD) \
                        .drop(df5.GU_OLD) \
                        .drop(df5.DONG_OLD) \
                        .drop(df5.GU_STR) \
                        .drop(df5.DONG_STR)
        df5.show()


        #카테고리코드 채우기
        df5=df5.withColumn('CATE_CODE',lit("C114"))

        #데이터프레임의 모든 결측치 세기
        df5.select([count(when(isnull(c), c)).alias(c) for c in df5.columns]).show()

        df5=df5.na.fill(0.0)

        

        save_data(DataWarehouse, df5, 'RETAIL')