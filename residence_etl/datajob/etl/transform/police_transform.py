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
from infra.util import std_day


class PoliceTransformer:

    @classmethod
    def transform(cls):

        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/POLICE/POLICE.csv", encoding='cp949')

        

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

        # df_temp(API에서 불러온 구,동)와 pandas_df2(원본 df) concat 하기
        # 구, 동 없는 값을 넣기
        df=pd.concat([df_temp,df], axis=1)
        # df.loc[15,'region_2depth_name']='종로구'
        # df.loc[15,'region_3depth_name']='교북동'
        # df.loc[62,'region_2depth_name']='동대문구'
        # df.loc[62,'region_3depth_name']='장안동'
        # df.loc[78,'region_2depth_name']='영등포구'
        # df.loc[78,'region_3depth_name']='신길동'
        # df.loc[84,'region_2depth_name']='영등포구'
        # df.loc[84,'region_3depth_name']='영등포동'
        # df.loc[155,'region_2depth_name']='강서구'
        # df.loc[155,'region_3depth_name']='화곡동'
        # df.loc[208,'region_2depth_name']='송파구'
        # df.loc[208,'region_3depth_name']='풍납동'
        # df.loc[232,'region_2depth_name']='도봉구'
        # df.loc[232,'region_3depth_name']='도봉동'


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
                lat.append('없음')
                lon.append('없음')

        lat_lon = np.array([lat,lon]).T
        df_temp2 = pd.DataFrame(lat_lon, columns = ['LAT','LON'])

        df_temp2.tail()

        #df와 위경도값 concat 하기
        df2=pd.concat([df,df_temp2], axis=1)

        #없는 위경도값  넣기
        # df2.loc[15,'LAT']='126.962199186657'
        # df2.loc[15,'LON']='37.571813578256'
        # df2.loc[62,'LAT']='127.065004726024'
        # df2.loc[62,'LON']='37.5696342993256'
        # df2.loc[78,'LAT']='126.912786804025'
        # df2.loc[78,'LON']='37.4990895649306'
        # df2.loc[84,'LAT']='126.901481165754'
        # df2.loc[84,'LON']='37.5134499023277'
        # df2.loc[155,'LAT']='126.849656371385'
        # df2.loc[155,'LON']='37.551558168546'
        # df2.loc[208,'LAT']='127.117094888156'
        # df2.loc[208,'LON']='37.5341648236009'
        # df2.loc[232,'LAT']='127.043412012604'
        # df2.loc[232,'LON']='37.6794736285741'

        df2.rename(columns={'region_2depth_name':'GU','region_3depth_name':'DONG'}, inplace=True)
        df2['STD_DAY'] = std_day()
        
        

        df2
        print(df2)

        #df2 다시 spark로 되돌리기
        df2=spark_session().createDataFrame(df2)

        #LOC 추가
        df_loc = find_data(DataWarehouse, 'LOC')
        
        df_loc
        print(df_loc)

        df3 = df2.join(df_loc, on=['GU', 'DONG'])
        
        df3 = df3.drop(df3.SI_DO_CODE) \
                        .drop(df3.GU_CODE) \
                        .drop(df3.GU) \
                        .drop(df3.DONG) \
                        .drop(df3.DONG_CODE) \
                        .drop(df3.LOCATION) \
                        .drop(df3.SI_DO)

        # df3.select([count(when(isnull(c), c)).alias(c) for c in df2.columns]).show()
        
        df3.show()

        df4=df3.withColumn('CATE_CODE',lit("B113"))
        save_data(DataWarehouse, df4, 'POLICE')