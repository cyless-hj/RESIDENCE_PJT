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



class PharmTransformer:

   
    @classmethod
    def transform(cls):
        
        pharm_json = cls._load_json(1)
        data_len = pharm_json.select('TbPharmacyOperateInfo.list_total_count').first()[0]
        page_len = data_len // 1000 + 1

    
        ############################################################

        tmp = pharm_json.select('TbPharmacyOperateInfo.row').first()[0]

        df_pharm = spark_session().createDataFrame(tmp)
        # df_pharm.show()

        df_pharm = df_pharm.drop(df_pharm.DUTYTEL1).drop(df_pharm.HPID).drop(df_pharm.POSTCDN1) \
                    .drop(df_pharm.POSTCDN2).drop(df_pharm.WORK_DTTM).drop(df_pharm.DUTYADDR)

        df_pharm = df_pharm.na.drop(how='any')
        
        df=df_pharm.withColumnRenamed('DUTYNAME','PHARMACY_NAME').withColumnRenamed('DUTYTIME1C','END_MON') \
            .withColumnRenamed('DUTYTIME1S','START_MON').withColumnRenamed('DUTYTIME2C','END_TUE') \
            .withColumnRenamed('DUTYTIME2S','START_TUE').withColumnRenamed('DUTYTIME3C','END_WED') \
            .withColumnRenamed('DUTYTIME3S','START_WED').withColumnRenamed('DUTYTIME4C','END_TUR') \
            .withColumnRenamed('DUTYTIME4S','START_TUR').withColumnRenamed('DUTYTIME5C','END_FRI') \
            .withColumnRenamed('DUTYTIME5S','START_FRI').withColumnRenamed('DUTYTIME6C','END_SAT') \
            .withColumnRenamed('DUTYTIME6S','START_SAT').withColumnRenamed('DUTYTIME7C','END_SUN') \
            .withColumnRenamed('DUTYTIME7S','START_SUN')\
            .withColumnRenamed('DUTYTIME8S','START_HOL').withColumnRenamed('DUTYTIME8C','END_HOL') \
            .withColumnRenamed('WGS84LAT','LAT').withColumnRenamed('WGS84LON','LON') \

        # df.show()

        #2.위경도 추출
        coord_list2 = []
        lat_list = df.select('LAT').rdd.flatMap(lambda x: x).collect()
        lon_list = df.select('LON').rdd.flatMap(lambda x: x).collect()
        for i in range(len(lat_list)):
            coord = ', '.join([str(lon_list[i]), str(lat_list[i])])
            coord_list2.append(coord)

        #3.동,구로 변환
        str_addr_list = []
        gu_list = []
        dong_list = []
        client_id = ""
        client_secret = ""
        output = "json"
        orders = 'roadaddr'
        for c in coord_list2:
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
        print(pd_df3)
        
        df2_2 = spark_session().createDataFrame(pd_df3)
        df2_2.show()
        df3 = df2_2.filter((col("ADD_STR") != '-') & (col("GU") != '-') & (col("DONG") != '-'))
        

        df3.select([count(when(isnull(c), c)).alias(c) for c in df3.columns]).show()
        df3.show()
    

        #4.카테고리 추가
        nm_list = df3.select('GU').rdd.flatMap(lambda x: x).collect()
        cate_day = Row(CATE_CODE='D112', STD_DAY=std_day())
        

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

        #5.loc 추가
        df_loc = find_data(DataWarehouse, 'LOC')

        df5 = df5.join(df_loc, on=['GU', 'DONG'])

        df6 = df5.drop(df5.SI_DO_CODE) \
                        .drop(df5.SI_DO) \
                        .drop(df5.GU_CODE) \
                        .drop(df5.GU) \
                        .drop(df5.DONG) \
                        .drop(df5.DONG_CODE)
        


        tmp=df6


#############################################################################################################################33    
        for i in range(2, page_len+1):
            pharm_json = cls._load_json(i)

            pharm_json = pharm_json.select('TbPharmacyOperateInfo.row').first()[0]

            df_pharm = spark_session().createDataFrame(pharm_json)
            # df_pharm.show()

            df_pharm = df_pharm.drop(df_pharm.DUTYTEL1).drop(df_pharm.HPID).drop(df_pharm.POSTCDN1) \
                        .drop(df_pharm.POSTCDN2).drop(df_pharm.WORK_DTTM).drop(df_pharm.DUTYADDR)

            df_pharm = df_pharm.na.drop(how='any')
            
            df=df_pharm.withColumnRenamed('DUTYNAME','PHARMACY_NAME').withColumnRenamed('DUTYTIME1C','END_MON') \
                .withColumnRenamed('DUTYTIME1S','START_MON').withColumnRenamed('DUTYTIME2C','END_TUE') \
                .withColumnRenamed('DUTYTIME2S','START_TUE').withColumnRenamed('DUTYTIME3C','END_WED') \
                .withColumnRenamed('DUTYTIME3S','START_WED').withColumnRenamed('DUTYTIME4C','END_TUR') \
                .withColumnRenamed('DUTYTIME4S','START_TUR').withColumnRenamed('DUTYTIME5C','END_FRI') \
                .withColumnRenamed('DUTYTIME5S','START_FRI').withColumnRenamed('DUTYTIME6C','END_SAT') \
                .withColumnRenamed('DUTYTIME6S','START_SAT').withColumnRenamed('DUTYTIME7C','END_SUN') \
                .withColumnRenamed('DUTYTIME7S','START_SUN')\
                .withColumnRenamed('DUTYTIME8S','START_HOL').withColumnRenamed('DUTYTIME8C','END_HOL') \
                .withColumnRenamed('WGS84LAT','LAT').withColumnRenamed('WGS84LON','LON') \

            # df.show()

            #2.위경도 추출
            coord_list2 = []
            lat_list = df.select('LAT').rdd.flatMap(lambda x: x).collect()
            lon_list = df.select('LON').rdd.flatMap(lambda x: x).collect()
            for i in range(len(lat_list)):
                coord = ', '.join([str(lon_list[i]), str(lat_list[i])])
                coord_list2.append(coord)

            #3.동,구로 변환
            str_addr_list = []
            gu_list = []
            dong_list = []
            client_id = "82j2oahdh7"
            client_secret = "FlHN77WtJeImSb8RvDSmiVUQCyex7bbWt24yp8oy"
            output = "json"
            orders = 'roadaddr'
            for c in coord_list2:
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
            print(pd_df3)
            
            df2_2 = spark_session().createDataFrame(pd_df3)
            # df2_2.show()
            df3 = df2_2.filter((col("ADD_STR") != '-') & (col("GU") != '-') & (col("DONG") != '-'))
            

            df3.select([count(when(isnull(c), c)).alias(c) for c in df3.columns]).show()
            df3.show()
    

            #4.카테고리 추가
            nm_list = df3.select('GU').rdd.flatMap(lambda x: x).collect()
            cate_day = Row(CATE_CODE='D112', STD_DAY=std_day())
            

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

            # df5.show()

            #5.loc 추가
            df_loc = find_data(DataWarehouse, 'LOC')

            df5 = df5.join(df_loc, on=['GU', 'DONG'])

            df6 = df5.drop(df5.SI_DO_CODE) \
                            .drop(df5.SI_DO) \
                            .drop(df5.GU_CODE) \
                            .drop(df5.GU) \
                            .drop(df5.DONG) \
                            .drop(df5.DONG_CODE)
            
            #6.PHARMACY_CODE 추가
            

            #7.PHARMACY_WEEK, PHARMACY_HOL 나누기
            
            tmp=tmp.union(df6)

            # tmp_pharm.show()

        tmp=tmp.distinct()

        df6 = tmp.withColumn('PHARMACY_CODE', monotonically_increasing_id())

        
        #7.PHARMACY_WEEK, PHARMACY_HOL 나누기
        df_pharm_fin=df6.select('PHARMACY_CODE','PHARMACY_NAME','STD_DAY','CATE_CODE','LAT','LON','ADD_STR','LOC_IDX')
        
        df_week=df6.select('PHARMACY_CODE','PHARMACY_NAME','START_MON','START_TUE','START_WED','START_TUR','START_FRI','END_MON'
                            ,'END_TUE','END_WED','END_TUR','END_FRI','STD_DAY')
        df_hol=df6.select('PHARMACY_CODE','PHARMACY_NAME','START_SAT','START_SUN','START_HOL','END_SAT','END_SUN','END_HOL'
                            ,'STD_DAY')
        
        save_data(DataWarehouse, df_pharm_fin, 'PHARMACY')
        save_data(DataWarehouse, df_week, 'PHARM_WEEK')
        save_data(DataWarehouse, df_hol, 'PHARM_HOL')

    @classmethod
    def _load_json(cls, i):
        pharm_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/PHARMACY/PHARMACY_" + std_day() + "_" + str(i) + ".json")
        #pharm_json = spark_session().read.format("json").json("s3a://residencebucket/raw_data/PHARMACY/PHARMACY_" + "2022-10-21"+ "_" + str(i) + ".json")
        return pharm_json

        
    
        