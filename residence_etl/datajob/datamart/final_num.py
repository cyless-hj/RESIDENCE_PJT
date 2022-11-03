from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from pyspark.sql.functions import col, ceil, lit
import pandas as pd
from infra.util import std_day
from infra.spark_session import spark_session
from pyspark.sql import functions as F

import boto3
from io import StringIO

#STD DAY로 필터걸기
class SubSampleDataMart:
        #'SI_DO','GU','SI_DO','GU','DONG_CODE'
    @classmethod
    def save(cls):
        loc = find_data(DataWarehouse, 'LOC')
        loc = loc.select('SI_DO','GU','DONG_CODE', 'DONG')
        pd_loc = loc.toPandas()

        subway = find_data(DataMart, 'SUBWAY')
        subway = subway.select('SI_DO','GU','DONG_CODE', 'DONG')
        subway = subway.filter(col("STD_DAY") == std_day())
        subway_g = subway.groupby(subway.DONG_CODE, subway.DONG).count()
        subway_g = subway_g.withColumnRenamed('count', 'SUBWAY_NUM')
        pd_sub_way = subway_g.toPandas()

        pd_tot = pd.merge(pd_loc, pd_sub_way, on=['DONG_CODE', 'DONG'], how='outer')

        starbucks = find_data(DataMart, 'STARBUCKS')
        starbucks = starbucks.select('SI_DO','GU','DONG_CODE', 'DONG')
        starbucks = starbucks.filter(col("STD_DAY") == std_day())
        starbucks_g = starbucks.groupby(starbucks.DONG_CODE, starbucks.DONG).count()
        starbucks_g = starbucks_g.withColumnRenamed('count', 'STARBUCKS_NUM')
        pd_starbucks = starbucks_g.toPandas()
        # starbucks_g.show()

        pd_tot = pd.merge(pd_tot, pd_starbucks, on=['DONG_CODE', 'DONG'], how='outer')
        # print(pd_tot)

        sport = find_data(DataMart, 'SPORT_FACILITY')
        sport = sport.select('SI_DO','GU','DONG_CODE', 'DONG')
        sport = sport.filter(col("STD_DAY") == std_day())
        sport_g = sport.groupby(sport.DONG_CODE, sport.DONG).count()
        sport_g = sport_g.withColumnRenamed('count', 'SPORT_NUM')
        pd_sport = sport_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_sport, on=['DONG_CODE', 'DONG'], how='outer')
        print(pd_tot)

        safed = find_data(DataMart, 'SAFE_DELIVERY')
        safed = safed.select('SI_DO','GU','DONG_CODE', 'DONG')
        safed = safed.filter(col("STD_DAY") == std_day())
        safed_g = safed.groupby(safed.DONG_CODE, safed.DONG).count()
        safed_g = safed_g.withColumnRenamed('count', 'SAFE_DLVR_NUM')
        pd_safed = safed_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_safed, on=['DONG_CODE', 'DONG'], how='outer')

        police = find_data(DataMart, 'POLICE')
        police = police.select('SI_DO','GU','DONG_CODE', 'DONG')
        police = police.filter(col("STD_DAY") == std_day())
        police_g = police.groupby(police.DONG_CODE, police.DONG).count()
        police_g = police_g.withColumnRenamed('count', 'POLICE_NUM')
        pd_police = police_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_police, on=['DONG_CODE', 'DONG'], how='outer')

        pharm = find_data(DataMart, 'PHARMACY')
        pharm = pharm.select('SI_DO','GU','DONG_CODE', 'DONG')
        pharm = pharm.filter(col("STD_DAY") == std_day())
        pharm_g = pharm.groupby(pharm.DONG_CODE, pharm.DONG).count()
        pharm_g = pharm_g.withColumnRenamed('count', 'PHARM_NUM')
        pd_pharm = pharm_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_pharm, on=['DONG_CODE', 'DONG'], how='outer')
        
        nv = find_data(DataMart, 'NOISE_VIBRATION')
        nv = nv.select('SI_DO','GU','DONG_CODE', 'DONG','NOISE_VIBR')
        nv = nv.filter(col("STD_DAY") == std_day())
        # nv_g = nv.groupby(nv.DONG_CODE, nv.DONG).count()
        nv_g = nv.withColumnRenamed('NOISE_VIBR', 'NOISE_VIBRATION_NUM')
        pd_nv = nv_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_nv, on=['DONG_CODE', 'DONG'], how='outer')

        middle = find_data(DataMart, 'MIDDLE')
        middle = middle.select('SI_DO','GU','DONG_CODE', 'DONG')
        middle = middle.filter(col("STD_DAY") == std_day())
        middle_g = middle.groupby(middle.DONG_CODE, middle.DONG).count()
        middle_g = middle_g.withColumnRenamed('count', 'MID_SCH_NUM')
        pd_middle = middle_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_middle, on=['DONG_CODE', 'DONG'], how='outer')

        mcdonalds = find_data(DataMart, 'MCDONALDS')
        mcdonalds = mcdonalds.select('SI_DO','GU','DONG_CODE', 'DONG')
        mcdonalds = mcdonalds.filter(col("STD_DAY") == std_day())
        mcdonalds_g = mcdonalds.groupby(mcdonalds.DONG_CODE, mcdonalds.DONG).count()
        mcdonalds_g = mcdonalds_g.withColumnRenamed('count', 'MC_NUM')
        pd_mcdonalds = mcdonalds_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_mcdonalds, on=['DONG_CODE', 'DONG'], how='outer')

        leisure = find_data(DataMart, 'LEISURE')
        leisure = leisure.select('SI_DO','GU','DONG_CODE', 'DONG')
        leisure = leisure.filter(col("STD_DAY") == std_day())
        leisure_g = leisure.groupby(leisure.DONG_CODE, leisure.DONG).count()
        leisure_g = leisure_g.withColumnRenamed('count', 'LEISURE_NUM')
        pd_leisure = leisure_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_leisure, on=['DONG_CODE', 'DONG'], how='outer')

        kinder = find_data(DataMart, 'KINDERGARTEN')
        kinder = kinder.select('SI_DO','GU','DONG_CODE', 'DONG')
        kinder = kinder.filter(col("STD_DAY") == std_day())
        kinder_g = kinder.groupby(kinder.DONG_CODE, kinder.DONG).count()
        kinder_g = kinder_g.withColumnRenamed('count', 'KINDER_NUM')
        pd_kinder = kinder_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_kinder, on=['DONG_CODE', 'DONG'], how='outer')

        kidsc = find_data(DataMart, 'KIDS_CAFE')
        kidsc = kidsc.select('SI_DO','GU','DONG_CODE', 'DONG')
        kidsc = kidsc.filter(col("STD_DAY") == std_day())
        kidsc_g = kidsc.groupby(kidsc.DONG_CODE, kidsc.DONG).count()
        kidsc_g = kidsc_g.withColumnRenamed('count', 'KIDS_NUM')
        pd_kidsc = kidsc_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_kidsc, on=['DONG_CODE', 'DONG'], how='outer')

        hospital = find_data(DataMart, 'HOSPITAL')
        hospital = hospital.select('SI_DO','GU','DONG_CODE', 'DONG')
        hospital = hospital.filter(col("STD_DAY") == std_day())
        hospital_g = hospital.groupby(hospital.DONG_CODE, hospital.DONG).count()
        hospital_g = hospital_g.withColumnRenamed('count', 'HOSPITAL_NUM')
        pd_hospital = hospital_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_hospital, on=['DONG_CODE', 'DONG'], how='outer')

        highsch = find_data(DataMart, 'HIGH_SCHOOL')
        highsch = highsch.select('SI_DO','GU','DONG_CODE', 'DONG')
        highsch = highsch.filter(col("STD_DAY") == std_day())
        highsch_g = highsch.groupby(highsch.DONG_CODE, highsch.DONG).count()
        highsch_g = highsch_g.withColumnRenamed('count', 'HIGH_SCH_NUM')
        pd_highsch = highsch_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_highsch, on=['DONG_CODE', 'DONG'], how='outer')

        gym = find_data(DataMart, 'GYM')
        gym = gym.select('SI_DO','GU','DONG_CODE', 'DONG')
        gym = gym.filter(col("STD_DAY") == std_day())
        gym_g = gym.groupby(gym.DONG_CODE, gym.DONG).count()
        gym_g = gym_g.withColumnRenamed('count', 'GYM_NUM')
        pd_gym = gym_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_gym, on=['DONG_CODE', 'DONG'], how='outer')

        golf = find_data(DataMart, 'GOLF')
        golf = golf.select('SI_DO','GU','DONG_CODE', 'DONG')
        golf = golf.filter(col("STD_DAY") == std_day())
        golf_g = golf.groupby(golf.DONG_CODE, golf.DONG).count()
        golf_g = golf_g.withColumnRenamed('count', 'GOLF_NUM')
        pd_golf = golf_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_golf, on=['DONG_CODE', 'DONG'], how='outer')

        fire = find_data(DataMart, 'FIRE_STA')
        fire = fire.select('SI_DO','GU','DONG_CODE', 'DONG')
        fire = fire.filter(col("STD_DAY") == std_day())
        fire_g = fire.groupby(fire.DONG_CODE, fire.DONG).count()
        fire_g = fire_g.withColumnRenamed('count', 'FIRE_NUM')
        pd_fire = fire_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_fire, on=['DONG_CODE', 'DONG'], how='outer')

        ele_sch = find_data(DataMart, 'ELEMENTARY')
        ele_sch = ele_sch.select('SI_DO','GU','DONG_CODE', 'DONG')
        ele_sch = ele_sch.filter(col("STD_DAY") == std_day())
        ele_sch_g = ele_sch.groupby(ele_sch.DONG_CODE, ele_sch.DONG).count()
        ele_sch_g = ele_sch_g.withColumnRenamed('count', 'ELE_SCH_NUM')
        pd_ele_sch = ele_sch_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_ele_sch, on=['DONG_CODE', 'DONG'], how='outer')

        dept = find_data(DataMart, 'DEPARTMENT_STORE')
        dept = dept.select('SI_DO','GU','DONG_CODE', 'DONG')
        dept = dept.filter(col("STD_DAY") == std_day())
        dept_g = dept.groupby(dept.DONG_CODE, dept.DONG).count()
        dept_g = dept_g.withColumnRenamed('count', 'DPTM_NUM')
        pd_dept = dept_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_dept, on=['DONG_CODE', 'DONG'], how='outer')

        con_store = find_data(DataMart, 'CON_STORE')
        con_store = con_store.select('SI_DO','GU','DONG_CODE', 'DONG')
        con_store = con_store.filter(col("STD_DAY") == std_day())
        con_store_g = con_store.groupby(con_store.DONG_CODE, con_store.DONG).count()
        con_store_g = con_store_g.withColumnRenamed('count', 'CON_NUM')
        pd_con_store = con_store_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_con_store, on=['DONG_CODE', 'DONG'], how='outer')

        child_med = find_data(DataMart, 'CHILD_MED')
        child_med = child_med.select('SI_DO','GU','DONG_CODE', 'DONG')
        child_med = child_med.filter(col("STD_DAY") == std_day())
        child_med_g = child_med.groupby(child_med.DONG_CODE, child_med.DONG).count()
        child_med_g = child_med_g.withColumnRenamed('count', 'CHILD_MED_NUM')
        pd_child_med = child_med_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_child_med, on=['DONG_CODE', 'DONG'], how='outer')

        cctv = find_data(DataMart, 'CCTV')
        cctv = cctv.select('SI_DO','GU','DONG_CODE', 'DONG')
        cctv = cctv.filter(col("STD_DAY") == std_day())
        cctv_g = cctv.groupby(cctv.DONG_CODE, cctv.DONG).count()
        cctv_g = cctv_g.withColumnRenamed('count', 'CCTV_NUM')
        pd_cctv = cctv_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_cctv, on=['DONG_CODE', 'DONG'], how='outer')

        car = find_data(DataMart, 'CAR_SHARING')
        car = car.select('SI_DO','GU','DONG_CODE', 'DONG')
        car = car.filter(col("STD_DAY") == std_day())
        car_g = car.groupby(car.DONG_CODE, car.DONG).count()
        car_g = car_g.withColumnRenamed('count', 'CAR_SHR_NUM')
        pd_car = car_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_car, on=['DONG_CODE', 'DONG'], how='outer')

        cafe = find_data(DataMart, 'CAFE')
        cafe = cafe.select('SI_DO','GU','DONG_CODE', 'DONG')
        cafe = cafe.filter(col("STD_DAY") == std_day())
        cafe_g = cafe.groupby(cafe.DONG_CODE, cafe.DONG).count()
        cafe_g = cafe_g.withColumnRenamed('count', 'CAFE_NUM')
        pd_cafe = cafe_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_cafe, on=['DONG_CODE', 'DONG'], how='outer')

        bus = find_data(DataMart, 'BUS')
        bus = bus.select('SI_DO','GU','DONG_CODE', 'DONG')
        bus = bus.filter(col("STD_DAY") == std_day())
        bus_g = bus.groupby(bus.DONG_CODE, bus.DONG).count()
        bus_g = bus_g.withColumnRenamed('count', 'BUS_NUM')
        pd_bus = bus_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_bus, on=['DONG_CODE', 'DONG'], how='outer')

        bike = find_data(DataMart, 'BIKE')
        bike = bike.select('SI_DO','GU','DONG_CODE', 'DONG')
        bike = bike.filter(col("STD_DAY") == std_day())
        bike_g = bike.groupby(bike.DONG_CODE, bike.DONG).count()
        bike_g = bike_g.withColumnRenamed('count', 'BIKE_NUM')
        pd_bike = bike_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_bike, on=['DONG_CODE', 'DONG'], how='outer')

        animal = find_data(DataMart, 'ANIMAL_HSPT')
        animal = animal.select('SI_DO','GU','DONG_CODE', 'DONG')
        animal = animal.filter(col("STD_DAY") == std_day())
        animal_g = animal.groupby(animal.DONG_CODE, animal.DONG).count()
        animal_g = animal_g.withColumnRenamed('count', 'ANI_HSPT_NUM')
        pd_animal = animal_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_animal, on=['DONG_CODE', 'DONG'], how='outer')

        academy = find_data(DataMart, 'ACADEMY')
        academy = academy.select('SI_DO','GU','DONG_CODE', 'DONG')
        academy = academy.filter(col("STD_DAY") == std_day())
        academy_g = academy.groupby(academy.DONG_CODE, academy.DONG).count()
        academy_g = academy_g.withColumnRenamed('count', 'ACADEMY_NUM')
        pd_academy = academy_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_academy, on=['DONG_CODE', 'DONG'], how='outer')
        print(pd_tot)

        park = find_data(DataMart, 'PARK')
        park = park.select('SI_DO','GU','DONG_CODE', 'DONG')
        park = park.filter(col("STD_DAY") == std_day())
        park_g = park.groupby(park.DONG_CODE, park.DONG).count()
        park_g = park_g.withColumnRenamed('count', 'PARK_NUM')
        pd_park = park_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_park, on=['DONG_CODE', 'DONG'], how='outer')
        print(pd_tot)

        #retail

        retail = find_data(DataMart, 'RETAIL')
        retail = retail.select('SI_DO','GU','DONG_CODE', 'DONG')
        retail = retail.filter(col("STD_DAY") == std_day())
        retail_g = retail.groupby(retail.DONG_CODE, retail.DONG).count()
        retail_g = retail_g.withColumnRenamed('count', 'RETAIL_NUM')
        pd_retail = retail_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_retail, on=['DONG_CODE', 'DONG'], how='outer')
        print(pd_tot)
        
        vegan = find_data(DataMart, 'PARK')
        vegan = vegan.select('SI_DO','GU','DONG_CODE', 'DONG')
        vegan = vegan.filter(col("STD_DAY") == std_day())
        vegan_g = vegan.groupby(park.DONG_CODE, park.DONG).count()
        vegan_g = vegan_g.withColumnRenamed('count', 'VEGAN_NUM')
        pd_vegan = vegan_g.toPandas()

        pd_tot = pd.merge(pd_tot, pd_vegan, on=['DONG_CODE', 'DONG'], how='outer')
        print(pd_tot)
        
        pd_tot = pd_tot.fillna(0)
        pd_tot = pd_tot.astype({'SUBWAY_NUM':'int', 'STARBUCKS_NUM':'int', 'SPORT_NUM':'int', 'SAFE_DLVR_NUM':'int',  'KINDER_NUM':'int', 'KIDS_NUM':'int', 'GYM_NUM':'int', 'GOLF_NUM':'int'
        ,'POLICE_NUM':'int', 'PHARM_NUM':'int', 'NOISE_VIBRATION_NUM':'int', 'MID_SCH_NUM':'int' ,'MC_NUM':'int', 'LEISURE_NUM':'int','KINDER_NUM':'int','KIDS_NUM':'int'
        ,'HOSPITAL_NUM':'int','HIGH_SCH_NUM':'int','GYM_NUM':'int','GOLF_NUM':'int', 'ELE_SCH_NUM':'int', 'FIRE_NUM':'int','CON_NUM':'int','DPTM_NUM':'int'
        ,'CHILD_MED_NUM':'int','CCTV_NUM':'int','CAR_SHR_NUM':'int','CAFE_NUM':'int', 'BUS_NUM':'int'
        ,'BIKE_NUM':'int','ANI_HSPT_NUM':'int','ACADEMY_NUM':'int', 'PARK_NUM':'int', 'RETAIL_NUM':'int', 'VEGAN_NUM':'int'
        })
        df_tot=spark_session().createDataFrame(pd_tot)

        df_tot=df_tot.withColumn('STD_DAY', lit(std_day()))
        df_tot = df_tot.drop(df_tot.SI_DO_y).drop(df_tot.GU_y)
        df_tot=df_tot.withColumnRenamed('SI_DO_x','SI_DO').withColumnRenamed('GU_x','GU')
        
        # df_tot.write.format('csv').option('header','true') \
        #         .save('s3a://residencebucket/fin_num1028.csv', mode='overwrite')
        #CSV 저장-PANDAS로 변환 필요
        bucket = 'residencebucket'  
        csv_buffer = StringIO()

        pd_tot=df_tot.toPandas()
        pd_tot.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, 'fin_num_'+ std_day() +'.csv').put(Body=csv_buffer.getvalue())
        
        
        #DM 에 저장
        df_tot=spark_session().createDataFrame(pd_tot)
        save_data(DataMart, df_tot,'FINAL_NUM' )

       