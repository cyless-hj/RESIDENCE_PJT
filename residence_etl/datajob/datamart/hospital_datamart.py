from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class HospitalDataMart:

    @classmethod
    def save(cls):
        hospital = find_data(DataWarehouse, 'HOSPITAL')
        hospital = hospital.filter(col("STD_DAY") == std_day())

        hospital_week = find_data(DataWarehouse, 'HOSPITAL_WEEK')
        hospital_week = hospital_week.filter(col("STD_DAY") == std_day())

        hospital_week = hospital_week.drop(hospital_week.HOSPITAL_WEEK_IDX) \
                                     .drop(hospital_week.HOSPITAL_NAME) \
                                     .drop(hospital_week.STD_DAY)

        hospital_hol = find_data(DataWarehouse, 'HOSPITAL_HOL')
        hospital_hol = hospital_hol.filter(col("STD_DAY") == std_day())

        hospital_hol = hospital_hol.drop(hospital_hol.HOSPITAL_HOL_IDX) \
                                   .drop(hospital_hol.HOSPITAL_NAME) \
                                   .drop(hospital_hol.STD_DAY)

        hospital_time = hospital_week.join(hospital_hol, on='HOSPITAL_CODE')
        hospital = hospital.join(hospital_time, on='HOSPITAL_CODE')


        loc = find_data(DataWarehouse, 'LOC')
        hospital = hospital.join(loc, on='LOC_IDX')

        hospital = hospital.drop(hospital.LOC_IDX)

        save_data(DataMart, hospital, 'HOSPITAL')