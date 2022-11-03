from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class PharmacyDataMart:

    @classmethod
    def save(cls):
        pharmacy = find_data(DataWarehouse, 'PHARMACY')
        pharmacy = pharmacy.filter(col("STD_DAY") == std_day())

        pharmacy_week = find_data(DataWarehouse, 'PHARM_WEEK')
        pharmacy_week = pharmacy_week.filter(col("STD_DAY") == std_day())

        pharmacy_week = pharmacy_week.drop(pharmacy_week.PHARMACY_WEEK_IDX) \
                                     .drop(pharmacy_week.PHARMACY_NAME) \
                                     .drop(pharmacy_week.STD_DAY)

        pharmacy_hol = find_data(DataWarehouse, 'PHARM_HOL')
        pharmacy_hol = pharmacy_hol.filter(col("STD_DAY") == std_day())

        pharmacy_hol = pharmacy_hol.drop(pharmacy_hol.PHARM_HOL_IDX) \
                                   .drop(pharmacy_hol.PHARMACY_NAME) \
                                   .drop(pharmacy_hol.STD_DAY)

        pharmacy_time = pharmacy_week.join(pharmacy_hol, on='PHARMACY_CODE')
        pharmacy = pharmacy.join(pharmacy_time, on='PHARMACY_CODE')


        loc = find_data(DataWarehouse, 'LOC')
        pharmacy = pharmacy.join(loc, on='LOC_IDX')

        pharmacy = pharmacy.drop(pharmacy.LOC_IDX)

        save_data(DataMart, pharmacy, 'PHARMACY')