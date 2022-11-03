from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class KidsCafeDataMart:

    @classmethod
    def save(cls):
        kidsc = find_data(DataWarehouse, 'KIDS_CAFE')
        kidsc = kidsc.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        kidsc = kidsc.join(loc, on='LOC_IDX')

        kidsc = kidsc.drop(kidsc.LOC_IDX)

        save_data(DataMart, kidsc, 'KIDS_CAFE')