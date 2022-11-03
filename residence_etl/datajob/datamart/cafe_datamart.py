from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class CafeDataMart:

    @classmethod
    def save(cls):
        cafe = find_data(DataWarehouse, 'CAFE')
        cafe = cafe.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        cafe = cafe.join(loc, on='LOC_IDX')

        cafe = cafe.drop(cafe.LOC_IDX)

        save_data(DataMart, cafe, 'CAFE')