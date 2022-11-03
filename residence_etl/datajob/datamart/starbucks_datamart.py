from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class StarbucksDataMart:

    @classmethod
    def save(cls):
        starbucks = find_data(DataWarehouse, 'STARBUCKS')
        starbucks = starbucks.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        starbucks = starbucks.join(loc, on='LOC_IDX')

        starbucks = starbucks.drop(starbucks.LOC_IDX)

        save_data(DataMart, starbucks, 'STARBUCKS')