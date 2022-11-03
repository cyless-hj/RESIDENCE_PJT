from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class LeisureDataMart:

    @classmethod
    def save(cls):
        leisure = find_data(DataWarehouse, 'LEISURE')
        leisure = leisure.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        leisure = leisure.join(loc, on='LOC_IDX')

        leisure = leisure.drop(leisure.LOC_IDX)

        save_data(DataMart, leisure, 'LEISURE')