from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class McdonaldsDataMart:

    @classmethod
    def save(cls):
        mcdonals = find_data(DataWarehouse, 'MCDONALDS')
        mcdonals = mcdonals.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        mcdonals = mcdonals.join(loc, on='LOC_IDX')

        mcdonals = mcdonals.drop(mcdonals.LOC_IDX)

        save_data(DataMart, mcdonals, 'MCDONALDS')