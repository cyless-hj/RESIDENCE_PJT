from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class VeganDataMart:

    @classmethod
    def save(cls):
        vegan = find_data(DataWarehouse, 'VEGAN')
        vegan = vegan.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        vegan = vegan.join(loc, on='LOC_IDX')

        vegan = vegan.drop(vegan.LOC_IDX)

        save_data(DataMart, vegan, 'VEGAN')
