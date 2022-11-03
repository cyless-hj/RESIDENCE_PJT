from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class PopuMZDataMart:

    @classmethod
    def save(cls):
        popu_mz = find_data(DataWarehouse, 'POPU_MZ')
        popu_mz = popu_mz.filter(col("STD_DAY") == std_day())
    

        save_data(DataMart, popu_mz, 'POPU_MZ')
