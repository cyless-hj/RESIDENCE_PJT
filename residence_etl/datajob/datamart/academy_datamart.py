from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class AcademyDataMart:

    @classmethod
    def save(cls):
        academy = find_data(DataWarehouse, 'ACADEMY')
        academy = academy.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        academy = academy.join(loc, on='LOC_IDX')

        academy = academy.drop(academy.LOC_IDX)

        save_data(DataMart, academy, 'ACADEMY')