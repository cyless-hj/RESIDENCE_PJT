from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class KindergartenDataMart:

    @classmethod
    def save(cls):
        kindergarten = find_data(DataWarehouse, 'KINDERGARTEN')
        kindergarten = kindergarten.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        kindergarten = kindergarten.join(loc, on='LOC_IDX')

        kindergarten = kindergarten.drop(kindergarten.LOC_IDX)

        save_data(DataMart, kindergarten, 'KINDERGARTEN')