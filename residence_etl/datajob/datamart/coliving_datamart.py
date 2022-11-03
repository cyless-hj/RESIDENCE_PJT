from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class ColivingDataMart:

    @classmethod
    def save(cls):
        coliving, loc = cls._load_data()
        coliving = cls._refact_coliving(coliving, loc)

        save_data(DataMart, coliving, 'COLIVING')

    @classmethod
    def _refact_coliving(cls, coliving, loc):
        coliving = coliving.join(loc, on='LOC_IDX')

        coliving = coliving.drop(coliving.LOC_IDX)
        return coliving

    @classmethod
    def _load_data(cls):
        coliving = find_data(DataWarehouse, 'COLIVING')
        coliving = coliving.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        return coliving, loc
