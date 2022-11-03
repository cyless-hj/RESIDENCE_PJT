from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class ParkDataMart:

    @classmethod
    def save(cls):
        park, loc = cls._load_data()
        park = cls._refact_df(park, loc)

        save_data(DataMart, park, 'PARK')

    @classmethod
    def _refact_df(cls, park, loc):
        park = park.join(loc, on='LOC_IDX')

        park = park.drop(park.LOC_IDX)
        return park

    @classmethod
    def _load_data(cls):
        park = find_data(DataWarehouse, 'PARK')
        park = park.filter(col("STD_DAY") == '2022-10-27')
        loc = find_data(DataWarehouse, 'LOC')
        return park, loc