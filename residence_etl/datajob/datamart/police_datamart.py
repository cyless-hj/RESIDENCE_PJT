from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class PoliceDataMart:

    @classmethod
    def save(cls):
        police, loc = cls._load_data()
        police = cls._refact_df(police, loc)

        save_data(DataMart, police, 'POLICE')

    @classmethod
    def _refact_df(cls, df, loc):
        df = df.join(loc, on='LOC_IDX')

        df = df.drop(df.LOC_IDX)
        return df

    @classmethod
    def _load_data(cls):
        police = find_data(DataWarehouse, 'POLICE')
        police = police.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        return police, loc
