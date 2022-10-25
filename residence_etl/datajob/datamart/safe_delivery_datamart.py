from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class SafeDeliveryDataMart:

    @classmethod
    def save(cls):
        safed, loc = cls._load_data()
        safed = cls._refact_df(safed, loc)

        save_data(DataMart, safed, 'SAFE_DELIVERY')

    @classmethod
    def _refact_df(cls, df, loc):
        df = df.filter(col("STD_DAY") == std_day())
        
        df = df.join(loc, on='LOC_IDX')

        df = df.drop(df.LOC_IDX)
        return df

    @classmethod
    def _load_data(cls):
        safed = find_data(DataWarehouse, 'SAFE_DELIVERY')
        safed = safed.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        return safed, loc
