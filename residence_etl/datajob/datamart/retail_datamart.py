from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class RetailDataMart:

    @classmethod
    def save(cls):
        retail, loc = cls._load_data()
        retail = cls._refact_df(retail, loc)

        save_data(DataMart, retail, 'RETAIL')

    @classmethod
    def _refact_df(cls, df, loc):
        df = df.join(loc, on='LOC_IDX')

        df = df.drop(df.LOC_IDX)
        return df

    @classmethod
    def _load_data(cls):
        retail = find_data(DataWarehouse, 'RETAIL')
        retail = retail.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        return retail, loc
