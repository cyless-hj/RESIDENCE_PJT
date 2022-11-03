from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class MiddleSchoolDataMart:

    @classmethod
    def save(cls):
        middle, loc = cls._load_data()
        middle = cls._refact_df(middle, loc)

        save_data(DataMart, middle, 'MIDDLE')

    @classmethod
    def _refact_df(cls, df, loc):
        df = df.join(loc, on='LOC_IDX')

        df = df.drop(df.LOC_IDX)
        return df

    @classmethod
    def _load_data(cls):
        middle = find_data(DataWarehouse, 'MIDDLE')
        middle = middle.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        return middle, loc
