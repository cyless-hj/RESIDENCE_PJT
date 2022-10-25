from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class HighSchoolDataMart:

    @classmethod
    def save(cls):
        high, loc = cls._load_data()
        high = cls._refact_df(high, loc)

        save_data(DataMart, high, 'HIGH_SCHOOL')

    @classmethod
    def _refact_df(cls, df, loc):
        df = df.filter(col("STD_DAY") == std_day())
        
        df = df.join(loc, on='LOC_IDX')

        df = df.drop(df.LOC_IDX)
        return df

    @classmethod
    def _load_data(cls):
        high = find_data(DataWarehouse, 'ELEMENTARY')
        high = high.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        return high, loc
