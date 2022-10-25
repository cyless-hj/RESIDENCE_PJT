from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class NoiseVibrationDataMart:

    @classmethod
    def save(cls):
        nv, loc = cls._load_data()
        nv = cls._refact_df(nv, loc)
        nv.show(50)
        #save_data(DataMart, nv, 'NOISE_VIBRATION')

    @classmethod
    def _refact_df(cls, df, loc):
        df = df.filter(col("STD_DAY") == std_day())
        
        df = df.join(loc, on='LOC_IDX')

        df = df.drop(df.LOC_IDX)
        return df

    @classmethod
    def _load_data(cls):
        nv = find_data(DataWarehouse, 'NOISE_VIBRATION')
        loc = find_data(DataWarehouse, 'LOC')
        return nv, loc
