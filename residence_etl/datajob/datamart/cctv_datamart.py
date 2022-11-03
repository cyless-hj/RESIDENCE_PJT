from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class CctvDataMart:

    @classmethod
    def save(cls):
        cctv, loc = cls._load_data()
        cctv = cls._refact_df(cctv, loc)

        save_data(DataMart, cctv, 'CCTV')

    @classmethod
    def _refact_df(cls, df, loc):
        df = df.join(loc, on='LOC_IDX')

        df = df.drop(df.LOC_IDX)
        return df

    @classmethod
    def _load_data(cls):
        cctv = find_data(DataWarehouse, 'CCTV')
        cctv = cctv.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        return cctv, loc