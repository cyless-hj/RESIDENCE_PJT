from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class BikeDataMart:

    @classmethod
    def save(cls):
        bike, loc = cls._load_data()
        bike = cls._refact_df(bike, loc)

        save_data(DataMart, bike, 'BIKE')

    @classmethod
    def _refact_df(cls, df, loc):
        df = df.join(loc, on='LOC_IDX')

        df = df.drop(df.LOC_IDX)
        return df

    @classmethod
    def _load_data(cls):
        bike = find_data(DataWarehouse, 'BIKE')
        bike = bike.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        return bike, loc
