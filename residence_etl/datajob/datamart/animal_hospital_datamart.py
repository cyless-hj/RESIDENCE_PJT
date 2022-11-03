from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class AnimalHospitalDataMart:

    @classmethod
    def save(cls):
        animal, loc = cls._load_data()
        animal = cls._refact_df(animal, loc)

        save_data(DataMart, animal, 'ANIMAL_HSPT')

    @classmethod
    def _refact_df(cls, df, loc):
        df = df.join(loc, on='LOC_IDX')

        df = df.drop(df.LOC_IDX)
        return df

    @classmethod
    def _load_data(cls):
        animal = find_data(DataWarehouse, 'ANIMAL_HSPT')
        animal = animal.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        return animal, loc