from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class DepartmentStoreDataMart:

    @classmethod
    def save(cls):
        department_store, loc = cls._load_data()
        department_store = cls._refact_df(department_store, loc)

        save_data(DataMart, department_store, 'DEPARTMENT_STORE')

    @classmethod
    def _refact_df(cls, df, loc):
        df = df.join(loc, on='LOC_IDX')

        df = df.drop(df.LOC_IDX)
        return df

    @classmethod
    def _load_data(cls):
        department_store = find_data(DataWarehouse, 'DEPARTMENT_STORE')
        department_store = department_store.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        return department_store, loc
