from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class ConStoreDataMart:

    @classmethod
    def save(cls):
        con_store = find_data(DataWarehouse, 'CON_STORE')
        con_store = con_store.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        con_store = con_store.join(loc, on='LOC_IDX')

        con_store = con_store.drop(con_store.LOC_IDX)

        save_data(DataMart, con_store, 'CON_STORE')