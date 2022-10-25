from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class PopuDongDataMart:

    @classmethod
    def save(cls):
        popu_dong = find_data(DataWarehouse, 'POPU_DONG')
        popu_dong = popu_dong.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        popu_dong = popu_dong.join(loc, on='LOC_IDX')

        popu_dong = popu_dong.drop(popu_dong.LOC_IDX) \
                         .drop(popu_dong.PO_DONG_ID)

        save_data(DataMart, popu_dong, 'POPU_DONG')
