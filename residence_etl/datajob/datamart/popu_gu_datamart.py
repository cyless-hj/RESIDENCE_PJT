from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class PopuGuDataMart:

    @classmethod
    def save(cls):
        popu_gu = find_data(DataWarehouse, 'POPU_GU')
        popu_gu = popu_gu.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        popu_gu = popu_gu.join(loc, on='LOC_IDX')

        popu_gu = popu_gu.drop(popu_gu.LOC_IDX) \
                         .drop(popu_gu.POGU_ID) \
                         .drop(popu_gu.DONG) \
                         .drop(popu_gu.DONG_CODE)

        save_data(DataMart, popu_gu, 'POPU_GU')
