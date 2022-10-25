from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class AreaGuDataMart:

    @classmethod
    def save(cls):
        area_gu = find_data(DataWarehouse, 'AREA_GU')
        loc = find_data(DataWarehouse, 'LOC')
        area_gu = area_gu.join(loc, on='LOC_IDX')

        area_gu = area_gu.drop(area_gu.LOC_IDX) \
                         .drop(area_gu.AREA_GU_ID) \
                         .drop(area_gu.DONG) \
                         .drop(area_gu.DONG_CODE)

        save_data(DataMart, area_gu, 'AREA_GU')
