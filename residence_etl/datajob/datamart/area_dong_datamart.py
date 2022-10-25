from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class AreaDongDataMart:

    @classmethod
    def save(cls):
        area_dong = find_data(DataWarehouse, 'AREA_DONG')
        loc = find_data(DataWarehouse, 'LOC')
        area_dong = area_dong.join(loc, on='LOC_IDX')

        area_dong = area_dong.drop(area_dong.LOC_IDX) \
                         .drop(area_dong.AREA_DONG_ID) \
                         .drop(area_dong.DONG)

        save_data(DataMart, area_dong, 'AREA_DONG')
