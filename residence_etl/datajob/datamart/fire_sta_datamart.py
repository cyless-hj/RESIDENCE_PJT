from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class FireStaDataMart:

    @classmethod
    def save(cls):
        fire_sta = find_data(DataWarehouse, 'FIRE_STA')
        fire_sta = fire_sta.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        fire_sta = fire_sta.join(loc, on='LOC_IDX')

        fire_sta = fire_sta.drop(fire_sta.LOC_IDX)

        save_data(DataMart, fire_sta, 'FIRE_STA')