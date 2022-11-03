from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class SportDataMart:

    @classmethod
    def save(cls):
        sport = find_data(DataWarehouse, 'SPORT_FACILITY')
        sport = sport.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        sport = sport.join(loc, on='LOC_IDX')

        sport = sport.drop(sport.LOC_IDX)

        save_data(DataMart, sport, 'SPORT_FACILITY')