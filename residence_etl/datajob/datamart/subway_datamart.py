from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class SubwayDataMart:

    @classmethod
    def save(cls):
        subway = find_data(DataWarehouse, 'SUBWAY')
        subway = subway.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        subway = subway.join(loc, on='LOC_IDX')

        subway = subway.drop(subway.LOC_IDX)

        save_data(DataMart, subway, 'SUBWAY')