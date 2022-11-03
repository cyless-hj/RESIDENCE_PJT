from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class GolfDataMart:

    @classmethod
    def save(cls):
        golf = find_data(DataWarehouse, 'GOLF')
        golf = golf.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        golf = golf.join(loc, on='LOC_IDX')

        golf = golf.drop(golf.LOC_IDX)

        save_data(DataMart, golf, 'GOLF')