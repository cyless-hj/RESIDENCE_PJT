from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class GymDataMart:

    @classmethod
    def save(cls):
        gym = find_data(DataWarehouse, 'GYM')
        gym = gym.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        gym = gym.join(loc, on='LOC_IDX')

        gym = gym.drop(gym.LOC_IDX)

        save_data(DataMart, gym, 'GYM')