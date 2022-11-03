from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class CarSharingDataMart:

    @classmethod
    def save(cls):
        carshr = find_data(DataWarehouse, 'CAR_SHARING')
        carshr = carshr.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        carshr = carshr.join(loc, on='LOC_IDX')

        carshr = carshr.drop(carshr.LOC_IDX)

        save_data(DataMart, carshr, 'CAR_SHARING')