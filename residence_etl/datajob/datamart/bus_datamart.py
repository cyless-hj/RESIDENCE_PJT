from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import cal_std_day, std_day


class BusDataMart:

    @classmethod
    def save(cls):
        bus = find_data(DataWarehouse, 'BUS')
        bus = bus.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        bus = bus.join(loc, on='LOC_IDX')

        bus = bus.drop(bus.LOC_IDX)

        save_data(DataMart, bus, 'BUS')