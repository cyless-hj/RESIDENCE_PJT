from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class ChildMedDataMart:

    @classmethod
    def save(cls):
        child_med = find_data(DataWarehouse, 'CHILD_MED')
        child_med = child_med.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        child_med = child_med.join(loc, on='LOC_IDX')

        child_med = child_med.drop(child_med.LOC_IDX)

        save_data(DataMart, child_med, 'CHILD_MED')