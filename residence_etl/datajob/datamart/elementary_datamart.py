from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class ElementaryDataMart:

    @classmethod
    def save(cls):
        elementary = find_data(DataWarehouse, 'ELEMENTARY')
        elementary = elementary.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        elementary = elementary.join(loc, on='LOC_IDX')

        elementary = elementary.drop(elementary.LOC_IDX)

        save_data(DataMart, elementary, 'ELEMENTARY')