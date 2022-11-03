from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil

from infra.util import std_day


class NoiseVibrationDataMart:

    @classmethod
    def save(cls):
        nv = find_data(DataWarehouse, 'NOISE_VIBRATION')
        nv = nv.filter(col("STD_DAY") == std_day())
        loc = find_data(DataWarehouse, 'LOC')
        nv = nv.join(loc, on='LOC_IDX')

        nv = nv.drop(nv.LOC_IDX) \
               .drop(nv.DONG_CODE) \
               .drop(nv.DONG)

        nv = loc.join(nv, on=['SI_DO_CODE', 'SI_DO', 'GU_CODE', 'GU'])
        nv = nv.drop(nv.LOC_IDX)

        save_data(DataMart, nv, 'NOISE_VIBRATION')