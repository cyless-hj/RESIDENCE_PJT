from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from infra.util import std_day
from pyspark.sql import Row
from pyspark.sql.functions import col, monotonically_increasing_id
import pandas as pd
from pyspark.sql.types import *


class NoiseVibrationTransformer:

    @classmethod
    def transform(cls):
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/NOISE_VIBRATION/NOISE_VIBRATION.csv")

        df_nv = cls.add_loc_idx(df)

        df_nv = cls._add_cate_day(df_nv)
        df_nv = df_nv.drop(df_nv.GU)

        save_data(DataWarehouse, df_nv, 'NOISE_VIBRATION')

    @classmethod
    def _add_cate_day(cls, df):
        gu_list = df.select('GU').rdd.flatMap(lambda x: x).collect()
        cate_day = Row(CATE_CODE='E111', STD_DAY=std_day())
        rows = []
        for g in range(len(gu_list)):
            rows.append(cate_day)
        cate_day_df = spark_session().createDataFrame(rows)

        pd_day_df = cate_day_df.toPandas()
        pd_popu_gu = df.toPandas()
        pd_popu_gu = pd_popu_gu.astype({'LOC_IDX':'int'})
        df = pd.concat([pd_day_df, pd_popu_gu], axis=1)

        df = spark_session().createDataFrame(df)
        return df

    @classmethod
    def add_loc_idx(cls, df):
        df_loc = find_data(DataWarehouse, 'LOC')
        df_loc = df_loc.drop(df_loc.SI_DO_CODE) \
                       .drop(df_loc.SI_DO) \
                       .drop(df_loc.DONG_CODE) \
                       .drop(df_loc.DONG)
        select_min_df = df_loc.groupby(df_loc.GU).agg({'LOC_IDX' :'min'})
        select_min_df = select_min_df.withColumnRenamed('min(LOC_IDX)', 'LOC_IDX')
        df = df.join(select_min_df, on='GU', how='left')
        return df