from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from infra.util import std_day
from pyspark.sql import Row
from pyspark.sql.functions import col, monotonically_increasing_id
import pandas as pd
from pyspark.sql.types import *

class PopuGuTransformer:

    @classmethod
    def transform(cls):
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/POPU_GU/POPU_GU.csv")

        df_popu_gu = cls.add_loc_idx(df)

        df_popu_gu = cls._add_std_day(df_popu_gu)

        save_data(DataWarehouse, df_popu_gu, 'POPU_GU')

    @classmethod
    def _add_std_day(cls, df_popu_gu):
        gu_list = df_popu_gu.select('GU').rdd.flatMap(lambda x: x).collect()
        day = Row(STD_DAY=std_day())
        rows = []
        for g in range(len(gu_list)):
            rows.append(day)
        day_df = spark_session().createDataFrame(rows)

        pd_day_df = day_df.toPandas()
        pd_popu_gu = df_popu_gu.toPandas()
        pd_popu_gu = pd_popu_gu.astype({'LOC_IDX':'int'})
        df_popu_gu = pd.concat([pd_day_df, pd_popu_gu], axis=1)

        df_popu_gu = spark_session().createDataFrame(df_popu_gu)
        df_popu_gu = df_popu_gu.drop(df_popu_gu.GU)
        return df_popu_gu

    @classmethod
    def add_loc_idx(cls, df):
        df_loc = find_data(DataWarehouse, 'LOC')
        df_loc = df_loc.drop(df_loc.SI_DO_CODE) \
                       .drop(df_loc.SI_DO) \
                       .drop(df_loc.DONG_CODE) \
                       .drop(df_loc.DONG)
        select_min_df = df_loc.groupby(df_loc.GU).agg({'LOC_IDX' :'min'})
        select_min_df = select_min_df.withColumnRenamed('min(LOC_IDX)', 'LOC_IDX')
        df_popu_gu = df.join(select_min_df, on='GU', how='left').drop(df.SI_DO)
        return df_popu_gu