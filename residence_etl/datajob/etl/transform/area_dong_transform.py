from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session
from pyspark.sql.types import *
from pyspark.sql.functions import col


class AreaDongTransformer:

    @classmethod
    def transform(cls):
        schema = StructType([
            StructField("SI_DO", StringType(), False),
            StructField("GU", StringType(), False),
            StructField("DONG", StringType(), False),
            StructField("AREA", FloatType(), False),
        ])
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("encoding", "UTF-8") \
                                 .schema(schema) \
                                 .csv("s3a://residencebucket/raw_data/AREA_DONG/AREA_DONG.csv")

        df_area_dong = cls._add_loc_idx(df)

        save_data(DataWarehouse, df_area_dong, 'AREA_DONG')


    @classmethod
    def _add_loc_idx(cls, df):
        df_loc = find_data(DataWarehouse, 'LOC')
        df_loc = df_loc.drop(df_loc.SI_DO_CODE) \
                       .drop(df_loc.SI_DO) \
                       .drop(df_loc.GU_CODE) \
                       .drop(df_loc.DONG_CODE)

        df_area_dong = df.join(df_loc, on=['GU', 'DONG'], how='left')
        df_area_dong = df_area_dong.drop(df_area_dong.SI_DO) \
                                   .drop(df_area_dong.GU) \
                                   .drop(df_area_dong.DONG)
                                   
        return df_area_dong
