from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import spark_session


class AreaGuTransformer:

    @classmethod
    def transform(cls):
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/AREA_GU/AREA_GU.csv")

        df_area_gu = cls.add_loc_idx(df)
        save_data(DataWarehouse, df_area_gu, 'AREA_GU')

    @classmethod
    def add_loc_idx(cls, df):
        df_loc = find_data(DataWarehouse, 'LOC')
        df_loc = df_loc.drop(df_loc.SI_DO_CODE) \
                       .drop(df_loc.SI_DO) \
                       .drop(df_loc.DONG_CODE) \
                       .drop(df_loc.DONG)
        select_min_df = df_loc.groupby(df_loc.GU).agg({'LOC_IDX' :'min'})
        select_min_df = select_min_df.withColumnRenamed('min(LOC_IDX)', 'LOC_IDX')
        df_area_gu = df.join(select_min_df, on='GU', how='left')
        df_area_gu = df_area_gu.drop(df_area_gu.GU)
        return df_area_gu