from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import spark_session


class LocTransformer:

    @classmethod
    def transform(cls):
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/LOC/LOC.csv")

        save_data(DataWarehouse, df, 'LOC')
