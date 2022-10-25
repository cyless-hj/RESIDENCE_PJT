from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import spark_session


class CategoryTransformer:

    @classmethod
    def transform(cls):
        df = spark_session().read.format("csv") \
                                 .option("header", "true") \
                                 .option("inferSchema", "true") \
                                 .csv("s3a://residencebucket/raw_data/CATEGORY/CATEGORY.csv")

        save_data(DataWarehouse, df, 'CATEGORY')
