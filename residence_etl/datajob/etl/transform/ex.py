from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import spark_session


class CoronaPatientTrasformer:

    @classmethod
    def transform(cls):
        df = spark_session().read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("s3a://residencebucket/시군구_코드.csv")

        df.show()