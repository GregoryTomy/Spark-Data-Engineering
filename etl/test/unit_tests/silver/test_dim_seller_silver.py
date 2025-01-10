from pyspark.sql import SparkSession
from etl.layers.silver.dim_seller_silver import DimSellerSiverETL
from etl.utils.base_table import ETLDataSet


class TestDimSellerSilverETL:
    """
    Test the `transform_upstream` method of the `DimSellerSiverETL` class.

    This test verifies that the `transform_upstream` method correctly transforms the input datasets
    and produces the expected output dataset with the correct schema and data.

    Args:
        spark (SparkSession): The Spark session to use for creating DataFrames.

    Asserts:
        - The columns of the transformed dataset match the expected columns.
        - The data of the transformed dataset matches the expected data.
    """

    def test_transform_upstream(self, spark: SparkSession):
        """
        Test the `transform_upstream` method of the `DimSellerSiverETL` class.

        This test verifies that the `transform_upstream` method correctly transforms the input datasets
        and produces the expected output dataset with the correct schema and data.

        Args:
            spark (SparkSession): The Spark session to use for creating DataFrames.

        Steps:
        1. Define the schema and sample data for the `appuser` and `seller` datasets.
        2. Create DataFrames for the `appuser` and `seller` datasets using the sample data and schema.
        3. Create `ETLDataSet` instances for the `appuser` and `seller` datasets.
        4. Instantiate the `DimSellerSiverETL` class with the `run_upstream` and `write_data` flags set to `False`.
        5. Call the `transform_upstream` method with the `appuser` and `seller` datasets.
        6. Verify that the transformed dataset has the expected columns.
        7. Verify that the transformed dataset has the expected data.

        Asserts:
            - The columns of the transformed dataset match the expected columns.
            - The data of the transformed dataset matches the expected data.
        """

        appuser_schema = [
            "user_id",
            "username",
            "email",
            "is_active",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
            "etl_inserted",
        ]

        seller_schema = [
            "seller_id",
            "user_id",
            "first_time_purchased_timestamp",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
            "etl_inserted",
        ]

        appuser_sample_data = [
            (
                1,
                "user_1",
                "email_1@example.com",
                True,
                "2025-01-01",
                "Updater A",
                "2025-01-01",
                "2025-01-01",
            )
        ]
        seller_sample_data = [
            (
                100,
                1,
                "2025-01-01",
                "2025-01-01",
                "Updater A",
                "2025-01-01",
                "2025-01-01",
            )
        ]

        appuser_df = spark.createDataFrame(appuser_sample_data, schema=appuser_schema)

        seller_df = spark.createDataFrame(seller_sample_data, schema=seller_schema)

        appuser_dataset = ETLDataSet(
            "appuser",
            appuser_df,
            ["user_id"],
            "",
            "delta",
            "rainforest",
            [],
        )

        seller_dataset = ETLDataSet(
            "seller",
            seller_df,
            ["user_id"],
            "",
            "delta",
            "rainforest",
            [],
        )

        dim_seller_etl = DimSellerSiverETL(
            spark,
            run_upstream=False,
            write_data=False,
        )

        transformed_dataset = dim_seller_etl.transform_upstream(
            [appuser_dataset, seller_dataset]
        )

        expected_columns = set(
            [
                "user_id",
                "username",
                "email",
                "is_active",
                "appuser_created_ts",
                "appuser_last_updated_by",
                "appuser_last_updated_ts",
                "appuser_etl_inserted",
                "seller_id",
                "first_time_purchased_timestamp",
                "seller_created_ts",
                "seller_last_updated_by",
                "seller_last_updated_ts",
                "seller_etl_inserted",
                "etl_inserted",
            ]
        )

        actual_columns = set(transformed_dataset.current_data.columns)

        assert (
            actual_columns == expected_columns
        ), f"Expected columns {expected_columns}, but got {actual_columns}"

        expected_data = [
            (
                1,
                "user_1",
                "email_1@example.com",
                True,
                "2025-01-01",
                "Updater A",
                "2025-01-01",
                "2025-01-01",
                100,
                "2025-01-01",
                "2025-01-01",
                "Updater A",
                "2025-01-01",
                "2025-01-01",
                transformed_dataset.current_data.select("etl_inserted").collect()[0][0],
            )
        ]

        assert (
            transformed_dataset.current_data.collect() == expected_data
        ), "Tranformed data does not match expected data"
