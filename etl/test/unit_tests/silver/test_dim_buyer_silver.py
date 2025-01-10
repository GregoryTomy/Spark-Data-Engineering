from pyspark.sql import SparkSession
from etl.layers.silver.dim_buyer_silver import DimBuyerSiverETL
from etl.utils.base_table import ETLDataSet


class TestDimBuyerSilverETL:
    """
    Test the `transform_upstream` method of the `DimBuyerSiverETL` class.
    This test verifies that the `transform_upstream` method correctly transforms
    the input datasets (`appuser` and `buyer`) and produces the expected output
    dataset with the correct schema and data.

    Args:
        spark (SparkSession): The Spark session to use for creating DataFrames.

    Asserts:
        - The columns of the transformed dataset match the expected columns.
        - The data in the transformed dataset matches the expected data.
    """

    def test_transform_upstream(self, spark: SparkSession):
        """
        Test the `transform_upstream` method of the `DimBuyerSiverETL` class.
        This test verifies that the `transform_upstream` method correctly transforms
        the input datasets (`appuser` and `buyer`) and produces the expected output
        dataset with the correct schema and data.
        Args:
            spark (SparkSession): The Spark session to use for creating DataFrames.
        Steps:
        1. Define the schemas for the `appuser` and `buyer` datasets.
        2. Create sample data for the `appuser` and `buyer` datasets.
        3. Create DataFrames from the sample data using the defined schemas.
        4. Create `ETLDataSet` instances for the `appuser` and `buyer` datasets.
        5. Instantiate the `DimBuyerSiverETL` class with the appropriate parameters.
        6. Call the `transform_upstream` method with the `appuser` and `buyer` datasets.
        7. Verify that the transformed dataset has the expected columns.
        8. Verify that the transformed dataset contains the expected data.
        Asserts:
            - The columns of the transformed dataset match the expected columns.
            - The data in the transformed dataset matches the expected data.
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

        buyer_schema = [
            "buyer_id",
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
        buyer_sample_data = [
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

        buyer_df = spark.createDataFrame(buyer_sample_data, schema=buyer_schema)

        appuser_dataset = ETLDataSet(
            "appuser",
            appuser_df,
            ["user_id"],
            "",
            "delta",
            "rainforest",
            [],
        )

        buyer_dataset = ETLDataSet(
            "buyer",
            buyer_df,
            ["user_id"],
            "",
            "delta",
            "rainforest",
            [],
        )

        dim_buyer_etl = DimBuyerSiverETL(
            spark,
            run_upstream=False,
            write_data=False,
        )

        transformed_dataset = dim_buyer_etl.transform_upstream(
            [appuser_dataset, buyer_dataset]
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
                "buyer_id",
                "first_time_purchased_timestamp",
                "buyer_created_ts",
                "buyer_last_updated_by",
                "buyer_last_updated_ts",
                "buyer_etl_inserted",
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
