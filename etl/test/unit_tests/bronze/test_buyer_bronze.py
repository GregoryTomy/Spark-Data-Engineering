from etl.layers.bronze.buyer import BuyerBronzeETL
from etl.utils.base_table import ETLDataSet


class TestBuyerBronzeETL:
    """
    Test suite for the BuyerBronzeETL class.
    This test suite contains unit tests for the methods of the BuyerBronzeETL class, which is responsible for
    extracting and transforming data in the ETL pipeline.
    Classes:
        TestBuyerBronzeETL: Contains unit tests for the BuyerBronzeETL class.
    Methods:
        test_extract_upstream(spark):
        test_transform_upstream(spark):
    """

    def test_extract_upstream(self, spark):
        """
        Test the extract_upstream method of the BuyerBronzeETL class.
        This test initializes an instance of the BuyerBronzeETL class with a Spark session,
        calls the extract_upstream method, and asserts that the first    element in the resulting
        dataset has a name attribute equal to "buyer".
        Args:
            spark (SparkSession): The Spark session to be used for the test.
        Asserts:
            The name attribute of the first element in the dataset returned by extract_upstream
            is equal to "buyer".
        """

        buyer_table = BuyerBronzeETL(spark=spark)
        buyer_table_dataset = buyer_table.extract_upstream()

        assert buyer_table_dataset[0].name == "buyer"

    def test_transform_upstream(self, spark):
        """
        Test the `transform_upstream` method of the `BuyerBronzeETL` class.
        This test verifies that the `transform_upstream` method correctly transforms the upstream dataset by:
        1. Adding a new column `etl_inserted` to the DataFrame.
        2. Ensuring the schema of the transformed DataFrame includes the new column.
        3. Verifying that the data in the original columns remains unchanged.
        Args:
            spark (SparkSession): The Spark session to use for creating DataFrames.
        Steps:
        1. Create a sample DataFrame with predefined data and schema.
        2. Instantiate the `BuyerBronzeETL` class.
        3. Create an `ETLDataSet` object with the sample DataFrame.
        4. Call the `transform_upstream` method with the `ETLDataSet` object.
        5. Assert that the new column `etl_inserted` is added to the DataFrame.
        6. Assert that the schema of the transformed DataFrame includes the new column.
        7. Assert that the data in the original columns remains unchanged.
        """

        sample_data = [
            (
                1,
                "user_1",
                "2024-01-01",
                "2024-01-01",
                "John",
                "2024-01-01",
            ),
            (
                2,
                "user_2",
                "2024-01-02",
                "2024-01-02",
                "Mack",
                "2022-01-02",
            ),
        ]
        schema = [
            "buyer_id",
            "user_id",
            "first_time_purchased_timestamp",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]

        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        buyer_table = BuyerBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=buyer_table.name,
            current_data=upstream_df,
            primary_keys=buyer_table.primary_keys,
            storage_path=buyer_table.storage_path,
            data_format=buyer_table.data_format,
            database=buyer_table.database,
            partition_keys=buyer_table.partition_keys,
        )

        transformed_datasets = buyer_table.transform_upstream([upstream_dataset])

        assert "etl_inserted" in transformed_datasets.current_data.columns

        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_datasets.current_data.columns) == expected_schema

        transformed_dataset = transformed_datasets.current_data.select(schema)
        assert transformed_dataset.collect() == upstream_df.collect()
