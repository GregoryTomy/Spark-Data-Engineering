from etl.layers.bronze.order_item import OrderItemBronzeETL
from etl.utils.base_table import ETLDataSet


class TestOrderItemBronzeETL:
    """
    Test suite for the OrderItemBronzeETL class.
    This test suite contains unit tests for the methods of the OrderItemBronzeETL class, which is responsible for
    extracting and transforming data in the ETL pipeline.
    Classes:
        TestOrderItemBronzeETL: Contains unit tests for the OrderItemBronzeETL class.
    Methods:
        test_extract_upstream(spark):
        test_transform_upstream(spark):
    """

    def test_extract_upstream(self, spark):
        """
        Test the extract_upstream method of the OrderItemBronzeETL class.
        This test initializes an instance of the OrderItemBronzeETL class with a Spark session,
        calls the extract_upstream method, and asserts that the first    element in the resulting
        dataset has a name attribute equal to "order_item".
        Args:
            spark (SparkSession): The Spark session to be used for the test.
        Asserts:
            The name attribute of the first element in the dataset returned by extract_upstream
            is equal to "order_item".
        """

        order_item_table = OrderItemBronzeETL(spark=spark)
        order_item_table_dataset = order_item_table.extract_upstream()

        assert order_item_table_dataset[0].name == "order_item"

    def test_transform_upstream(self, spark):
        """
        Test the `transform_upstream` method of the `OrderItemBronzeETL` class.
        This test verifies that the `transform_upstream` method correctly transforms the upstream dataset by:
        1. Adding a new column `etl_inserted` to the DataFrame.
        2. Ensuring the schema of the transformed DataFrame includes the new column.
        3. Verifying that the data in the original columns remains unchanged.
        Args:
            spark (SparkSession): The Spark session to use for creating DataFrames.
        Steps:
        1. Create a sample DataFrame with predefined data and schema.
        2. Instantiate the `OrderItemBronzeETL` class.
        3. Create an `ETLDataSet` object with the sample DataFrame.
        4. Call the `transform_upstream` method with the `ETLDataSet` object.
        5. Assert that the new column `etl_inserted` is added to the DataFrame.
        6. Assert that the schema of the transformed DataFrame includes the new column.
        7. Assert that the data in the original columns remains unchanged.
        """

        sample_data = [
            (
                1,
                100,
                500,
                10,
                2,
                100.0,
                10.0,
                "2024-01-01",
            ),
            (
                2,
                101,
                501,
                11,
                1,
                150.0,
                15.0,
                "2024-01-02",
            ),
        ]
        schema = [
            "order_item_id",
            "order_id",
            "product_id",
            "seller_id",
            "quantity",
            "base_price",
            "tax",
            "created_ts",
        ]

        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        order_item_table = OrderItemBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=order_item_table.name,
            current_data=upstream_df,
            primary_keys=order_item_table.primary_keys,
            storage_path=order_item_table.storage_path,
            data_format=order_item_table.data_format,
            database=order_item_table.database,
            partition_keys=order_item_table.partition_keys,
        )

        transformed_datasets = order_item_table.transform_upstream([upstream_dataset])

        assert "etl_inserted" in transformed_datasets.current_data.columns

        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_datasets.current_data.columns) == expected_schema

        transformed_dataset = transformed_datasets.current_data.select(schema)
        assert transformed_dataset.collect() == upstream_df.collect()
