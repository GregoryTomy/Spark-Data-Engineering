from etl.layers.bronze.product_category import ProductCategoryBronzeETL
from etl.utils.base_table import ETLDataSet


class TestProductCategoryBronzeETL:
    """
    Test suite for the ProductCategoryBronzeETL class.
    This test suite contains unit tests for the methods of the ProductCategoryBronzeETL class, which is responsible for
    extracting and transforming data in the ETL pipeline.
    Classes:
        TestProductCategoryBronzeETL: Contains unit tests for the ProductCategoryBronzeETL class.
    Methods:
        test_extract_upstream(spark):
        test_transform_upstream(spark):
    """

    def test_extract_upstream(self, spark):
        """
        Test the extract_upstream method of the ProductCategoryBronzeETL class.
        This test initializes an instance of the ProductCategoryBronzeETL class with a Spark session,
        calls the extract_upstream method, and asserts that the first    element in the resulting
        dataset has a name attribute equal to "product_category".
        Args:
            spark (SparkSession): The Spark session to be used for the test.
        Asserts:
            The name attribute of the first element in the dataset returned by extract_upstream
            is equal to "product_category".
        """

        product_category_table = ProductCategoryBronzeETL(spark=spark)
        product_category_table_dataset = product_category_table.extract_upstream()

        assert product_category_table_dataset[0].name == "product_category"

    def test_transform_upstream(self, spark):
        """
        Test the `transform_upstream` method of the `ProductCategoryBronzeETL` class.
        This test verifies that the `transform_upstream` method correctly transforms the upstream dataset by:
        1. Adding a new column `etl_inserted` to the DataFrame.
        2. Ensuring the schema of the transformed DataFrame includes the new column.
        3. Verifying that the data in the original columns remains unchanged.
        Args:
            spark (SparkSession): The Spark session to use for creating DataFrames.
        Steps:
        1. Create a sample DataFrame with predefined data and schema.
        2. Instantiate the `ProductCategoryBronzeETL` class.
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
                "2024-01-01",
                "Updater A",
                "2024-01-01",
            ),
            (
                2,
                200,
                "2024-01-02",
                "Updater B",
                "2024-01-02",
            ),
        ]
        schema = [
            "product_id",
            "category_id",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]

        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        product_category_table = ProductCategoryBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=product_category_table.name,
            current_data=upstream_df,
            primary_keys=product_category_table.primary_keys,
            storage_path=product_category_table.storage_path,
            data_format=product_category_table.data_format,
            database=product_category_table.database,
            partition_keys=product_category_table.partition_keys,
        )

        transformed_datasets = product_category_table.transform_upstream(
            [upstream_dataset]
        )

        assert "etl_inserted" in transformed_datasets.current_data.columns

        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_datasets.current_data.columns) == expected_schema

        transformed_dataset = transformed_datasets.current_data.select(schema)
        assert transformed_dataset.collect() == upstream_df.collect()
