from etl.layers.bronze.ratings import RatingsBronzeETL
from etl.utils.base_table import ETLDataSet


class TestRatingsBronzeETL:
    """
    Test suite for the RatingsBronzeETL class.
    This test suite contains unit tests for the methods of the RatingsBronzeETL class, which is responsible for
    extracting and transforming data in the ETL pipeline.
    Classes:
        TestRatingsBronzeETL: Contains unit tests for the RatingsBronzeETL class.
    Methods:
        test_extract_upstream(spark):
        test_transform_upstream(spark):
    """

    def test_extract_upstream(self, spark):
        """
        Test the extract_upstream method of the RatingsBronzeETL class.
        This test initializes an instance of the RatingsBronzeETL class with a Spark session,
        calls the extract_upstream method, and asserts that the first    element in the resulting
        dataset has a name attribute equal to "ratings".
        Args:
            spark (SparkSession): The Spark session to be used for the test.
        Asserts:
            The name attribute of the first element in the dataset returned by extract_upstream
            is equal to "ratings".
        """

        ratings_table = RatingsBronzeETL(spark=spark)
        ratings_table_dataset = ratings_table.extract_upstream()

        assert ratings_table_dataset[0].name == "ratings"

    def test_transform_upstream(self, spark):
        """
        Test the `transform_upstream` method of the `RatingsBronzeETL` class.
        This test verifies that the `transform_upstream` method correctly transforms the upstream dataset by:
        1. Adding a new column `etl_inserted` to the DataFrame.
        2. Ensuring the schema of the transformed DataFrame includes the new column.
        3. Verifying that the data in the original columns remains unchanged.
        Args:
            spark (SparkSession): The Spark session to use for creating DataFrames.
        Steps:
        1. Create a sample DataFrame with predefined data and schema.
        2. Instantiate the `RatingsBronzeETL` class.
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
                5,
                "2024-01-01",
                "Updater A",
                "2024-01-01",
            ),
            (
                2,
                200,
                4,
                "2024-01-02",
                "Updater B",
                "2024-01-02",
            ),
        ]
        schema = [
            "ratings_id",
            "product_id",
            "rating",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
        ]

        upstream_df = spark.createDataFrame(sample_data, schema=schema)

        ratings_table = RatingsBronzeETL(spark=spark)

        upstream_dataset = ETLDataSet(
            name=ratings_table.name,
            current_data=upstream_df,
            primary_keys=ratings_table.primary_keys,
            storage_path=ratings_table.storage_path,
            data_format=ratings_table.data_format,
            database=ratings_table.database,
            partition_keys=ratings_table.partition_keys,
        )

        transformed_datasets = ratings_table.transform_upstream([upstream_dataset])

        assert "etl_inserted" in transformed_datasets.current_data.columns

        expected_schema = set(schema + ["etl_inserted"])
        assert set(transformed_datasets.current_data.columns) == expected_schema

        transformed_dataset = transformed_datasets.current_data.select(schema)
        assert transformed_dataset.collect() == upstream_df.collect()
