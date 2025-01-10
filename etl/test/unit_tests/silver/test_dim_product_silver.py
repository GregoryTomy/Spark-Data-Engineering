import logging

from pyspark.sql import SparkSession, Row
from etl.layers.silver.dim_product_silver import DimProductSiverETL
from etl.utils.base_table import ETLDataSet

logging.basicConfig(level=logging.DEBUG)


class TestDimProductSilverETL:
    """
    Test the `transform_upstream` method of the `DimProductSiverETL` class.
    This test verifies that the `transform_upstream` method correctly transforms
    the input datasets (`product`, `brand`, `manufacturer`) and produces the expected output
    dataset with the correct schema and data.

    Args:
        spark (SparkSession): The Spark session to use for creating DataFrames.

    Asserts:
        - The columns of the transformed dataset match the expected columns.
        - The data in the transformed dataset matches the expected data.
    """

    def test_transform_upstream(self, spark: SparkSession):
        product_schema = [
            "product_id",
            "name",
            "description",
            "price",
            "brand_id",
            "manufacturer_id",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
            "etl_inserted",
        ]

        brand_schema = [
            "brand_id",
            "name",
            "country",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
            "etl_inserted",
        ]

        manufacturer_schema = [
            "manufacturer_id",
            "name",
            "type",
            "created_ts",
            "last_updated_by",
            "last_updated_ts",
            "etl_inserted",
        ]

        product_sample_data = [
            (
                1,
                "Product A",
                "Description A",
                100.0,
                1,
                1,
                "2024-01-01",
                "Updater A",
                "2024-01-01",
                "2024-01-01",
            )
        ]

        brand_sample_data = [
            (
                1,
                "Brand A",
                "Country X",
                "2024-01-01",
                "Alice",
                "2024-01-01",
                "2024-01-01",
            )
        ]

        manufacturer_sample_data = [
            (
                1,
                "Manufacturer A",
                "Type 1",
                "2024-01-01",
                "Updater 1",
                "2024-01-01",
                "2024-01-01",
            )
        ]

        product_df = spark.createDataFrame(product_sample_data, schema=product_schema)
        brand_df = spark.createDataFrame(brand_sample_data, schema=brand_schema)
        manufacturer_df = spark.createDataFrame(
            manufacturer_sample_data, schema=manufacturer_schema
        )

        product_dataset = ETLDataSet(
            "product",
            product_df,
            ["product_id"],
            "",
            "delta",
            "rainforest",
            [],
        )

        brand_dataset = ETLDataSet(
            "brand",
            brand_df,
            ["brand_id"],
            "",
            "delta",
            "rainforest",
            [],
        )

        manufacturer_dataset = ETLDataSet(
            "manufacturer",
            manufacturer_df,
            ["manufacturer_id"],
            "",
            "delta",
            "rainforest",
            [],
        )

        dim_product_etl = DimProductSiverETL(
            spark,
            run_upstream=False,
            write_data=False,
        )

        transformed_dataset = dim_product_etl.transform_upstream(
            [product_dataset, brand_dataset, manufacturer_dataset]
        )

        expected_columns = set(
            [
                "product_id",
                "product_name",
                "description",
                "price",
                "brand_id",
                "manufacturer_id",
                "product_created_ts",
                "product_last_updated_by",
                "product_last_updated_ts",
                "product_etl_inserted",
                "brand_name",
                "country",
                "brand_created_ts",
                "brand_last_updated_by",
                "brand_last_updated_ts",
                "brand_etl_inserted",
                "manufacturer_name",
                "type",
                "manufacturer_created_ts",
                "manufacturer_last_updated_by",
                "manufacturer_last_updated_ts",
                "manufacturer_etl_inserted",
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
                "Product A",
                "Description A",  #
                100.0,
                1,
                1,
                "2024-01-01",
                "Updater A",
                "2024-01-01",
                "2024-01-01",
                "Brand A",
                "Country X",
                "2024-01-01",
                "Alice",
                "2024-01-01",
                "2024-01-01",
                "Manufacturer A",
                "Type 1",
                "2024-01-01",
                "Updater 1",
                "2024-01-01",
                "2024-01-01",
                transformed_dataset.current_data.select("etl_inserted").collect()[0][0],
            )
        ]

        assert (
            transformed_dataset.current_data.collect() == expected_data
        ), "Tranformed data does not match expected data"
