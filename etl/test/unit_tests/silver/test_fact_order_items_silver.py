from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from etl.layers.silver.fact_order_items_silver import FactOrderItemsSiverETL
from etl.utils.base_table import ETLDataSet


class TestFactOrderItemsSilverETL:
    def test_transform_upstream(self, spark: SparkSession):
        schema = StructType(
            [
                StructField("order_item_id", IntegerType(), True),
                StructField("order_id", IntegerType(), True),
                StructField("product_id", IntegerType(), True),
                StructField("seller_id", IntegerType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("base_price", DoubleType(), True),
                StructField("tax", DoubleType(), True),
                StructField("created_ts", StringType(), True),
                StructField("etl_inserted", TimestampType(), True),
            ]
        )

        sample_data = [(1, 100, 500, 10, 2, 100.0, 10.0, "2025-01-12", datetime.now())]

        order_items_df = spark.createDataFrame(
            spark.sparkContext.parallelize(sample_data),
            schema,
        )

        order_items_dataset = ETLDataSet(
            "order_items",
            order_items_df,
            ["order_item_id"],
            "",
            "delta",
            "rainforest",
            [],
        )

        fact_order_items_etl = FactOrderItemsSiverETL(spark)
        transformed_dataset = fact_order_items_etl.transform_upstream(
            [order_items_dataset]
        )

        expected_columns = [
            "order_item_id",
            "order_id",
            "product_id",
            "seller_id",
            "quantity",
            "base_price",
            "tax",
            "actual_price",
            "created_ts",
            "etl_inserted",
        ]

        actual_columns = transformed_dataset.current_data.columns
        assert set(actual_columns) == set(expected_columns), (
            "Columnds do not match expected"
        )

        expected_data = [
            (
                1,
                100,
                500,
                10,
                2,
                100.0,
                10.0,
                90.0,
                "2025-01-12",
                transformed_dataset.current_data.select("etl_inserted").collect()[0][
                    "etl_inserted"
                ],
            )
        ]

        actual_data = [
            (
                row["order_item_id"],
                row["order_id"],
                row["product_id"],
                row["seller_id"],
                row["quantity"],
                row["base_price"],
                row["tax"],
                row["actual_price"],
                row["created_ts"],
                row["etl_inserted"],
            )
            for row in transformed_dataset.current_data.collect()
        ]

        assert actual_data == expected_data, (
            "Transformed data does not match expected data"
        )
