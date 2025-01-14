from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, expr
from pyspark.sql.functions import mean as spark_mean
from etl.layers.gold.wide_order_items_gold import WideOrderItemsGoldETL
from etl.utils.base_table import ETLDataSet, TableETL


class DailyCategoryMetricsGoldETL(TableETL):
    """
    DailyCategoryMetricsGoldETL is a class that extends the TableETL base class to perform ETL operations for daily category metrics in the gold layer.

    Attributes:
        spark (SparkSession): The Spark session to use for ETL operations.
        upstream_tables (Optional[List[Type[TableETL]]]): List of upstream ETL table classes to extract data from.
        name (str): The name of the ETL table.
        primary_keys (List[str]): List of primary key columns for the ETL table.
        storage_path (str): The storage path for the ETL table data.
        data_format (str): The data format for the ETL table (e.g., "delta").
        database (str): The database name for the ETL table.
        partition_keys (List[str]): List of partition key columns for the ETL table.
        run_upstream (bool): Flag to indicate whether to run upstream ETL processes.
        write_data (bool): Flag to indicate whether to write data to storage.

    Methods:
        extract_upstream() -> List[ETLDataSet]:
            Returns a list of datasets extracted from the upstream ETL tables.

        transform_upstream(upstream_datasets: List[ETLDataSet]) -> ETLDataSet:

        read(partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
    """

    def __init__(
        self,
        spark: SparkSession,
        upstream_tables: Optional[List[Type[TableETL]]] = [
            WideOrderItemsGoldETL,
        ],
        name: str = "daily_category_metrics",
        primary_keys: List[str] = ["order_date", "category"],
        storage_path: str = "s3a://rainforest/delta/gold/daily_category_metrics",
        data_format: str = "delta",
        database: str = "rainforest",
        partition_keys: List[str] = ["etl_inserted"],
        run_upstream=True,
        write_data=True,
    ) -> None:
        super().__init__(
            spark,
            upstream_tables,
            name,
            primary_keys,
            storage_path,
            data_format,
            database,
            partition_keys,
            run_upstream,
            write_data,
        )

    def extract_upstream(self) -> List[ETLDataSet]:
        """
        Extracts data from upstream ETL tables.
        This method initializes each upstream ETL table class, runs the ETL process
        if specified, and reads the data from each table. The extracted data is
        collected into a list of ETLDataSet objects.
        Returns:
            List[ETLDataSet]: A list of datasets extracted from the upstream ETL tables.
        """

        upstream_etl_datasets = []
        for table_etl_class in self.upstream_tables:
            table = table_etl_class(
                spark=self.spark,
                run_upstream=self.run_upstream,
                write_data=self.write_data,
            )
            if self.run_upstream:
                table.run()

            upstream_etl_datasets.append(table.read())

        return upstream_etl_datasets

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        """
        Transforms the upstream datasets to generate daily category metrics.
        Args:
            upstream_datasets (List[ETLDataSet]): List of upstream ETL datasets.
                The first dataset in the list is expected to contain order data.
        Returns:
            ETLDataSet: A new ETLDataSet containing the transformed daily category metrics data.
        Transformation Steps:
            1. Extracts the current data from the first upstream dataset.
            2. Casts the 'order_ts' column to 'order_date'.
            3. Filters the data to include only active orders.
            4. Explodes the 'categories' column to create a row for each category.
            5. Groups the data by 'order_date' and 'category' and calculates:
                - Mean of 'actual_price' as 'mean_actual_price'.
                - Median of 'actual_price' as 'median_actual_price' using percentile approximation.
            6. Adds a column 'etl_inserted' with the current timestamp.
            7. Creates a new ETLDataSet with the transformed data and updates the current data.
        """

        wide_orders_data = upstream_datasets[0].current_data
        wide_orders_data = wide_orders_data.withColumn(
            "order_date", col("created_ts").cast("date")
        )

        wide_orders_data = wide_orders_data.filter(col("is_active"))

        data_exploded = wide_orders_data.select(
            "order_id",
            "order_date",
            "product_id",
            "categories",
            "actual_price",
            explode("categories").alias("category"),
            "etl_inserted",
        )

        category_metrics_data = data_exploded.groupBy(
            "order_date",
            "category",
        ).agg(
            spark_mean("actual_price").alias("mean_actual_price"),
            expr("percentile_approx(actual_price, 0.5)").alias("median_actual_price"),
        )

        current_timestamp = datetime.now()

        category_metrics_data = category_metrics_data.withColumn(
            "etl_inserted", lit(current_timestamp)
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            current_data=category_metrics_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        self.current_data = etl_dataset.current_data

        return etl_dataset

    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
        """
        Reads data from the specified storage path and returns it as an ETLDataSet.
        Args:
            partition_values (Optional[Dict[str, str]]): A dictionary of partition key-value pairs to filter the data.
                                                         If None, the latest partition will be used.
        Returns:
            ETLDataSet: An object containing the selected data, metadata, and configuration details.
        Raises:
            Exception: If there is an error reading the data or filtering the partitions.
        """

        selected_columns = [
            col("order_date"),
            col("category"),
            col("mean_actual_price"),
            col("median_actual_price"),
            col("etl_inserted"),
        ]

        if not self.write_data:
            return ETLDataSet(
                name=self.name,
                current_data=self.current_data.select(selected_columns),
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )

        elif partition_values:
            partition_filter = " AND".join(
                [f"{k} = '{v}'" for k, v in partition_values.items()]
            )
        else:
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .selectExpr("max(etl_inserted)")
                .collect()[0][0]
            )

            partition_filter = f"etl_inserted = '{latest_partition}'"

        fact_order_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        fact_order_data = fact_order_data.select(selected_columns)

        etl_dataset = ETLDataSet(
            name=self.name,
            current_data=fact_order_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
