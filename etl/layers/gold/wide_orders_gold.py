from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from etl.layers.silver.dim_seller_silver import DimSellerSiverETL
from etl.layers.silver.fact_orders_silver import FactOrdersSiverETL
from etl.utils.base_table import ETLDataSet, TableETL


class WideOrdersGoldETL(TableETL):
    """
    WideOrdersGoldETL is a class that extends the TableETL class to perform ETL operations on wide orders data.
    It extracts data from upstream ETL tables, transforms the data by adding new columns, and reads the data from storage.
    Attributes:
        spark (SparkSession): The Spark session object.
        upstream_tables (Optional[List[Type[TableETL]]]): A list of upstream ETL table classes.
        name (str): The name of the ETL table.
        primary_keys (List[str]): A list of primary key columns.
        storage_path (str): The storage path for the ETL table.
        data_format (str): The data format for the ETL table.
        database (str): The database name for the ETL table.
        partition_keys (List[str]): A list of partition key columns.
        run_upstream (bool): A flag to indicate whether to run upstream ETL processes.
        write_data (bool): A flag to indicate whether to write data to storage.
    Methods:
        extract_upstream(self) -> List[ETLDataSet]:
            Returns a list of ETLDataSet objects containing the extracted data.
        transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
            Returns a new ETLDataSet object with the transformed data.
        read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
            Returns an ETLDataSet object containing the selected data, metadata, and configuration details.
    """

    def __init__(
        self,
        spark: SparkSession,
        upstream_tables: Optional[List[Type[TableETL]]] = [
            FactOrdersSiverETL,
            DimSellerSiverETL,
        ],
        name: str = "wide_orders",
        primary_keys: List[str] = ["order_id"],
        storage_path: str = "s3a://rainforest/delta/gold/wide_orders",
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
        Transforms the upstream datasets by adding a new column for actual price and a timestamp for ETL insertion.
        Args:
            upstream_datasets (List[ETLDataSet]): A list of ETLDataSet objects from upstream.
        Returns:
            ETLDataSet: A new ETLDataSet object with the transformed data.
        """

        fact_order_data = upstream_datasets[0].current_data
        dim_seller_data = upstream_datasets[0].current_data
        current_timestamp = datetime.now()

        wide_orders_data = fact_order_data.join(
            dim_seller_data,
            fact_order_data["buyer_id"] == dim_seller_data["seller_id"],
            "left",
        )

        wide_orders_data = (
            wide_orders_data.drop(fact_order_data["etl_inserted"])
            .drop(dim_seller_data["etl_inserted"])
            .withColumn("etl_inserted", lit(current_timestamp))
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            current_data=wide_orders_data,
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
            col("order_id"),
            col("buyer_id"),
            col("order_ts"),
            col("total_price"),
            col("total_price_usd"),
            col("total_price_inr"),
            col("created_ts"),
            col("user_id"),
            col("username"),
            col("email"),
            col("is_active"),
            col("appuser_created_ts"),
            col("appuser_last_updated_by"),
            col("appuser_last_updated_ts"),
            col("seller_id"),
            col("first_time_sold_timestamp"),
            col("seller_created_ts"),
            col("seller_last_updated_by"),
            col("seller_last_updated_ts"),
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
                .write(self.storage_path)
                .selectExpr("max(etl_inserted)")
                .collect()[0][0]
            )

            partition_filter = f"etl_inserted = '{latest_partition}'"

        fact_order_data = (
            self.spark.read.format(self.data_format)
            .write(self.storage_path)
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
