from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from etl.layers.bronze.orders import OrdersBronzeETL
from etl.utils.base_table import ETLDataSet, TableETL


class FactOrdersSiverETL(TableETL):
    """
    FactOrdersSiverETL is a class that extends the TableETL class to perform ETL operations for the fact_order table in the silver layer.
    Attributes:
        spark (SparkSession): The Spark session object used for data processing.
        upstream_tables (Optional[List[Type[TableETL]]]): A list of upstream ETL table classes.
        name (str): The name of the ETL table.
        primary_keys (List[str]): A list of primary key columns for the ETL table.
        storage_path (str): The storage path for the ETL table data.
        data_format (str): The data format used for storing the ETL table data.
        database (str): The database name where the ETL table resides.
        partition_keys (List[str]): A list of partition key columns for the ETL table.
        run_upstream (bool): A flag indicating whether to run the upstream ETL processes.
        write_data (bool): A flag indicating whether to write the data to storage.
    Methods:
        __init__(self, spark: SparkSession, upstream_tables: Optional[List[Type[TableETL]]], name: str, primary_keys: List[str], storage_path: str, data_format: str, database: str, partition_keys: List[str], run_upstream: bool, write_data: bool) -> None:
            Initializes the FactOrdersSiverETL class with the specified parameters.
        extract_upstream(self) -> List[ETLDataSet]:
            Returns a list of datasets extracted from the upstream ETL tables.
        transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
            Returns a new ETLDataSet object with the transformed data.
        read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
            Returns an object containing the selected data, metadata, and configuration details.
    """

    def __init__(
        self,
        spark: SparkSession,
        upstream_tables: Optional[List[Type[TableETL]]] = [
            OrdersBronzeETL,
        ],
        name: str = "fact_orders",
        primary_keys: List[str] = ["order_id"],
        storage_path: str = "s3a://rainforest/delta/silver/fact_orders",
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

        order_data = upstream_datasets[0].current_data
        current_timestamp = datetime.now()

        usd_conversion_rate = 0.0116
        inr_conversion_rate = 86

        tranformed_data = (
            order_data.withColumn(
                "total_price_usd", col("total_price") * lit(usd_conversion_rate)
            )
            .withColumn(
                "total_price_inr", col("total_price") * lit(inr_conversion_rate)
            )
            .withColumn("etl_inserted", lit(current_timestamp))
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            current_data=tranformed_data,
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
