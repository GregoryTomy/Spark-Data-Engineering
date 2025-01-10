from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from etl.layers.bronze.appuser import AppUserBronzeETL
from etl.layers.bronze.seller import SellerBronzeETL
from etl.utils.base_table import ETLDataSet, TableETL


class DimSellerSiverETL(TableETL):
    """
    DimSellerSiverETL is a class that extends the TableETL class to perform ETL operations for the dim_seller table in the silver layer.
    Attributes:
        spark (SparkSession): The Spark session object.
        upstream_tables (Optional[List[Type[TableETL]]]): A list of upstream ETL table classes.
        name (str): The name of the ETL table.
        primary_keys (List[str]): A list of primary key columns.
        storage_path (str): The storage path for the ETL table.
        data_format (str): The data format for the ETL table.
        database (str): The database name for the ETL table.
        partition_keys (List[str]): A list of partition key columns.
        run_upstream (bool): A flag indicating whether to run upstream ETL processes.
        write_data (bool): A flag indicating whether to write data to storage.
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
            AppUserBronzeETL,
            SellerBronzeETL,
        ],
        name: str = "dim_seller",
        primary_keys: List[str] = ["seller_id"],
        storage_path: str = "s3a://rainforest/delta/silver/dim_seller",
        data_format: str = "detla",
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
        Transforms the upstream datasets by joining appuser and seller data, renaming common columns,
        and adding an ETL insertion timestamp.
        Args:
            upstream_datasets (List[ETLDataSet]): A list of ETLDataSet objects where the first element
                                                  is the appuser dataset and the second element is the seller dataset.
        Returns:
            ETLDataSet: A new ETLDataSet object containing the transformed data.
        Raises:
            ValueError: If the upstream_datasets list does not contain exactly two elements.
        """

        appuser_data = upstream_datasets[0].current_data
        seller_data = upstream_datasets[1].current_data
        current_timestamp = datetime.now()

        common_columns = set(appuser_data.columns).intersection(seller_data.columns)

        appuser_data = appuser_data.selectExpr(
            *[
                f"`{col}`as appuser_{col}"
                if col in common_columns and col != "user_id"
                else col
                for col in appuser_data.columns
            ]
        )

        seller_data = seller_data.selectExpr(
            *[
                f"`{col}`as seller_{col}"
                if col in common_columns and col != "user_id"
                else col
                for col in seller_data.columns
            ]
        )

        dim_seller_data = appuser_data.join(
            seller_data,
            appuser_data["user_id"] == seller_data["user_id"],
            "inner",
        )

        dim_seller_data = dim_seller_data.drop(seller_data["user_id"])

        tranformed_data = dim_seller_data.withColumn(
            "etl_inserted", lit(current_timestamp)
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
            col("user_id"),
            col("username"),
            col("email"),
            col("is_active"),
            col("appuser_created_ts"),
            col("appuser_last_updated_by"),
            col("appuser_last_updated_ts"),
            col("seller_id"),
            col("first_time_purchased_timestamp"),
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

        dim_seller_data = (
            self.spark.read.format(self.data_format)
            .write(self.storage_path)
            .filter(partition_filter)
        )

        dim_seller_data = dim_seller_data.select(selected_columns)

        etl_dataset = ETLDataSet(
            name=self.name,
            current_data=dim_seller_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
