from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from etl.utils.base_table import ETLDataSet, TableETL
from etl.utils.database import get_upstream_table


class ManufacturerBronzeETL(TableETL):
    """
    ManufacturerBronzeETL is a class that handles the ETL (Extract, Transform, Load) process for the 'manufacturer' table in the bronze layer of the data lake.

    Attributes:
        spark (SparkSession): The Spark session object.
        upstream_tables (Optional[List[Type[TableETL]]]): A list of upstream TableETL objects.
        name (str): The name of the table.
        primary_keys (List[str]): A list of primary key columns.
        storage_path (str): The storage path for the table data.
        data_format (str): The data format used for storage (e.g., "delta").
        database (str): The database name.
        partition_keys (List[str]): A list of partition key columns.
        run_upstream (bool): A flag indicating whether to run upstream ETL processes.
        write_data (bool): A flag indicating whether to write data to storage.
        current_data (DataFrame): The current DataFrame being processed.

    Methods:
        extract_upstream() -> List[ETLDataSet]:

        transform_upstream(upstream_datasets: List[ETLDataSet]) -> List[ETLDataSet]:

        read(partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
            Reads the data from the storage path, optionally filtering by partition values, and returns it as an ETLDataSet object.
    """

    def __init__(
        self,
        spark: SparkSession,
        upstream_tables: Optional[List[Type[TableETL]]] = None,
        name: str = "manufacturer",
        primary_keys: List[str] = ["manufacturer_id"],
        storage_path: str = "s3a://rainforest/delta/bronze/manufacturer",
        data_format: str = "delta",
        database: str = "rainforest",
        partition_keys: List[str] = ["etl_inserted"],
        run_upstream: bool = True,
        write_data: bool = True,
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
        self.current_data = None

    def extract_upstream(self) -> List[ETLDataSet]:
        """
        Extracts data from an upstream source and returns it as a list of ETLDataSet objects.

        This method retrieves manufacturer data from a specified table in the upstream source,
        loads it into a DataFrame, and then creates an ETLDataSet object with the
        extracted data and relevant metadata.

        Returns:
            List[ETLDataSet]: A list containing a single ETLDataSet object with the
            extracted manufacturer data.
        """
        table_name = "rainforest.manufacturer"
        manufacturer_data = get_upstream_table(table_name, self.spark)

        etl_dataset = ETLDataSet(
            name=self.name,
            current_data=manufacturer_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return [etl_dataset]

    def transform_upstream(
        self, upstream_datasets: List[ETLDataSet]
    ) -> List[ETLDataSet]:
        """
        Transforms the upstream datasets by adding a new column 'etl_inserted' with the current timestamp.

        Args:
            upstream_datasets (List[ETLDataSet]): A list of ETLDataSet objects from upstream.

        Returns:
            List[ETLDataSet]: A list containing the transformed ETLDataSet with the new 'etl_inserted' column.
        """
        manufacturer_data = upstream_datasets[0].current_data
        current_timestamp = datetime.now()

        tranformed_data = manufacturer_data.withColumn(
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
        Reads data from the storage path and returns an ETLDataSet object.
        If `write_data` is False, returns an ETLDataSet with the current data.
        If `partition_values` is provided, filters the data based on the given partition values.
        Otherwise, filters the data to get the latest partition based on the 'etl_inserted' column.
        Args:
            partition_values (Optional[Dict[str, str]]): A dictionary of partition key-value pairs to filter the data.
        Returns:
            ETLDataSet: An object containing the filtered data and metadata.
        """

        if not self.write_data:
            return ETLDataSet(
                name=self.name,
                current_data=self.current_data,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )
        elif partition_values:
            partition_filter = " AND ".join(
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

        manufacturer_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        manufacturer_data = manufacturer_data.select(
            col("manufacturer_id"),
            col("name"),
            col("type"),
            col("created_ts"),
            col("last_updated_by"),
            col("last_updated_ts"),
            col("etl_inserted"),
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            current_data=manufacturer_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
