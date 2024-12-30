from __future__ import annotations

import os
import logging
from abc import abstractmethod, ABC
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Type, Optional

import great_expectations as gx
from pyspark.sql import DataFrame

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class InvalidDataException(Exception):
    """
    Exception raised for errors in the input data.

    Attributes:
        message -- explanation of the error
    """

    pass


@dataclass
class ETLDataSet:
    """
    ETLDataSet class represents a dataset used in an ETL (Extract, Transform, Load) process.

    Attributes:
        name (str): The name of the dataset.
        current_data (DataFrame): The current data in the dataset.
        primary_keys (List[str]): The list of primary key columns for the dataset.
        storage_path (str): The storage path where the dataset is stored.
        data_format (str): The format of the data (e.g., CSV, Parquet).
        database (str): The database where the dataset is stored.
        partition_keys (List[str]): The list of partition key columns for the dataset.
    """

    name: str
    current_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    data_format: str
    database: str
    partition_keys: List[str]


class TableETL(ABC):
    """
    TableETL is an abstract base class that defines the structure for an ETL (Extract, Transform, Load) process.
    Subclasses must implement the abstract methods to define the specific logic for extracting, transforming,
    and reading data.

    Attributes:
        spark: The Spark session object.
        upstream_tables (Optional[List[Type[TableETL]]]): A list of upstream TableETL instances.
        name (str): The name of the table.
        primary_keys (List[str]): A list of primary key columns.
        storage_path (str): The path where the data will be stored.
        data_format (str): The format of the data (e.g., "parquet", "csv").
        database (str): The database name.
        partition_keys (List[str]): A list of partition key columns.
        run_upstream (bool): A flag indicating whether to run upstream ETL processes.
        write_data (bool): A flag indicating whether to write the data to storage.

    Methods:
        extract_upstream: Extracts data from upstream sources. Must be implemented by subclasses.
        transform_upstream: Transforms the given list of upstream datasets and returns a single ETLDataSet. Must be implemented by subclasses.
        read: Reads data based on the given partition values. Must be implemented by subclasses.
        validate: Validates the given ETL dataset using Great Expectations.
        write: Writes the given ETL dataset into storage.
        run: Executes the ETL process by extracting, transforming, validating, and writing the data.
    """

    @abstractmethod
    def __init__(
        self,
        spark,
        upstream_tables: Optional[List[Type[TableETL]]],
        name: str,
        primary_keys: List[str],
        storage_path: str,
        data_format: str,
        database: str,
        partition_keys: List[str],
        run_upstream: bool = True,
        write_data: bool = True,
    ) -> None:
        logging.info(f"Initializing ETL Table: {name}")

        self.spark = spark
        self.upstream_tables = upstream_tables
        self.name = name
        self.primary_keys = primary_keys
        self.storage_path = storage_path
        self.data_format = data_format
        self.database = database
        self.partition_keys = partition_keys
        self.run_upstream = run_upstream
        self.write_data = write_data

    @abstractmethod
    def extract_upstream(self) -> List[ETLDataSet]:
        """
        Extracts data from upstream sources.

        This method is intended to be overridden by subclasses to implement
        the logic for extracting data from upstream sources.

        Returns:
            List[ETLDataSet]: A list of ETLDataSet objects containing the extracted data.
        """
        logging.info(f"Extracting upstream datasets for table: {self.name}")
        pass

    @abstractmethod
    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        """
        Transforms the given list of upstream datasets and returns a single ETLDataSet.

        Args:
            upstream_datasets (List[ETLDataSet]): A list of ETLDataSet objects representing the upstream datasets.

        Returns:
            ETLDataSet: A single ETLDataSet object resulting from the transformation of the upstream datasets.
        """
        logging.info(f"Transforming upstream datasets for table: {self.name}")
        pass

    @abstractmethod
    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
        logging.info(f"Reading data for table: {self.name}")
        pass

    def validate(self, data: ETLDataSet) -> bool:
        """
        Validates the given ETL dataset using Great Expectations.

        This method checks if an expectation file exists for the dataset. If the file does not exist,
        it logs an informational message and skips validation. If the file exists, it creates a
        Great Expectations context and runs a checkpoint to validate the dataset against the
        expectations defined in the file.

        Args:
            data (ETLDataSet): The ETL dataset to be validated.

        Returns:
            bool: True if the validation is successful or skipped, False if the validation fails.
        """
        logging.info(f"Validating dataset for table: {self.name}")

        ge_path = "etl/great_expectations"
        expec_json_path = f"{ge_path}/expectations/{self.name}.json"
        file_path = Path(expec_json_path)

        if not file_path.exists():
            logging.info(
                f"Expectation file {expec_json_path} does not exist. Skipping validation."
            )
            return True

        try:
            context = gx.get_context(
                context_root_dir=os.path.join(os.getcwd(), "etl", "great_expectations")
            )

            batch_request = (
                context.get_datasource("spark_datasource")
                .get_asset(self.name)
                .build_batch_request(dataframe=data.current_data)
            )

            validations = [
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": self.name,
                }
            ]

            result = context.run_checkpoint(
                checkpoint_name="data_quality_checkpoint", validations=validations
            ).list_validation_results()

            logging.info(f"Validation results for {self.name}: {result}")

            return result

        except Exception as e:
            logging.error(f"Validation failed for {self.name}: {e}")

            return False

    def write(self, data: ETLDataSet) -> None:
        """
        Writes given ETL dataset into storage.

        Args:
            data (ETLDataSet): The dataset to be written, which includes the current data,
                               data format, partition keys, and storage path.

        Returns:
            None
        """
        logging.info(f"Writing dataset to storage for table: {self.name}")
        try:
            (
                data.current_data.write.option("mergeSchema", "true")
                .format(data.data_format)
                .mode("overwrite")
                .partitionBy(data.partition_keys)
                .save(data.storage_path)
            )
        except Exception as e:
            logging.error(f"Failed to write dataset to storage for {self.name}: {e}")
            raise

    def run(self) -> None:
        """
        Executes the ETL process by extracting, transforming, validating, and writing the data.

        This method orchestrates the ETL process by first extracting data from upstream sources,
        then transforming the extracted data, validating the transformed data, and finally writing
        the validated data to the storage if the validation is successful.
        """
        try:
            transformed_data = self.transform_upstream(self.extract_upstream())
            if not self.validate(transformed_data):
                raise InvalidDataException(
                    f"The {self.name} dataset did not pass validation, please check the metadata db for more information."
                )
            if self.write_data:
                self.write(transformed_data)
        except Exception as e:
            logging.error(f"ETL process failed for {self.name}: {e}")
            raise
