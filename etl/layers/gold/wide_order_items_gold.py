from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, collect_list, struct
from etl.layers.silver.dim_category_silver import DimCategorySiverETL
from etl.layers.silver.dim_product_silver import DimProductSiverETL
from etl.layers.silver.dim_seller_silver import DimSellerSiverETL
from etl.layers.silver.fact_order_items_silver import FactOrderItemsSiverETL
from etl.layers.silver.brg_product_category_silver import ProductCategorySiverETL
from etl.utils.base_table import ETLDataSet, TableETL


class WideOrderItemsGoldSiverETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_tables: Optional[List[Type[TableETL]]] = [
            FactOrderItemsSiverETL,
            DimProductSiverETL,
            DimSellerSiverETL,
            ProductCategorySiverETL,
            DimCategorySiverETL,
        ],
        name: str = "wide_order_items",
        primary_keys: List[str] = ["order_item_id"],
        storage_path: str = "s3a://rainforest/delta/silver/wide_order_items",
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
        Transforms the upstream datasets by adding a new column for actual price and a timestamp for ETL insertion.
        Args:
            upstream_datasets (List[ETLDataSet]): A list of ETLDataSet objects from upstream.
        Returns:
            ETLDataSet: A new ETLDataSet object with the transformed data.
        """

        fact_order_items_data = upstream_datasets[0].current_data
        dim_product_data = upstream_datasets[1].current_data
        dim_seller_data = upstream_datasets[2].current_data
        product_category_data = upstream_datasets[3].current_data
        dim_category_data = upstream_datasets[4].current_data
        current_timestamp = datetime.now()

        wide_order_items_data = fact_order_items_data.join(
            dim_product_data,
            "product_id",
            "left",
        )

        product_category_enriched_data = (
            product_category_data.join(dim_category_data, "category_id")
            .drop(dim_category_data["etl_inserted"])
            .drop(product_category_data["etl_inserted"])
        )

        product_category_data_product_grain = product_category_enriched_data.groupby(
            "product_id"
        ).agg(collect_list(struct("category_id", "category_name")).alias("categories"))

        wide_order_items_data = wide_order_items_data.join(
            product_category_data_product_grain,
            "product_id",
            "left",
        )

        wide_order_items_data = (
            wide_order_items_data.drop(fact_order_items_data["etl_inserted"])
            .drop(dim_product_data["etl_inserted"])
            .drop(dim_seller_data["etl_inserted"])
            .withColumn("etl_inserted", lit(current_timestamp))
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            current_data=wide_order_items_data,
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
            col("order_item_id"),
            col("order_id"),
            col("product_id"),
            col("seller_id"),
            col("quantity"),
            col("base_price"),
            col("actual_price"),
            col("created_ts"),
            col("tax"),
            col("categories"),
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
