import os

from pyspark.sql import SparkSession


from pyspark.sql import DataFrame


def get_upstream_table(table_name: str, spark: SparkSession) -> DataFrame:
    """
    Reads a table from an upstream PostgreSQL database into a Spark DataFrame.

    This function connects to an upstream PostgreSQL database using JDBC and reads
    the specified table into a Spark DataFrame. The connection details such as host,
    port, database name, username, and password are retrieved from environment variables.
    Default values are provided if the environment variables are not set.

    Args:
        table_name (str): The name of the table to read from the upstream database.
        spark (SparkSession): The SparkSession object used to read the table.

    Returns:
        DataFrame: A Spark DataFrame containing the data from the specified table.
    """
    host = os.getenv("UPSTREAM_HOST", "upstream")
    port = os.getenv("UPSTREAM_PORT", "5432")
    database = os.getenv("UPSTREAM_DATABASE", "upstreamdb")
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

    connection_properties = {
        "user": os.getenv("UPSTREAM_USERNAME", "sdeuser"),
        "password": os.getenv("UPSTREAM_PASSWORD", "sdepassword"),
        "driver": "org.postgresql.Driver",
    }

    return spark.read.jdbc(
        url=jdbc_url, table=table_name, properties=connection_properties
    )
