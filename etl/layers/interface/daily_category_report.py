from pyspark.sql.functions import col


def create_daily_category_report_view(daily_category_metrics_data):
    """
    Creates a global temporary view named 'daily_category_report' from the provided DataFrame.
    The function renames the columns of the input DataFrame as follows:
    - 'order_data' to 'Date'
    - 'category' to 'Product Category'
    - 'mean_actual_price' to 'Mean Revenue'
    - 'median_actual_price' to 'Median Revenue'
    Args:
        daily_category_metrics_data (DataFrame): A Spark DataFrame containing daily category metrics data.
    """

    renamed_data = daily_category_metrics_data.select(
        col("order_data").alias("Date"),
        col("category").alias("Product Category"),
        col("mean_actual_price").alias("Mean Revenue"),
        col("median_actual_price").alias("Median Revenue"),
    )

    renamed_data.createOrReplaceGlobalTempView("daily_category_report")
