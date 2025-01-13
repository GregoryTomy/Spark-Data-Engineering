from pyspark.sql.functions import col


def create_daily_order_report_view(daily_order_metrics_data):
    """
    Creates a global temporary view for the daily order report.
    This function takes a DataFrame containing daily order metrics data, renames
    specific columns to more user-friendly names, and creates or replaces a global
    temporary view named "daily_order_report".
    Args:
        daily_order_metrics_data (DataFrame): A DataFrame containing the daily order
                                              metrics data with columns "order_date",
                                              "total_price_sum", and "total_price_mean".
    Returns:
        None
    """

    renamed_data = daily_order_metrics_data.select(
        col("order_date").alias("Date"),
        col("total_price_sum").alias("Revenue"),
        col("total_price_mean").alias("Mean Revenue"),
    )

    renamed_data.createOrReplaceGlobalTempView("daily_order_report")
