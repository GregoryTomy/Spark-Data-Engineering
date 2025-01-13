from pyspark.sql import SparkSession

from etl.layers.gold.daily_category_metrics import DailyCategoryMetricsGoldETL
from etl.layers.gold.daily_order_metrics import DailyOrderMetricsGoldETL
from etl.layers.interface.daily_category_report import create_daily_category_report_view
from etl.layers.interface.daily_order_report import create_daily_order_report_view


def run_etl(spark):
    print("===================================")
    print("Daily Category Report")
    print("===================================")

    daily_category_metrics = DailyCategoryMetricsGoldETL(spark=spark)
    daily_category_metrics.run()

    create_daily_category_report_view(daily_category_metrics.read().current_data)
    spark.sql("select * from global_temp.daily_category_report").show()

    print("===================================")
    print("Daily Order Report")
    print("===================================")

    daily_orders_metrics = DailyOrderMetricsGoldETL(spark=spark)
    daily_orders_metrics.run()

    create_daily_order_report_view(daily_orders_metrics.read().current_data)
    spark.sql("select * from global_temp.daily_order_report").show()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Rainforest Data Pipeline")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    run_etl(spark)
