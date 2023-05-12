from pyspark.sql import SparkSession
from storage import Storage
from transformations import Transformations
import sys
from delta import *

def run_local():
    for arg in sys.argv:
        if arg == "local":
            return True
    return False

def get_spark():
    path_to_files = "/home/ubuntu/project/src/spark/"
    if run_local():
        path_to_files = "./"

    builder = SparkSession.builder.appName("Project") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    sc = spark.sparkContext
    sc.addPyFile(path_to_files + "storage.py")
    sc.addPyFile(path_to_files + "transformations.py")
    sc.addPyFile(path_to_files + "udfs.py")
    return spark

def main():
    spark = get_spark()
    # Get dataframes
    df_news, df_sectors, df_stocks, df_companynames = Storage.get_dataframes(spark, run_local())
    df_sectors.cache()
    df_companynames.cache()
    T = Transformations()

    # Stop if there are no new stocks or news articles
    if df_news is None and df_stocks is None:
        return
    
    # Update delta table if there are new stocks.
    # Load all stocks from delta table.
    if df_stocks is not None:
        df_stocks_with_companynames = T.join_with_companynames(df_stocks, df_companynames)
        df_stocks_with_price_change = T.calculate_stock_price_change(df_stocks_with_companynames)
        df_stocks_with_price_change = Storage.save_stocks_to_delta(spark, df_stocks_with_price_change, run_local())
    else:
        delta_stock = Storage.read_delta(spark, run_local(), "delta_stock")
        if delta_stock == None:
            print("No new stocks and no existing stocks")
            return
        df_stocks_with_price_change = delta_stock.toDF()
    df_stocks_with_price_change.cache()
    
    # Get news articles from delta table and check if there are new news articles (no duplicates)
    delta_news, new = Storage.read_delta_or_create(spark, df_news, run_local(), "delta_news")
    if not new:
        df_news_with_time_period = T.compare_and_return_unique(delta_news, df_news)
    else:
        df_news_with_time_period = df_news
    df_news_with_time_period.cache() 
    # Stop if there are no news articles in delta table.
    if len(df_news_with_time_period.take(1)) <= 0:
        return
    
    # Save the new news articles in delta table
    Storage.save_news_to_delta(delta_news, df_news_with_time_period)

    # Do transformations to enhance dataset
    df_sector = T.search_for_sectors_and_aggregate(df_news_with_time_period, df_sectors) 
    df_sector_correlation = T.add_correlation_sector(df_sector, df_stocks_with_price_change)
    df_sector_grouped = T.groupby_sector(df_sector_correlation)

    # Do transformations to enhance dataset
    df_stock = T.search_for_stocks_and_aggregate(df_news_with_time_period, df_stocks_with_price_change)
    df_stock_correlation = T.add_correlation_stock(df_stock, df_stocks_with_price_change)
    df_stock_grouped = T.groupby_stock(df_stock_correlation)

    # Save results in delta table
    Storage.save_all_to_delta(spark, \
                                df_stock_correlation, \
                                df_sector_correlation, \
                                df_stock_grouped, \
                                df_sector_grouped, \
                                run_local())

if __name__ == "__main__":
    if run_local():
        print("Running with local file paths\n")
    main()
