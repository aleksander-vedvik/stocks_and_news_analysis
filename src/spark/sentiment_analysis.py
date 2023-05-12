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

def get_spark(master="local[*]", name="Sentiment analysis"):
    path_to_files = "/home/ubuntu/project/src/spark/"
    builder = SparkSession.builder.appName(name)
    if run_local():
        SPARK_JARS = ["com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.0"]
        path_to_files = "./"
        builder.config("spark.ui.port", "4050")
        builder.config("spark.jars.packages", ",".join(SPARK_JARS))
        builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        builder.config("spark.kryoserializer.buffer.max", "2000M")
        builder.config("spark.driver.maxResultSize", "0")
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.addPyFile(path_to_files + "storage.py")
    sc.addPyFile(path_to_files + "transformations.py")
    sc.addPyFile(path_to_files + "udfs.py")
    return spark

def main():
    spark = get_spark()
    # Get the news articles dataframe from the csv file from Hadoop
    df_news = Storage.get_news_dataframe(spark, run_local())
    T = Transformations()
    # Add sentiment analysis and add time period
    df_news_sentiment_analysis = T.add_sentiment_analysis(df_news)   
    df_news_with_time_period = T.add_time_period(df_news_sentiment_analysis)
    # Save the result as a parquet file (it is later saved in delta table in main.py)
    Storage.save_news_to_parquet(df_news_with_time_period, run_local())
        
if __name__ == "__main__":
    if run_local():
        print("Running with local file paths\n")
    main()
