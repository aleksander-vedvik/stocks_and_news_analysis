from pyspark.sql import SparkSession
from delta import *
from delta.tables import *
import sys

def run_local():
    for arg in sys.argv:
        if arg == "local":
            return True
    return False

def get_spark():
    builder = SparkSession.builder.appName("Project") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def main():
    # This script is only used to view the finished results
    path = "/data/delta/" 
    path_csv = "/data/csv/" 
    if run_local():
        path = "../data/delta/" 
        path_csv = "../data/csv/" 
    spark = get_spark()
    dsec = DeltaTable.forPath(spark, path + "delta_sector_corr")
    dstc = DeltaTable.forPath(spark, path + "delta_stock_corr")
    dseg = DeltaTable.forPath(spark, path + "delta_sector_grouped")
    dstg = DeltaTable.forPath(spark, path + "delta_stock_grouped")
    ds = DeltaTable.forPath(spark, path + "delta_stock")
    dn = DeltaTable.forPath(spark, path + "delta_news")

    dsec.toDF().show()
    dstc.toDF().show()
    dseg.toDF().show()
    dstg.toDF().show()
    ds.toDF().show()
    dn.toDF().show()
    
    dsec.toDF().write.mode("overwrite").options(header='True', delimiter=',').csv(path_csv + "sector_correlation")
    dstc.toDF().write.mode("overwrite").options(header='True', delimiter=',').csv(path_csv + "stock_correlation")
    dseg.toDF().write.mode("overwrite").options(header='True', delimiter=',').csv(path_csv + "sector_grouped")
    dstg.toDF().write.mode("overwrite").options(header='True', delimiter=',').csv(path_csv + "stock_grouped")
    ds.toDF().write.mode("overwrite").options(header='True', delimiter=',').csv(path_csv + "stocks")
    dn.toDF().write.mode("overwrite").options(header='True', delimiter=',').csv(path_csv + "news")

if __name__ == "__main__":
    if run_local():
        print("Running with local file paths\n")
    main()
