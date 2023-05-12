from pyspark.sql.types import *
from delta import *
from delta.tables import *

news_schema = StructType([
                StructField('Title', StringType(), True),
                StructField('Url', StringType(), True),
                StructField('Date', StringType(), True),
                StructField('Authors', StringType(), True),
                StructField('Publisher', StringType(), True),
                StructField('ShortDescription', StringType(), True),
                StructField('Text', StringType(), True),
                StructField('Category', StringType(), True),
                StructField('Stock', StringType(), True),
                StructField('Country', StringType(), True)
                ])

stocks_schema = StructType([
                StructField('Timestamp', IntegerType(), True),
                StructField('Stockname', StringType(), True),
                StructField('Open', FloatType(), True),
                StructField('Close', FloatType(), True),
                StructField('Volume', IntegerType(), True),
                StructField('High', FloatType(), True),
                StructField('Low', FloatType(), True),
                ])

stockname_schema = StructType([
                StructField('Stockname', StringType(), True),
                StructField('Companyname', StringType(), True),
                StructField('Category', StringType(), True),
                ])

news_schema_parquet = StructType([
                StructField('Title', StringType(), True),
                StructField('Text', StringType(), True),
                StructField('ShortDescription', StringType(), True),
                StructField('Date', StringType(), True),
                StructField('sentiment', FloatType(), True),
                StructField('unix', IntegerType(), True),
                StructField('time_period', StringType(), True)
                ])

class Storage:
    # Used sentiment_analysis.py
    @staticmethod
    def get_news_dataframe(spark, local: bool):
        path = "/out/news/news.csv"
        if local:
            path = "../data/news.csv"
        df_news = spark.read.csv(path,header=False,schema=news_schema)
        return df_news

    # Used main.py
    @staticmethod
    def get_dataframes(spark, local: bool):
        paths = ["/out/news/news.parquet","/data/sectors.csv","/out/stocks/stocks.csv", "/data/companynames.csv"] 
        if local:
            paths = ["../data/news.parquet","../data/sectors.csv","../data/stocks.csv", "../data/companynames.csv"] 
        try:
            df_news = spark.read.parquet(paths[0], schema=news_schema_parquet)
            if len(df_news.take(1)) <= 0:
                df_news = None
        except Exception:
            df_news = None
        try:
            df_stocks = spark.read.csv(paths[2],header=False,schema=stocks_schema)
        except Exception:
            df_stocks = None
        df_sectors = spark.read.csv(paths[1],header=True,inferSchema=True)
        df_companynames = spark.read.csv(paths[3],header=True,schema=stockname_schema)
        return df_news, df_sectors, df_stocks, df_companynames

    # Used sentiment_analysis.py
    @staticmethod
    def save_news_to_parquet(df1,local: bool):
        path = "/out/news/" 
        if local:
            path = "../data/" 
        df1.write.mode("overwrite").parquet(path + "news.parquet")

    # Used in main.py (indirectly)
    @staticmethod
    def read_delta_or_create(spark, df, local: bool, filename: str):
        path = "/data/delta/" 
        if local:
            path = "../data/delta/" 
        created_new = False
        try:
            deltaTable = DeltaTable.forPath(spark, path + filename)
        except Exception:
            df.write.format("delta").save(path + filename)
            deltaTable = DeltaTable.forPath(spark, path + filename)
            created_new = True
        return deltaTable, created_new
        
    # Used in main.py
    @staticmethod
    def save_all_to_delta(spark, df_stock_correlation, df_sector_correlation, df_stock_grouped, df_sector_grouped, local: bool):
        Storage.save_stock_correlation_to_delta(spark, df_stock_correlation, local)
        Storage.save_sector_correlation_to_delta(spark, df_sector_correlation, local)
        Storage.save_stock_grouped_to_delta(spark, df_stock_grouped, local)
        Storage.save_sector_grouped_to_delta(spark, df_sector_grouped, local)
    
    # Used in main.py (indirectly)
    @staticmethod
    def save_stock_correlation_to_delta(spark, df_stock_correlation, local: bool):
        delta_stock_corr, new = Storage.read_delta_or_create(spark, df_stock_correlation, local, "delta_stock_corr")
        if not new:
            delta_stock_corr.alias('delta').merge(df_stock_correlation.alias('update'), 'delta.time_period == update.time_period AND delta.stock == update.stock') \
                .whenMatchedUpdate(set={
                    "sentiment": "update.sentiment",
                    "number_of_articles": "update.number_of_articles + delta.number_of_articles",
                    "correlation": "(update.correlation*update.number_of_articles + delta.correlation*delta.number_of_articles)/(delta.number_of_articles + update.number_of_articles)",
                }) \
                .whenNotMatchedInsert(values={
                    "stock": "update.stock",
                    "time_period": "update.time_period",
                    "number_of_articles": "update.number_of_articles",
                    "name": "update.name",
                    "sentiment": "update.sentiment",
                    "stock_price_change": "update.stock_price_change",
                    "correlation": "update.correlation",
                }).execute()

    # Used in main.py (indirectly)
    @staticmethod
    def save_sector_correlation_to_delta(spark, df_sector_correlation, local: bool):
        delta_sector_corr, new = Storage.read_delta_or_create(spark, df_sector_correlation, local, "delta_sector_corr")
        if not new:
            delta_sector_corr.alias('delta').merge(df_sector_correlation.alias('update'), 'delta.time_period == update.time_period AND delta.sector == update.sector') \
                .whenMatchedUpdate(set={
                    "sentiment": "update.sentiment",
                    "number_of_articles": "update.number_of_articles + delta.number_of_articles",
                    "correlation": "(update.correlation*update.number_of_articles + delta.correlation*delta.number_of_articles)/(delta.number_of_articles + update.number_of_articles)",
                }) \
                .whenNotMatchedInsert(values={
                    "sector": "update.sector",
                    "time_period": "update.time_period",
                    "number_of_articles": "update.number_of_articles",
                    "sentiment": "update.sentiment",
                    "stock_price_change": "update.stock_price_change",
                    "correlation": "update.correlation",
                }).execute()
                
    # Used in main.py (indirectly)
    @staticmethod
    def save_sector_grouped_to_delta(spark, df_sector_grouped, local: bool):
        delta_sector_grouped, new = Storage.read_delta_or_create(spark, df_sector_grouped, local, "delta_sector_grouped")
        if not new:
            delta_sector_grouped.alias('delta').merge(df_sector_grouped.alias('update'), 'delta.sector == update.sector') \
                .whenMatchedUpdate(set={
                    "number_of_articles": "update.number_of_articles + delta.number_of_articles",
                    "correlation": "(update.correlation*update.number_of_articles + delta.correlation*delta.number_of_articles)/(delta.number_of_articles + update.number_of_articles)",
                }) \
                .whenNotMatchedInsert(values={
                    "sector": "update.sector",
                    "number_of_articles": "update.number_of_articles",
                    "correlation": "update.correlation",
                }).execute()

    # Used in main.py (indirectly)
    @staticmethod
    def save_stock_grouped_to_delta(spark, df_stock_grouped, local: bool):
        delta_stock_grouped, new = Storage.read_delta_or_create(spark, df_stock_grouped, local, "delta_stock_grouped")
        if not new:
            delta_stock_grouped.alias('delta').merge(df_stock_grouped.alias('update'), 'delta.stock == update.stock') \
                .whenMatchedUpdate(set={
                    "number_of_articles": "update.number_of_articles + delta.number_of_articles",
                    "correlation": "(update.correlation*update.number_of_articles + delta.correlation*delta.number_of_articles)/(delta.number_of_articles + update.number_of_articles)",
                }) \
                .whenNotMatchedInsert(values={
                    "stock": "update.stock",
                    "name": "update.name",
                    "number_of_articles": "update.number_of_articles",
                    "correlation": "update.correlation",
                }).execute()
                
    # Used in main.py
    @staticmethod
    def save_stocks_to_delta(spark, df_stocks, local: bool):
        delta_stock, new = Storage.read_delta_or_create(spark, df_stocks, local, "delta_stock")
        if not new:
            delta_stock.alias('delta').merge(df_stocks.alias('update'),
                                             'delta.time_period == update.time_period AND delta.symbol == update.symbol'
                                             ) \
                .whenNotMatchedInsert(values={
                    "time_period": "update.time_period",
                    "symbol": "update.symbol",
                    "Companyname": "update.Companyname",
                    "category": "update.category",
                    "stock_price_change": "update.stock_price_change",
                    "first_value": "update.first_value",
                    "first_date": "update.first_date",
                    "last_value": "update.last_value",
                    "last_date": "update.last_date",
                }).execute()
        return delta_stock.toDF()
    
    # Used in main.py
    @staticmethod
    def save_news_to_delta(delta_news, df_news):
        delta_news \
            .alias('delta') \
            .merge(df_news.alias('update'),
                   'delta.title == update.title AND delta.text == update.text AND delta.date == update.date'
                   ) \
            .whenNotMatchedInsert(values={
                "title": "update.title",
                "text": "update.text",
                "shortdescription": "update.shortdescription",
                "date": "update.date",
                "sentiment": "update.sentiment",
                "unix": "update.unix",
                "time_period": "update.time_period",
            }).execute()