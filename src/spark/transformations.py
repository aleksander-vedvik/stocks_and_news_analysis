from pyspark.sql.functions import col, explode, count
import pyspark.sql.functions as F
from udfs import UDF
from pyspark.ml import Pipeline
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.sql.functions import when, col, concat_ws, explode, first, monotonically_increasing_id, regexp_replace, broadcast
from delta import *
from delta.tables import *

class Transformations:
    def __init__(self):
        self.UDF = UDF()

    # OLD (not used)
    # WAS USED IN CONJUNCTION WITH add_stocks_sectors_sentiment()
    # REPLACED BY:
    #   - add_sentiment_analysis
    #   - search_for_sectors_and_aggregate
    #   - search_for_stocks_and_aggregate
    # First: Converts each element in the sector array to a separate row.
    # Second: Groups each row by sector and time period. Counts the number of articles for each.
    #   Input:
    #             sectors
    #       ------------------
    #       [Tech, Energy, ...]
    #
    #   Output:
    #          sector
    #       -----------
    #           Tech
    #          Energy
    def explode_sectors_and_aggregate(self, df):
        return df \
            .withColumn("sector", explode("sectors")) \
            .groupBy("sector", "time_period") \
            .agg(count("*") \
            .alias("number_of_articles"), \
            F.avg("sentiment").alias("sentiment")) \
            .orderBy(col("sector").asc(), col("time_period").desc()) 

    # OLD (not used)
    # REPLACED BY:
    #   - add_sentiment_analysis
    #   - search_for_sectors_and_aggregate
    #   - search_for_stocks_and_aggregate
    # Uses user defined functions
    def add_stocks_sectors_sentiment(self, df, df_stocks_combined, df_sectors_combined):
        return df \
            .withColumn("unix", self.UDF.clean_date(col("date"))) \
            .withColumn("time_period", self.UDF.convert_unix_to_date(col("unix"))) \
            .withColumn("cleaned", self.UDF.clean(col("Title"), col("Text"), col("ShortDescription"))) \
            .join(df_stocks_combined) \
            .withColumn("stocks", self.UDF.find_stocks(col("cleaned"), col("all_stocks"))) \
            .join(df_sectors_combined) \
            .withColumn("sectors", self.UDF.find_sectors(col("cleaned"), col("all_sectors"))) \
            .withColumn("sentiment", self.UDF.sentiment_analysis(col("cleaned"))) \
            .select("title", "unix", "time_period", "stocks", "sectors", "sentiment") 
            
    # USED IN sentiment_analysis.py
    # REPLACES: add_stocks_sectors_sentiment()
    # First: Runs all rows through sparknlp and adds the result to column sentiment_text. Also adds an autoincrement ID to each article.
    # Second: Extracts the result and confidence from the sentiment_result column and adds to result and confidence columns, respectively.
    # Third: Groups by the given ID and chooses the result with highest confidence.
    # Fourth: Changes the result from negative, na and positive to -1, 0 and 1, respectively, and adds it to sentiment column.
    # Fifth: Joins the original dataframe to only get Title, Text, ShortDescription, Date and sentiment in a new dataframe.
    #   Input:
    #        Title | Text | ShortDescription |    Date    | ... 
    #        ---------------------------------------------------
    #        It's..| The..|  In New York ..  | 2020-12-12 | ...
    #
    #   Output:  
    #        Title | Text | ShortDescription |    Date    | sentiment 
    #       ----------------------------------------------------------
    #        It's..| The..|  In New York ..  | 2020-12-12 | -0.01
    def add_sentiment_analysis(self, df_news):
        pipeline = PretrainedPipeline('analyze_sentiment', lang='en')
        df_result = pipeline.transform(df_news.withColumnRenamed("Text", "text1").withColumn("text", concat_ws(". ", col("Title"), col("text1"), col("ShortDescription"))))
        df_result_with_id = df_result \
                            .withColumn("id", monotonically_increasing_id()) \
                            .withColumnRenamed("text", "sentiment_text") \
                            .withColumnRenamed("text1", "Text")
        df_result_with_id.cache()
        df_id_sentiment = df_result_with_id \
            .withColumn("sentiments", explode("sentiment")) \
            .selectExpr("id", "sentiments.result result", "sentiments.metadata.confidence confidence") \
            .orderBy(col("confidence").desc()) \
            .groupBy("id") \
            .agg( \
                first("result").alias("result"), \
                first("confidence").alias("confidence"), \
            ) \
            .withColumn("result_number", when(col("result") == "positive", 1) \
                        .when(col("result") == "negative", -1) \
                        .otherwise(0)) \
            .withColumn("sentiment", col("result_number") * col("confidence")) \
            .drop("result", "confidence", "result_number")
        return df_result_with_id.select("id", "Title", "Text", "ShortDescription", "Date").join(df_id_sentiment, df_result_with_id.id == df_id_sentiment.id).drop("id")
    
    # USED IN sentiment_analysis.py
    # Adds columns unix timestamp, time period. 
    def add_time_period(self, df_news):
        return df_news \
            .withColumn("unix", self.UDF.clean_date(col("date"))) \
            .withColumn("time_period", self.UDF.convert_unix_to_date(col("unix")))
            
    # USED IN main.py
    # REPLACES add_stocks_sectors_sentiment()
    # Searches all news articles and returns a dataframe with the matching stocks.
    def search_for_stocks_and_aggregate(self, df_news, df_stocks):
        selection = "Companyname"
        df_stocks = df_stocks.select("symbol", selection).distinct().filter(col(selection) != 'null')
        return df_news \
            .join(df_stocks) \
            .withColumn("CleanedCompanyname", 
                when(F.lower(col(selection)).contains("inc."), F.trim(regexp_replace(col(selection), "Inc.", "")))  \
                .when(F.lower(col(selection)).contains("incorporated"), F.trim(regexp_replace(col(selection), "Incorporated", ""))) \
                .when(F.lower(col(selection)).contains("corp."), F.trim(regexp_replace(col(selection), "Corp.", ""))) \
                .when(F.lower(col(selection)).contains("corporation"), F.trim(regexp_replace(col(selection), "Corporation", ""))) \
                .when(F.lower(col(selection)).contains("ltd."), F.trim(regexp_replace(col(selection), "Ltd.", ""))) \
                .when(F.lower(col(selection)).contains("limited"), F.trim(regexp_replace(col(selection), "Limited", "")))
                .otherwise(col(selection))) \
            .filter((F.lower(col("Title")).contains(F.lower(col("CleanedCompanyname")))) \
                | (F.lower(col("Text")).contains(F.lower(col("CleanedCompanyname")))) \
                | (F.lower(col("ShortDescription")).contains(F.lower(col("CleanedCompanyname"))))) \
            .groupBy("symbol", "time_period") \
            .agg(count("*").alias("number_of_articles"), \
            F.first("Companyname").alias("name"), \
            F.avg("sentiment").alias("sentiment")) \
            .withColumnRenamed("symbol", "stock") \
            .orderBy(col("stock").asc(), col("time_period").desc())
            
    # USED IN main.py
    # REPLACES add_stocks_sectors_sentiment()
    # Collects all rows from each dataframe containing the sectors.
    def search_for_sectors_and_aggregate(self, df_news, df_sectors):
        return self.search_for_sector(df_news, df_sectors, "Information Technology") \
            .union(self.search_for_sector(df_news, df_sectors, "Health Care")) \
            .union(self.search_for_sector(df_news, df_sectors, "Financials")) \
            .union(self.search_for_sector(df_news, df_sectors, "Consumer Discretionary")) \
            .union(self.search_for_sector(df_news, df_sectors, "Communication Services")) \
            .union(self.search_for_sector(df_news, df_sectors, "Industrials")) \
            .union(self.search_for_sector(df_news, df_sectors, "Consumer Staples")) \
            .union(self.search_for_sector(df_news, df_sectors, "Energy")) \
            .union(self.search_for_sector(df_news, df_sectors, "Utilities")) \
            .union(self.search_for_sector(df_news, df_sectors, "Real Estate")) \
            .union(self.search_for_sector(df_news, df_sectors, "Materials")) \
            .orderBy(col("sector").asc(), col("time_period").desc()) 

    # USED IN main.py (indirectly)
    # Searches all news articles and returns a dataframe with the matching sectors.
    def search_for_sector(self, df_news, df_sectors, sector: str):
        return df_news \
            .join(df_sectors.select(sector)) \
            .filter((F.lower(col("Title")).contains(F.lower(col(sector)))) \
                | (F.lower((col("Text"))).contains(F.lower(col(sector)))) \
                | (F.lower((col("ShortDescription"))).contains(F.lower(col(sector))))) \
            .dropDuplicates(["Title", "Text", "ShortDescription"]) \
            .withColumn("sector", F.lit(sector)) \
            .groupBy("sector", "time_period") \
            .agg(count("*") \
            .alias("number_of_articles"), \
            F.avg("sentiment").alias("sentiment")) \

    # USED IN main.py
    # First: Joins the dataframe grouped by sector and time period with the stock price for each sector and time period.
    # Second: Calculates the correlation between the sentiment of the news articles and the stock prices for each time period and sector.
    #   Input:
    #       sector  | time_period | sentiment | ...
    #       ---------------------------------------
    #       Tech    |   2020-02   |   -0.3    | ...
    #       Energy  |   2020-03   |   0.01    | ...
    #
    #   Output:
    #       sector  | time_period | sentiment | stock_price | correlation | ...
    #       -------------------------------------------------------------------
    #       Tech    |   2020-02   |   -0.3    |     0.7     |      0      | ...
    #       Energy  |   2020-03   |   0.01    |     0.4     |      1      | ...
    def add_correlation_sector(self, df_sector, df_stocks_prices_sector):
        df_sps = df_stocks_prices_sector.withColumnRenamed("time_period", "period").select("period", "category", "stock_price_change")
        return df_sector \
            .join(df_sps, (df_sector.time_period == df_sps.period) & (df_sector.sector == df_sps.category), "left") \
            .withColumn("correlation", when((col("sentiment") > 0.0) & (col("stock_price_change") > 0.0), 1.0) \
                        .when((col("sentiment") < 0.0) & (col("stock_price_change") < 0.0), -1.0) \
                        .otherwise(0.0)) \
            .drop(col("category")) \
            .drop(col("period")) \

    # USED IN main.py
    # Groups by sector and aggregates number of articles and the correlation. 
    #   Input:
    #       sector  | time_period | sentiment | stock_price | correlation | ...
    #       -------------------------------------------------------------------
    #       Tech    |   2020-02   |   -0.3    |     0.7     |      0      | ...
    #       Energy  |   2020-03   |   0.01    |     0.4     |      1      | ...
    #
    #   Output:
    #       sector  | number_of_articles | corr 
    #       --------------------------------
    #       Tech    |      1293      | -0.3    
    #       Energy  |      8392      | 0.01   
    def groupby_sector(self, df):
        return df \
            .groupBy("sector") \
            .agg(F.sum("number_of_articles") \
            .alias("number_of_articles"), \
            F.mean("correlation").alias("corr")) \
            .orderBy(col("sector").asc())
            
    # OLD (not used)
    # REPLACED by search_for_stocks_and_aggregate()
    def explode_stocks_and_aggregate(self, df):
        return df \
            .withColumn("stock", explode("stocks")) \
            .groupBy("stock", "time_period") \
            .agg(count("*") \
            .alias("number_of_articles"), \
            F.avg("sentiment").alias("sentiment")) \
            .orderBy(col("stock").asc(), col("time_period").desc()) 

    # USED IN main.py
    # First: Joins the dataframe grouped by stock and time period with the stock price for each stock and time period.
    # Second: Calculates the correlation between the sentiment of the news articles and the stock prices for each time period and stock.
    #   Input:
    #       stock  | time_period | sentiment | ...
    #       ---------------------------------------
    #       AAPL   |   2020-02   |   -0.3    | ...
    #       NHY    |   2020-03   |   0.01    | ...
    #
    #   Output:
    #       stock  | time_period | sentiment | stock_price | correlation | ...
    #       -------------------------------------------------------------------
    #       AAPL   |   2020-02   |   -0.3    |     0.7     |      0      | ...
    #       NHY    |   2020-03   |   0.01    |     0.4     |      1      | ...
    def add_correlation_stock(self, df_stock, df_stocks_prices_symbol):
        df_sps = df_stocks_prices_symbol.withColumnRenamed("time_period", "period").select("period", "symbol", "stock_price_change")
        return df_stock \
            .join(df_sps, (df_stock.time_period == df_sps.period) & (df_stock.stock == df_sps.symbol), "left") \
            .withColumn("correlation", when((col("sentiment") > 0.0) & (col("stock_price_change") > 0.0), 1.0) \
                        .when((col("sentiment") < 0.0) & (col("stock_price_change") < 0.0), -1.0) \
                        .otherwise(0.0)) \
            .drop(col("category")) \
            .drop(col("period")) \
            .drop(col("symbol"))

    # USED IN main.py
    # Groups by stock and aggregates number of articles and the correlation. 
    #   Input:
    #       stock  | time_period | sentiment | stock_price | correlation | ...
    #       -------------------------------------------------------------------
    #       AAPL   |   2020-02   |   -0.3    |     0.7     |      0      | ...
    #       NHY    |   2020-03   |   0.01    |     0.4     |      1      | ...
    #
    #   Output:
    #       stock  | number_of_articles | corr 
    #       --------------------------------
    #       AAPL   |      1293      | -0.3    
    #       NHY    |      8392      | 0.01   
    def groupby_stock(self, df):
        return df \
            .groupBy("stock") \
            .agg(F.first("name").alias("name"), \
            F.sum("number_of_articles").alias("number_of_articles"), \
            F.mean("correlation").alias("corr")) \
            .orderBy(col("stock").asc()) 
            
    # USED IN main.py
    # Groups all stock prices by symbol name and time period. Uses the close price to determine 
    # how much the price has changed in % in the specified time period.
    #   Input:
    #       Timestamp  | Stockname | Close | ... 
    #       -------------------------------------------------------------------
    #       129488..   |    AAPL   |  3001 | ...
    #       123849..   |    NHY    |  123  | ...
    #
    #   Output:
    #       time_period |   symbol  | stock_prices 
    #       --------------------------------------
    #       2022-10     |    AAPL   |    -0.3    
    #       2023-02     |    NHY    |    0.01   
    def calculate_stock_price_change(self, df_stocks_with_companynames):
        return df_stocks_with_companynames \
            .filter(col("Close") > 0.0) \
            .withColumn("time_period", self.UDF.convert_unix_to_date(col("Timestamp"))) \
            .orderBy(col("symbol").asc(), col("time_period").desc()) \
            .groupBy("time_period", "symbol", "category") \
            .agg(self.UDF.first_date(F.collect_list("close"),F.collect_list("timestamp")).alias("first_date"), \
            self.UDF.first_value(F.collect_list("close")).alias("first_value"), \
            self.UDF.last_date(F.collect_list("close"),F.collect_list("timestamp")).alias("last_date"), \
            self.UDF.last_value(F.collect_list("close")).alias("last_value"), \
            F.first("Companyname").alias("Companyname")) \
            .filter(col("first_date") != "null") \
            .filter(col("last_date") != "null") \
            .withColumn("stock_price_change", (col("last_value") - col("first_value")) / col("first_value")) \
            .orderBy(col("symbol").asc(), col("time_period").desc())
            
    # USED IN main.py
    # Joins the stocks dataframe with the companyname dataframe to get the stock name and sector.
    #   Input:
    #       timestamp   |   symbol  | stock_prices 
    #       --------------------------------------
    #       2022-10     |    AAPL   |    2451    
    #       2023-02     |    NHY    |    0.01   
    #
    #   Output:
    #       timestamp   |   symbol  | stock_prices | company_name |  category
    #       ------------------------------------------------------------------
    #       2022-10     |    AAPL   |     -0.3     |   Apple Inc  | Technology
    #       2023-02     |    NHY    |     0.01     |  Norsk Hydro | Materials
    def join_with_companynames(self, df_stocks, df_companynames):
        df_company_names = df_companynames.dropDuplicates(["Stockname"])
        df_stocks = df_stocks.withColumnRenamed("Stockname", "Symbol")
        # ADD BROADCAST TO JOIN:
        #return df_stocks \
        #    .select("Timestamp", "Symbol", "Close") \
        #    .distinct() \
        #    .join(broadcast(df_company_names), df_company_names.Stockname == df_stocks.Symbol, "left") \
        #    .drop(col("Stockname"))
        return df_stocks \
            .select("Timestamp", "Symbol", "Close") \
            .distinct() \
            .join(df_company_names, df_company_names.Stockname == df_stocks.Symbol, "left") \
            .drop(col("Stockname"))
                
    # USED IN main.py
    # Removes existing news articles from the new incoming articles. Ensures no duplicates are being processed.
    def compare_and_return_unique(self, delta_news, df_news):
        delta_news_df = delta_news.toDF()
        return df_news \
            .subtract(delta_news_df) 