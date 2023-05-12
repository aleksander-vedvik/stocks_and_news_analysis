from pyspark.sql.functions import udf
from pyspark.sql.types import *
from datetime import datetime

class UDF:
    def __init__(self):
        self.clean = udf(UDF.clean_udf, returnType=ArrayType(StringType(), False))
        self.find_stocks = udf(UDF.find_stocks_udf, returnType=ArrayType(StringType(), False))
        self.find_sectors = udf(UDF.find_sectors_udf, returnType=ArrayType(StringType(), False))
        self.clean_date = udf(UDF.clean_date_udf, returnType=FloatType())
        self.convert_unix_to_date = udf(UDF.convert_from_unix_to_month_udf, returnType=StringType())
        self.sentiment_analysis = udf(UDF.sentiment_analysis_udf, returnType=FloatType())
        self.correlation = udf(UDF.correlation_udf, returnType=FloatType())
        self.calculate_change = udf(UDF.calculate_change_udf, returnType=StringType())
        self.first_date = udf(UDF.first_date_udf, returnType=StringType())
        self.first_value = udf(UDF.first_value_udf, returnType=StringType())
        self.last_date = udf(UDF.last_date_udf, returnType=StringType())
        self.last_value = udf(UDF.last_value_udf, returnType=StringType())
        
    # OLD (not used)
    # We changed how the matching works, so this is obsolete.
    @staticmethod
    def clean_udf(title, text, short_description):
        if title is None and text is None and short_description is None:
            return None
        cleaned_text = []
        if title is not None:
            splitted_title = title.split(" ")
            for word in splitted_title:
                clean_word = []
                for letter in word:
                    if 65 <= ord(letter) <= 90 or 97 <= ord(letter) <= 122 or 48 <= ord(letter) <= 57:
                        clean_word.append(letter.lower())   
                cleaned_text.append("".join(clean_word))
        if text is not None:
            splitted_text = text.split(" ")
            for word in splitted_text:
                clean_word = []
                for letter in word:
                    if 65 <= ord(letter) <= 90 or 97 <= ord(letter) <= 122 or 48 <= ord(letter) <= 57:
                        clean_word.append(letter.lower())   
                cleaned_text.append("".join(clean_word))
        if short_description is not None:
            splitted_short_description = short_description.split(" ")
            for word in splitted_short_description:
                clean_word = []
                for letter in word:
                    if 65 <= ord(letter) <= 90 or 97 <= ord(letter) <= 122 or 48 <= ord(letter) <= 57:
                        clean_word.append(letter.lower())   
                cleaned_text.append("".join(clean_word))
        return cleaned_text
    
    # OLD (not used)
    # This udf is replaced by using transforms instead. See the method add_stocks_sectors_sentiment()
    # in transformations.py to see what it is replaced with.
    @staticmethod
    def find_stocks_udf(text, stocks_list):
        if text is None or stocks_list is None:
            return []
        stocks = set()
        for stock in stocks_list:
            if stock is None:
                continue
            for word in text:
                if stock.lower() in word:
                    stocks.add(stock)
                    break
        return list(stocks)

    # OLD (not used)
    # This udf is replaced by using transforms instead. See the method add_stocks_sectors_sentiment()
    # in transformations.py to see what it is replaced with.
    @staticmethod
    def find_sectors_udf(text, sectors_list):
        if text is None or sectors_list is None:
            return []
        sectors = set()
        for row in sectors_list:
            for i, key_word in enumerate(row):
                if key_word is None:
                    continue
                keys = key_word.lower().split(" ")
                in_text = True
                for key in keys:
                    in_word = False
                    for word in text:
                        if key in word:
                            in_word = True
                            break
                    if not in_word:
                        in_text = False
                        break
                if in_text:
                    sectors.add(sectors_list[0][i])
        return list(sectors)

    # OLD (never used because it was a test function)
    @staticmethod
    def sentiment_analysis_udf(text):
        if text is None:
            return None
        return 0.5

    # Used in add_time_period() in transformations.py
    @staticmethod
    def clean_date_udf(date):
        if date is None:
            return None
        try:
            d = datetime.strptime(date[0:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            try:
                d = datetime.strptime(date[0:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                try:
                    d = datetime.strptime(date[0:10], "%Y-%m-%d")
                except Exception:
                    return 0
        return datetime.timestamp(d)

    # Used in add_time_period() and calculate_stock_price_change() in transformations.py
    @staticmethod
    def convert_from_unix_to_month_udf(unix):
        if unix is None:
            return None
        return datetime.utcfromtimestamp(unix).strftime('%Y-%m')

    # OLD (not used)
    # This udf is replaced by using transforms instead. See the method add_correlation_sector() and add_correlation_stock()
    # in transformations.py to see what it is replaced with.
    @staticmethod
    def correlation_udf(sentiment, stock_price):
        if sentiment is None or stock_price is None:
            return None
        if float(sentiment) * float(stock_price) > 0.0:
            return 1.0
        return 0.0

    # OLD (not used)
    # Test function
    @staticmethod
    def calculate_change_udf(prices):
        if prices is None:
            return None
        if len(prices) <= 1:
            return 0
        start = 0
        for i, price in enumerate(prices):
            if price != 0:
               start = i 
               break
        end = -1
        for i in range(len(prices)-1, 0, -1):
            if prices[i] != 0:
               end = i 
               break
        if prices[start] == 0 and prices[end] == 0:
            return 0
        return (prices[end]-prices[start])/prices[start]

    # Used in calculate_stock_price_change() in transformations.py
    @staticmethod
    def first_date_udf(prices, timestamps):
        if prices is None or timestamps is None:
            return None
        if len(prices) <= 1 or len(timestamps) <= 1:
            return None
        for i, price in enumerate(prices):
            if price != 0:
               return timestamps[i]
        return timestamps[0]
    
    # Used in calculate_stock_price_change() in transformations.py
    @staticmethod
    def first_value_udf(prices):
        if prices is None:
            return None
        if len(prices) <= 1:
            return 0
        for price in prices:
            if price != 0:
                return price
        return 0

    # Used in calculate_stock_price_change() in transformations.py
    @staticmethod
    def last_date_udf(prices, timestamps):
        if prices is None or timestamps is None:
            return None
        if len(prices) <= 1 or len(timestamps) <= 1:
            return None
        for i in range(len(prices)-1, 0, -1):
            if prices[i] != 0:
               return timestamps[i]
        return timestamps[-1]
    
    # Used in calculate_stock_price_change() in transformations.py
    @staticmethod
    def last_value_udf(prices):
        if prices is None:
            return None
        if len(prices) <= 1:
            return 0
        for i in range(len(prices)-1, 0, -1):
            if prices[i] != 0:
                return prices[i]
        return 0