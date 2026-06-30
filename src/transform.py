import os
import json 
from datetime import date, timedelta
from requests import get
from dotenv import load_dotenv
from pyspark.sql import functions as F  
from conection import configure_spark
from time import sleep
#os.environ["HADOOP_HOME"] = r"C:\Hadoop"
#os.environ["PATH"] += os.pathsep + r"C:\Hadoop\bin"


def silver_layer(spark):
    df_ohlc = spark.read.format("delta").load("data/bronze/raw_ohlc")
    df_ohlc = df_ohlc.select("results")
    df_ohlc = df_ohlc.withColumn("item", F.explode("results"))
    df_ohlc = df_ohlc.select("item")

    df_ohlc = df_ohlc.select(
    F.col("item.ticker").alias("ticker"),
    F.col("item.v").alias("volume"),
    F.col("item.vw").alias("volume_weighted"),
    F.col("item.o").alias("open_price"),
    F.col("item.c").alias("close_price"),
    F.col("item.h").alias("highest_price"),
    F.col("item.l").alias("lowest_price"),
    F.col("item.t").alias("timestamp"),
    F.col("item.n").alias("transactions")
    )

    df_cs = spark.read.format("delta").load("data/bronze/raw_cs")
    df_cs = df_cs.select("results")
    df_cs = df_cs.withColumn("item", F.explode("results"))
    df_cs = df_cs.select("item.*")
    df_cs = df_cs.select("name", "primary_exchange", "ticker", "type")

    df_adrc = spark.read.format("delta").load("data/bronze/raw_adrc")
    df_adrc = df_adrc.select("results")
    df_adrc = df_adrc.withColumn("item", F.explode("results"))
    df_adrc = df_adrc.select("item.*")
    df_adrc = df_adrc.select("name", "primary_exchange", "ticker", "type")

    df_securities = df_cs.union(df_adrc)
    df_market_security = df_ohlc.join(df_securities, on="ticker", how="inner")

    df_nasdaq = spark.read.format("delta").load("data/bronze/raw_nasdaq_screener")
    df_nasdaq = df_nasdaq.select("data.rows")
    df_nasdaq = df_nasdaq.withColumn("item", F.explode("rows"))
    df_nasdaq = df_nasdaq.select("item.*")
    df_nasdaq = df_nasdaq.select("country", "industry", "ipoyear", "sector", "symbol", "marketCap")

    df_integrated = df_market_security.join(df_nasdaq, df_market_security["ticker"] == df_nasdaq["symbol"], how="inner")

    columns = ["country", "industry", "sector"]
    for column in columns:
        df_integrated = df_integrated.withColumn(
        column,
        F.when(F.col(column) == "", F.lit("Unknown")).otherwise(F.col(column))
        )

    columns = ["ipoyear", "marketCap"]
    for column in columns:
        df_integrated = df_integrated.withColumn(
        column,
        F.when(F.col(column) != "", F.col(column))
        )

    df_integrated = df_integrated.withColumn(
    "marketCap",
    F.when(F.col("marketCap") != 0, F.col("marketCap"))
    )
    allowed_values = ["XNYS", "XNAS", "XASE"]
    df_integrated = df_integrated.filter(F.col("primary_exchange").isin(allowed_values))

    df_market_data = df_integrated.select("ticker","volume", "volume_weighted", "transactions", "open_price", "close_price", "highest_price", "lowest_price", "timestamp", "marketCap")
    df_market_data = df_market_data.withColumn("date", F.to_date(F.from_unixtime(F.col("timestamp")/1000)))

    df_market_data = df_market_data.withColumn("daily_return", F.round(F.col("close_price") - F.col("open_price"), 2)) \
                    .withColumn("daily_return_pct", F.round(100 * (F.col("close_price")-F.col("open_price"))/(F.col("open_price")), 2)) \
                    .withColumn("price_range", F.round(F.col("highest_price") - F.col("lowest_price"), 2)) \
                    .withColumn("volume_per_transactions", F.round(F.col("volume")/F.col("transactions"), 2)) \
                    .withColumn("financial_volume", F.round(F.col("volume") * F.col("volume_weighted"), 2)) \
                    .withColumn("is_positive_day", F.col("close_price") >= F.col("open_price"))
    
    df_security = df_integrated.select("ticker", "name", "primary_exchange", "type", "country", "ipoyear", "industry", "sector")
    df_security = df_security.withColumn("industry_id", F.md5(F.col("industry")))
    df_security = df_security.withColumn("sector_id", F.md5(F.col("sector")))

    #df_industry_sector = df_security.select("industry_id", "sector_id")
    #df_industry_sector = df_industry_sector.dropDuplicates()

    df_sector = df_security.select("sector_id", "sector")
    df_sector = df_sector.dropDuplicates()

    df_industry = df_security.select("industry_id", "industry")
    df_industry = df_industry.drop_duplicates()

    df_security = df_security.select("ticker", "name", "primary_exchange", "type", "country", "ipoyear", "industry_id", "sector_id")
    #allowed_values = ["XNYS", "XNAS", "XASE"]
    #df_security = df_security.filter(F.col("primary_exchange").isin(allowed_values))

    df_security = df_security.withColumn("ipoyear", F.col("ipoyear").cast("int"))
    mapping = {"name": "security_name", "type": "security_type", "ipoyear": "ipo_year"}
    df_security = df_security.withColumnsRenamed(mapping)

    df_market_data.write.format("delta").mode("overwrite").save("data/silver/market_data")
    df_security.write.format("delta").mode("overwrite").save("data/silver/security")
    df_industry.write.format("delta").mode("overwrite").save("data/silver/industry")
    df_sector.write.format("delta").mode("overwrite").save("data/silver/sector")
    #df_industry_sector.write.format("delta").mode("overwrite").save("data/silver/industry_sector")

def gold_layer(spark):
    market_data  = spark.read.format("delta").load("data/silver/market_data")
    security = spark.read.format("delta").load("data/silver/security")
    industry = spark.read.format("delta").load("data/silver/industry")
    sector = spark.read.format("delta").load("data/silver/sector")

    fact_market_data = market_data
    fact_market_data = fact_market_data.withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))
    fact_market_data = fact_market_data.drop("date", "timestamp")
    
    dim_date = market_data
    dim_date = dim_date.select("date")
    dim_date = dim_date.withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

    dim_date = dim_date.withColumn("year", F.year(F.col("date")))
    dim_date = dim_date.withColumn("quarter", F.quarter(F.col("date")))

    dim_date = dim_date.withColumn("month", F.month(F.col("date")))
    dim_date = dim_date.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))

    dim_date = dim_date.withColumn("day_of_month", F.dayofmonth(F.col("date")))
    dim_date = dim_date.withColumn("day_of_week", F.dayofweek(F.col("date")))
    dim_date = dim_date.withColumn("day_name", F.date_format(F.col("date"), "EEEE"))

    dim_date = dim_date.drop_duplicates()
    
    dim_security = security.join(industry, on="industry_id", how="left").join(sector, on="sector_id", how="left")
    dim_security = dim_security.drop("sector_id", "industry_id")

    fact_market_data.write.format("delta").mode("overwrite").save("data/gold/fact_market_data")
    dim_date.write.format("delta").mode("overwrite").save("data/gold/dim_date")
    dim_security.write.format("delta").mode("overwrite").save("data/gold/dim_security")    

if __name__ == "__main__":
    spark = configure_spark()
    gold_layer(spark)
