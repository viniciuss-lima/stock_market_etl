import os
import json 
from datetime import date, timedelta
from requests import get
from dotenv import load_dotenv
from pyspark.sql import functions as F  
from conection import configure_spark
from time import sleep

load_dotenv()
api_key = os.getenv("API_KEY")

def get_daily_market_summary():
    #today = date.today()
    #days_ago = today - timedelta(days=1)
    days_ago = "2026-06-26"
    response = get(f"https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{days_ago}?adjusted=true&apiKey={api_key}")
    data = response.json()
    if data['status'] == 'OK' and data['queryCount'] != 0:
        with open('data/staging/ohlc.json', 'w', encoding='utf-8') as file:
            file.write(json.dumps(data, indent=4)) 

def get_common_stocks():
    next_url = "https://api.massive.com/v3/reference/tickers?type=CS&market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey=Tptcil65HEpUDo3wCAHPhqaEYDEHMKe7"
    list = []

    while next_url:
        response = get(next_url)
        data = response.json()
        next_url = data.get("next_url")

        if(next_url):
            next_url = f"{next_url}&apiKey={api_key}"

        list.append(data)
        sleep(20)

    with open("data/staging/cs.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(list, indent=4))

def get_american_depositary_receipt_common():
    next_url = "https://api.massive.com/v3/reference/tickers?type=ADRC&market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey=Tptcil65HEpUDo3wCAHPhqaEYDEHMKe7"
    list = []

    while next_url:
        response = get(next_url)
        data = response.json()
        next_url = data.get("next_url")

        if(next_url):
            next_url = f"{next_url}&apiKey={api_key}"

        list.append(data)
        sleep(20)

    with open("data/staging/adrc.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(list, indent=4))

def get_stock_screener():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
        "Referer": "https://www.nasdaq.com/",
        "Accept": "application/json, text/plain, */*"
    }
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=false&limit=25&download=true"

    response = get(url, headers=headers)
    data = response.json()

    with open("data/staging/nasdaq_screener.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(data, indent=4))

def ohlc_to_bronze(spark):   
    df = spark.read.option("multiline", True).json("data/staging/ohlc.json")
    
    df = df.withColumn("results", F.expr(
        "transform(results, x -> struct(x.T as ticker, x.v as v, x.vw as vw, x.o as o, x.c as c, x.h as h, x.l as l, x.t as t, x.n as n))"
    ))
    
    df.write.format("delta").mode("overwrite").save("data/bronze/raw_ohlc")

def cs_to_bronze(spark):
    df = spark.read.option("multiline", True).json("data/staging/cs.json")
    df.write.format("delta").mode("overwrite").save("data/bronze/raw_cs")

def adrc_to_bronze(spark):
    df = spark.read.option("multiline", True).json("data/staging/adrc.json")
    df.write.format("delta").mode("overwrite").save("data/bronze/raw_adrc")

def nasdaq_screener_to_bronze(spark):
    df = spark.read.option("multiline", True).json("data/staging/nasdaq_screener.json")
    df.write.format("delta").mode("overwrite").save("data/bronze/raw_nasdaq_screener")

def bronze_layer():
    spark = configure_spark()
    
    ohlc_to_bronze(spark)
    cs_to_bronze(spark)
    adrc_to_bronze(spark)
    nasdaq_screener_to_bronze(spark)

if __name__ == "__main__":
   result = get_daily_market_summary()
   get_common_stocks()
   get_american_depositary_receipt_common()
   get_stock_screener()

   bronze_layer()