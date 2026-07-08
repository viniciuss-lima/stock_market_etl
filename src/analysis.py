import pandas
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os 

load_dotenv()
    
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

def query_1(spark):

    query = """
    SELECT
        s.sector,
        AVG(f.daily_return_pct) AS avg_return
    FROM fact_market_data f
    JOIN dim_security s
        ON f.ticker = s.ticker
    GROUP BY s.sector
    ORDER BY avg_return DESC
    """

    df = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}") \
    .option("query", query) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

    pdf = df.toPandas()
    pdf['avg_return'] = pdf['avg_return'].astype('double')
    pdf = pdf.sort_values("avg_return", ascending=True)

    plt.figure(figsize=(12,6))

    colors = ['green' if x >= 0 else 'red' for x in pdf["avg_return"]]
    plt.barh(pdf["sector"], pdf["avg_return"], color=colors)

    plt.xlabel("Average Daily Return (%)")
    plt.ylabel("Sector")
    plt.title("Average Daily Return by Sector", fontweight='bold')

    plt.tight_layout()
    plt.axvline(0, color="black", linewidth=1)
    plt.grid(axis='x', linestyle='--', color='grey')
   
    plt.savefig('images/sector_performance_analysis.png')

def query_2(spark):
    query = """
    (
    SELECT
        f.ticker,
        f.daily_return_pct
    FROM fact_market_data f
    JOIN dim_date d
        ON f.date_key = d.date_key
    WHERE d.date = (SELECT MAX(date) FROM dim_date)
    ORDER BY f.daily_return_pct DESC
    LIMIT 10
    )
    UNION ALL
    (
    SELECT
        f.ticker,
        f.daily_return_pct
    FROM fact_market_data f
    JOIN dim_date d
        ON f.date_key = d.date_key
    WHERE d.date = (SELECT MAX(date) FROM dim_date)
    ORDER BY f.daily_return_pct ASC
    LIMIT 10
    )
    """

    df = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}") \
    .option("query", query) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

    pdf = df.toPandas()
    pdf['daily_return_pct'] = pdf['daily_return_pct'].astype('double')
    pdf = pdf.sort_values("daily_return_pct", ascending=True)

    colors = ['green' if x > 0 else 'red' for x in pdf['daily_return_pct']]

    plt.figure(figsize=(12,6))
    plt.barh(pdf['ticker'], pdf['daily_return_pct'], color=colors)
    plt.title("Top Daily Gainers and Losers", fontweight='bold')
    plt.xlabel("Daily Return (%)")
    plt.ylabel("Ticker")
    plt.tight_layout()
    plt.axvline(x=0, color='black', linestyle='-') 
    plt.grid(axis='x', linestyle='--', color='grey')
    for i, v in enumerate(pdf["daily_return_pct"]):
        plt.text(v + (0.5 if v > 0 else -5.5), i, f"{v:.1f}%", va='center')
    plt.savefig('images/daily_gainers_and_losers')

def analysis(spark):
    query_1(spark=spark)
    query_2(spark=spark)

if __name__ == "__main__":
    from connection import configure_spark

    spark = configure_spark()
    analysis(spark)