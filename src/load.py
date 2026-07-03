from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
import os

def create_tables(conn):
    with open('src/create_tables.sql', 'r') as file:
        sql_file = file.read()
    
    conn.execute(text(sql_file))

def insert_data(conn, spark):
    df_fact_market_data = spark.read.format("delta").load("data/gold/fact_market_data")
    df_dim_date = spark.read.format("delta").load("data/gold/dim_date")
    df_dim_security = spark.read.format("delta").load('data/gold/dim_security')

    df_dim_date.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/market_data") \
    .option("dbtable", "stg_dim_date") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    conn.execute(text("""
    INSERT INTO dim_date (date_key, date, year, quarter, month, month_name, day_of_month, day_of_week, day_name)
    SELECT date_key, date, year, quarter, month, month_name, day_of_month, day_of_week, day_name FROM stg_dim_date
    ON CONFLICT (date_key) DO NOTHING;
    """))

    conn.execute(text("DROP TABLE IF EXISTS stg_dim_date"))

    df_dim_security.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/market_data") \
    .option("dbtable", "stg_dim_security") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    conn.execute(text("""
    INSERT INTO dim_security (ticker, security_name, primary_exchange, security_type, country, ipo_year, industry, sector)
    SELECT ticker, security_name, primary_exchange, security_type, country, ipo_year, industry, sector FROM stg_dim_security
    ON CONFLICT (ticker) DO NOTHING;
    """))

    conn.execute(text("DROP TABLE IF EXISTS stg_dim_security"))

    df_fact_market_data.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/market_data") \
    .option("dbtable", "stg_fact_market_data") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    conn.execute(text("""
    INSERT INTO fact_market_data (ticker, date_key, volume, volume_weighted, transactions, open_price, close_price, highest_price, lowest_price, market_cap, daily_return, daily_return_pct, price_range, volume_per_transactions, financial_volume, is_positive_day)
    SELECT ticker, date_key, volume, volume_weighted, transactions, open_price, close_price, highest_price, lowest_price, market_cap, daily_return, daily_return_pct, price_range, volume_per_transactions, financial_volume, is_positive_day FROM stg_fact_market_data
    ON CONFLICT (ticker, date_key) DO NOTHING;
    """))

    conn.execute(text("DROP TABLE IF EXISTS stg_fact_market_data"))
    
def load(spark):
    
    load_dotenv()

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    with engine.begin() as conn:
        create_tables(conn)
        insert_data(conn, spark=spark)

if __name__ == "__main__":
   from connection import configure_spark

   spark = configure_spark()
   load(spark)
    