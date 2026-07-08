from src.extract import extract
from src.transform import transform
from src.load import load
from src.connection import configure_spark
from src.analysis import analysis

def main():
    spark = configure_spark()

    extract(spark)
    transform(spark)
    load(spark)
    analysis(spark)
    
if __name__ == "__main__":
    main()