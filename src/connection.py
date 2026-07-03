from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def configure_spark():
    
    builder = SparkSession.builder.appName("my_app") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark

if __name__ == "__main__":
    configure_spark()    