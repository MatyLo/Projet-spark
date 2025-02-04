from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    # Récupération d'arguments
    arg1 = sys.argv[1] if len(sys.argv) > 1 else None
    arg2 = sys.argv[2] if len(sys.argv) > 2 else None

    spark = SparkSession.builder.appName("MyLocalSparkJob").getOrCreate()

    print(f"Argument 1: {arg1}")
    print(f"Argument 2: {arg2}")
    # TODO: placez ici votre transformation Spark

    spark.stop()