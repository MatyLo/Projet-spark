import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("clean") \
        .master("local[*]") \
        .getOrCreate()
    data = spark.read.option("header", True).parquet("data/exo2/clean/")
    data_sort = calcul_dep_pop(data)
    #data_sort.show()
    data_sort.write.mode("overwrite").csv("data/exo2/aggregate")

def calcul_dep_pop(df):
    return df.groupBy(f.col("departement")).count().sort(f.desc("count"))