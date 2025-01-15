import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from time import perf_counter, time
from datetime import datetime

def main():
    spark = SparkSession.builder.appName("udf python").master("local[*]").getOrCreate()

    start_read = time()
    df = spark.read.option("header", True).csv("src/resources/exo4/sell.csv")
    read_time = time() - start_read
    print(f"Temps de lecture des données : {read_time:.2f} secondes")

    def addCategoryNameCol(col):
        if col<6:
            return "food"
        else:
            return "furniture"

    addCategoryNameCol_udf = f.udf(addCategoryNameCol, StringType())

    df = df.withColumn("category", df["category"].cast(IntegerType()))
   
    #Méthode time
    """start_transform_udf = time()
    df = df.withColumn("category_name", addCategoryNameCol_udf(df["category"]))
    df.show()
    #df.write.parquet("data/exo4/python_udf_category_name.csv", mode="overwrite")
    transform_udf_time = time() - start_transform_udf
    print(f"Temps de transformation (avec UDF Python) : {transform_udf_time:.2f} secondes")"""

    #Méthode perf-counter
    debut_perf_counter = perf_counter()
    df = df.withColumn("category_name", addCategoryNameCol_udf(df["category"]))
    df.show()
    #df.write.parquet("data/exo4/python_udf_category_name.csv", mode="overwrite")
    transform_udf_time = perf_counter() - debut_perf_counter
    print(f"Temps de transformation (avec UDF Python) : {transform_udf_time:.2f} secondes")


    total_time_udf = read_time + transform_udf_time
    print(f"Temps total (avec UDF Python) : {total_time_udf:.2f} secondes")


    spark.stop()
