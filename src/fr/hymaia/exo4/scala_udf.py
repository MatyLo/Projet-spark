from logging import config
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.types import StringType, IntegerType
from time import perf_counter, time
from datetime import datetime

def main():
    spark = SparkSession.builder.appName("exo4").master("local[*]").config('spark.jars', 'src/resources/exo4/udf.jar').getOrCreate()

    start_read = time()
    df = spark.read.option("header", True).csv("src/resources/exo4/sell.csv")
    read_time = time() - start_read
    print(f"Temps de lecture des données : {read_time:.2f} secondes")


    def addCategoryName(col):
        # on récupère le SparkContext
        sc = spark.sparkContext
        # Via sc._jvm on peut accéder à des fonctions Scala
        add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
        # On retourne un objet colonne avec l'application de notre udf Scala
        return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

    #Méthode time
    start_transform_udf = time()
    df_scalaUDF = df.withColumn('category_name', addCategoryName(df["category"]))
    #df.show()
    df_scalaUDF.write.parquet("data/exo4/scala_udf_category_name_time.csv", mode="overwrite")
    transform_udf_scala_time = time() - start_transform_udf
    print(f"Temps de transformation(time) (avec UDF scala) : {transform_udf_scala_time:.2f} secondes")

    total_time_udf_scala = read_time + transform_udf_scala_time
    print(f"Temps total avec time (avec UDF scala) : {total_time_udf_scala:.2f} secondes")

    #Méthode perf_counter
    """debut_perf_counter2 = perf_counter()
    df = df.withColumn('category_name', addCategoryName(df["category"]))
    df.show()
    #df.write.parquet("data/exo4/scala_udf_category_name_perfcounter.csv", mode="overwrite")
    transform_udf_scala_perfcounter = perf_counter() - debut_perf_counter2
    print(f"Temps de transformation (perfcounter) (avec UDF scala) : {transform_udf_scala_perfcounter:.2f} secondes")

    total_perfcounter_udf_scala = read_time + transform_udf_scala_perfcounter
    print(f"Temps total avec perfcounter (avec UDF scala) : {total_perfcounter_udf_scala:.2f} secondes")"""


    spark.stop()
