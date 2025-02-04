import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from time import perf_counter, time
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import sum


def main():
    spark = SparkSession.builder.appName("no udf").master("local[*]").getOrCreate()
    
    start_read = time()
    df = spark.read.option("header", True).csv("src/resources/exo4/sell.csv")
    read_time = time() - start_read
    print(f"Temps de lecture des données : {read_time:.2f} secondes")
    
    #Méthode time
    start_transform_no_udf = time()
    df_noUDF = df.withColumn("category_name", f.when(f.col("category").cast("int")<6,"food").otherwise("furniture"))
    df.show()
    #df_noUDF.write.parquet("data/exo4/no_udf_category_name_time.csv", mode="overwrite")
    transform_no_udf_time = time() - start_transform_no_udf
    print(f"Temps de transformation (time) (sans UDF) : {transform_no_udf_time:.2f} secondes")

    total_time_no_udf = read_time + transform_no_udf_time
    print(f"Temps total avec time (sans UDF) : {total_time_no_udf:.2f} secondes")

    #Méthode perf_counter
    debut_perf_counter1 = perf_counter()
    df = df.withColumn("category_name", f.when(f.col("category").cast("int")<6,"food").otherwise("furniture"))
    df.show()
    #df.write.parquet("data/exo4/no_udf_category_name_perfcounter.csv", mode="overwrite")
    transform_no_udf_perfcounter = perf_counter() - debut_perf_counter1
    print(f"Temps de transformation (perfcounter) (sans UDF) : {transform_no_udf_perfcounter:.2f} secondes")
    
    total_perfcounter_no_udf = read_time + transform_no_udf_perfcounter
    print(f"Temps total avec perfcounter (sans UDF) : {total_perfcounter_no_udf:.2f} secondes")
    

    window_somme_prix = Window.partitionBy("date","category")
    df_w1 = df.withColumn("total_price_per_category_per_day",sum("price").over(window_somme_prix))
    df_w1.show()

    window_somme_prix_30j = Window.partitionBy("category").orderBy("date").rowsBetween(-30, 0)
    df_w2 = df.withColumn("total_price_per_category_per_day_last_30_days",sum("price").over(window_somme_prix_30j))
    df_w2.show()

    spark.stop()