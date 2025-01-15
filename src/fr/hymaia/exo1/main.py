import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()
    df = spark.read.option("header", True).csv("src/resources/exo1/data.csv")
    df_wordcount = wordcount(df, "text")
    df_wordcount.write.mode("overwrite").partitionBy("count").parquet("data/exo1/output")
    #.mode("overwrite") est pour écraser la donnée. Sinon si on lance 2 fois la fonction il va nous dire que c'est déjà créer
    #partitionBy est pour faire un regroupement
    #print("Hello world!")


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
