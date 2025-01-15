import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("clean") \
        .master("local[*]") \
        .getOrCreate()
    city_zipcode = spark.read.option("header", True).csv("src/resources/exo2/city_zipcode.csv")
    clients_bdd = spark.read.option("header", True).csv("src/resources/exo2/clients_bdd.csv")
    df_majeurs = clients_majeurs(clients_bdd)
    df_ville_clients_majeurs = nom_ville(df_majeurs,city_zipcode)
    df_ville_clients_majeurs_dep = departement(df_ville_clients_majeurs)
    df_ville_clients_majeurs_dep.write.mode("overwrite").parquet("data/exo2/clean")

def clients_majeurs(df):
    return df.where(f.col("age") >= 18)

def nom_ville(df1,df2):
    return df1.join(df2, 'zip', "inner").select("name","age","zip","city")

def departement(df):
    return df.withColumn("departement", f.when(f.col("zip").cast("int").between(20000,20190),"2A").when(f.col("zip").cast("int").between(20191,21000),"2B").otherwise(f.substring('zip', 1,2)))