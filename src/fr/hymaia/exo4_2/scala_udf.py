from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.functions import col

def addCategoryName(spark_session, col):
    sc = spark_session.sparkContext
    # Récupération de la fonction Scala à partir du SparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

def main():
    # Initialiser la SparkSession avec le JAR Scala
    spark_session = SparkSession.builder \
        .appName("Scala_UDF") \
        .config("spark.jars", "src/resources/exo4/udf.jar") \
        .getOrCreate()
    
    # Charger le fichier CSV
    sell_path = "src/resources/exo4/sell.csv"
    sell_df = spark_session.read.option("header", True).csv(sell_path)
    
    # Utiliser l'UDF Scala pour ajouter la colonne `category_name`
    sell_with_category_df = sell_df.withColumn("category_name", addCategoryName(spark_session, col("category")))
    sell_with_category_df.show()
    
    # Fermer la session Spark
    spark_session.stop()

if __name__ == "__main__":
    main()
