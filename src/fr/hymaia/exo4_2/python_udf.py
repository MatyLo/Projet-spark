from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Fonction Python corrigée
def get_category_name(category):
    try:
        category = int(category)  # Conversion en entier
        return "food" if category < 6 else "furniture"
    except ValueError:
        return None

# Enregistrer la fonction comme UDF
category_name_udf = udf(get_category_name, StringType())

def main():
    # Initialiser Spark
    spark = SparkSession.builder.appName("UDF Example").getOrCreate()
    
    # Charger le DataFrame à partir du fichier CSV
    sell_df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    
    # Convertir la colonne 'category' en entier si ce n'est pas déjà le cas
    sell_df = sell_df.withColumn("category", sell_df["category"].cast("int"))
    
    # Ajouter une colonne en utilisant l'UDF
    sell_with_category_df = sell_df.withColumn("category_name", category_name_udf(sell_df["category"]))
    
    # Afficher le résultat
    sell_with_category_df.show()
    
    spark.stop()

if __name__ == "__main__":
    main()
