import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from time import time, perf_counter
from datetime import datetime
import timeit, functools

show_dataframes_at_end_of_processing = True
count_dataframes = False


########### DEFINITION FONCTIONS POUR LA UDF
# Fonction pour returner food or furniture dependant dans le valeur de la colonne
def checkFoodOrFurniture(col):
    if col < 6:
        return "food"
    else:
        return "furniture"

# transofrmation de la fonction defini dessus en UDF
checkFoodOrFurniture_python_udf = f.udf(checkFoodOrFurniture, StringType())

# definition de la fonction qui utilise la UDF pour ajuter le nom de catégorie
def useUdfToAddCategoryNameColumn(udf_function_to_experiment, df_to_modify, udf_type, spark_session=None):
    if udf_type == "python_udf":
        print(udf_type)
        return df_to_modify.withColumn(
            "category_name",
            udf_function_to_experiment(df_to_modify["category"])
        )
    elif udf_type == "scala_udf":
        print(udf_type)
        return df_to_modify.withColumn(
            "category_name"
            , udf_function_to_experiment(spark_session, df_to_modify["category"])
        )
    elif udf_type == "no_udf":
        print(udf_type)
        return udf_function_to_experiment(df_to_modify)

########### DEFINITION METHODES CALCUL DE TEMPS
# Méthode 1 : Utiliser `time`
def methode_time(udf_function_to_experiment, df_to_modify, udf_type, spark_session=None):
    debut = time()

    df_to_modify = useUdfToAddCategoryNameColumn(udf_function_to_experiment, df_to_modify, udf_type, spark_session)
    if show_dataframes_at_end_of_processing: df_to_modify.show()
    if count_dataframes: df_to_modify.count()

    fin = time()
    temps_execution = fin - debut

    print(f"Temps d'exécution (méthode 1 - time) : {temps_execution} secondes")

    return temps_execution

# Méthode 2 : Utiliser `datetime`
def methode_datetime(udf_function_to_experiment, df_to_modify, udf_type, spark_session=None):
    debut = datetime.now()

    df_to_modify = useUdfToAddCategoryNameColumn(udf_function_to_experiment, df_to_modify, udf_type, spark_session)
    if show_dataframes_at_end_of_processing: df_to_modify.show()
    if count_dataframes: df_to_modify.count()

    fin = datetime.now()
    temps_execution = (fin - debut).total_seconds()
    print(f"Temps d'exécution (méthode 2 - datetime) :{temps_execution:.2f} secondes")

    return temps_execution

# Méthode 3 : Utiliser `timeit`
def methode_timeit(udf_function_to_experiment, df_to_modify, udf_type, spark_session=None):

    def code_a_mesurer(udf_function_to_experiment, df_to_modify, udf_type, spark_session=None):
        df_to_modify = useUdfToAddCategoryNameColumn(udf_function_to_experiment, df_to_modify, udf_type, spark_session)
        if show_dataframes_at_end_of_processing: df_to_modify.show()
        if count_dataframes: df_to_modify.count()

    temps_execution = timeit.timeit(functools.partial(code_a_mesurer, udf_function_to_experiment, df_to_modify, udf_type, spark_session)
                                    , number=1)
    print(f"Temps d'exécution (méthode 3 - timeit) :{temps_execution:.2f} secondes")

    return temps_execution

# Méthode 4 : Utiliser un décorateur
def mesurer_temps(fonction):
    def wrapper(*args, **kwargs):
        debut = time()
        result = fonction(*args, **kwargs)
        fin = time()
        print(f"Temps d'exécution ({fonction.__name__} 4) : {fin - debut:.2f} secondes")

        temps_execution = fin - debut
        return temps_execution
    return wrapper

@mesurer_temps
def methode_decorateur(udf_function_to_experiment, df_to_modify, udf_type, spark_session=None):
    df_to_modify = useUdfToAddCategoryNameColumn(udf_function_to_experiment, df_to_modify, udf_type, spark_session)
    if show_dataframes_at_end_of_processing: df_to_modify.show()
    if count_dataframes: df_to_modify.count()
    

# Méthode 5 : Utiliser `perf_counter`
def methode_perf_counter(udf_function_to_experiment, df_to_modify, udf_type, spark_session=None):
    debut = perf_counter()

    df_to_modify = useUdfToAddCategoryNameColumn(udf_function_to_experiment, df_to_modify, udf_type, spark_session)
    if show_dataframes_at_end_of_processing: df_to_modify.show()
    if count_dataframes: df_to_modify.count()

    fin = perf_counter()
    temps_execution = fin - debut
    print(f"Temps d'exécution (méthode 5 - perf_counter) : {temps_execution:.2f} secondes")

    return temps_execution

########### MAIN
def main():
    # Initialization commune
    spark_session = SparkSession.builder.appName("udfpython").master("local[*]").getOrCreate()
    sell_df = spark_session.read.option("header", True).csv("src/resources/exo4/sell.csv")

    # Appeler les différentes méthodes
    methode_time(checkFoodOrFurniture_python_udf, sell_df, "python_udf")
    methode_datetime(checkFoodOrFurniture_python_udf, sell_df, "python_udf")
    methode_timeit(checkFoodOrFurniture_python_udf, sell_df, "python_udf")
    temps_execution = methode_decorateur(checkFoodOrFurniture_python_udf, sell_df, "python_udf")
    methode_perf_counter(checkFoodOrFurniture_python_udf, sell_df, "python_udf")

    spark_session.stop()
    
if __name__ == "__main__":
    main()