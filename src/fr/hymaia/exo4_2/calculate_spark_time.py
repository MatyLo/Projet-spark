import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from time import time, perf_counter
from datetime import datetime
import timeit
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType,StructField, StringType, FloatType

#import time calculation methods
from src.fr.hymaia.exo4_2.calculation_methods import methode_time, methode_datetime, methode_timeit, methode_decorateur, methode_perf_counter

#import the different udfs from the other files
from src.fr.hymaia.exo4.python_udf import category_name_udf
from src.fr.hymaia.exo4.scala_udf import addCategoryName
from src.fr.hymaia.exo4.no_udf import add_category_name_column


# Create a Spark session
spark_session = SparkSession.builder \
    .appName("Time_Execution") \
    .config("spark.jars", "src/resources/exo4/udf.jar") \
    .getOrCreate()

# Define the function to execute and measure its execution time
def calculate_time(dataframe):
    return dataframe.count()  # or any other PySpark operation you want to measure

#function that will add the measurements to the final dataframe
def addTimeMeasurementToResults(results_df, results_table_schema, type_of_udf: str, time_measurement_method: str, time_of_execution: float):
        row = [(type_of_udf, time_measurement_method, time_of_execution)]
        row_df = spark_session.createDataFrame(data=row,schema=results_table_schema)
        return results_df.unionAll(row_df)

# Fonction principale
def main():
    ################ first make all the basic operations
    ## Charger le DataFrame à partir du fichier CSV
    sell_df = spark_session.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    
    ## Convertir la colonne 'category' en entier si ce n'est pas déjà le cas
    sell_df = sell_df.withColumn("category", sell_df["category"].cast("int"))

    results_table_schema = StructType([ \
            StructField("type_of_udf", StringType(),True), \
            StructField("time_measurement_method", StringType(),True), \
            StructField("time_of_execution", FloatType(),True)
        ])
    row = [("Initializing Row", "Initializing Row", 0.0)]
    row_df = spark_session.createDataFrame(data=row,schema=results_table_schema)
    results_df = row_df
    
    ################ begin of time experimentations ################
    ###### python udf
    temps_execution = methode_time(category_name_udf, sell_df, "python_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "python udf", "time", temps_execution)

    temps_execution = methode_datetime(category_name_udf, sell_df, "python_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "python udf", "datetime", temps_execution)

    temps_execution = methode_timeit(category_name_udf, sell_df, "python_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "python udf", "timeit", temps_execution)

    temps_execution = methode_decorateur(category_name_udf, sell_df, "python_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "python udf", "decorateur", temps_execution)

    temps_execution = methode_perf_counter(category_name_udf, sell_df, "python_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "python udf", "perf_counter", temps_execution)


    # ###### scala udf
    temps_execution = methode_time(addCategoryName, sell_df, "scala_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "scala udf", "time", temps_execution)

    temps_execution = methode_datetime(addCategoryName, sell_df, "scala_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "scala udf", "datetime", temps_execution)

    temps_execution = methode_timeit(addCategoryName, sell_df, "scala_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "scala udf", "timeit", temps_execution)

    temps_execution = methode_decorateur(addCategoryName, sell_df, "scala_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "scala udf", "decorateur", temps_execution)

    temps_execution = methode_perf_counter(addCategoryName, sell_df, "scala_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "scala udf", "perf_counter", temps_execution)


    ###### no udf
    temps_execution = methode_time(add_category_name_column, sell_df, "no_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "no udf", "time", temps_execution)

    temps_execution = methode_datetime(add_category_name_column, sell_df, "no_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "no udf", "datetime", temps_execution)

    temps_execution = methode_timeit(add_category_name_column, sell_df, "no_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "no udf", "timeit", temps_execution)

    temps_execution = methode_decorateur(add_category_name_column, sell_df, "no_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "no udf", "decorateur", temps_execution)

    temps_execution = methode_perf_counter(add_category_name_column, sell_df, "no_udf", spark_session)
    results_df = addTimeMeasurementToResults(results_df, results_table_schema, "no udf", "perf_counter", temps_execution)
    
    ###### fin experimentations
    results_df.show()
    results_df.coalesce(1).write.mode("overwrite").format("csv").save("data/exo4/time_execution_results.csv", header = 'true')
    
    spark_session.stop()
    
if __name__ == "__main__":
    main()
    