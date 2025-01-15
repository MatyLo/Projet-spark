from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_aggregate_job import calcul_dep_pop
from pyspark.sql import Row

class TestMain(unittest.TestCase):
    def test_calcul_dep_pop(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name="Maty", age="25", zip="94270", city="LE KREMLIN BICETRE", departement="94"),
                Row(name="Anna", age="55", zip="20190", city="AZILONE AMPAZA", departement="2A"),
                Row(name="Yvonne", age="18", zip="02100", city="SAINT QUENTIN", departement="02"),
                Row(name="Loane", age="43", zip="94270", city="LE KREMLIN BICETRE", departement="94"),
                Row(name="Yvonne", age="18", zip="02100", city="SAINT QUENTIN", departement="02"),
                Row(name="Ines", age="32", zip="94270", city="LE KREMLIN BICETRE", departement="94"),
                Row(name="Romain", age="30", zip="20191", city="UNKNOWN", departement= "2B")
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(departement="94", count=3),
                Row(departement="2A", count=1),
                Row(departement="02", count=2),
                Row(departement="2B", count=1),
            ]
        )

        actual = calcul_dep_pop(input)

        self.assertCountEqual(actual.collect(), expected.collect())