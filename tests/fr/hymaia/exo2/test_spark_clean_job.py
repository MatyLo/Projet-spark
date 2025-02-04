from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_clean_job import clients_majeurs,nom_ville,departement
from pyspark.sql import Row

class TestMain(unittest.TestCase):
    def test_clients_majeurs(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name="Maty", age="25", zip="94270"),
                Row(name="Anna", age="55", zip="20190"),
                Row(name="Sire", age="13", zip="75013"),
                Row(name="Hélène", age="5", zip="68210"),
                Row(name="Yvonne", age="18", zip="02100"),
                Row(name="Yvonne", age="22", zip="02100"),
                Row(name="Yvonne", age="7", zip="02100"),
                Row(name="Stéphanie", age="17", zip="26150")
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(naame="Maty", age="25", zip="94270"),
                Row(naame="Anna", age="55", zip="20190"),
                Row(naame="Yvonne", age="18", zip="02100"),
                Row(naame="Yvonne", age="22", zip="02100"),
            ]
        )

        actual = clients_majeurs(input)

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_nom_ville(self):
        # GIVEN
        clients = spark.createDataFrame(
            [
                Row(name="Maty", age="25", zip="94270"),
                Row(name="Anna", age="55", zip="20190"),
                Row(name="Sire", age="13", zip="75013"),
                Row(name="Hélène", age="5", zip="68210"),
                Row(name="Yvonne", age="18", zip="02100"),
                Row(name="Yvonne", age="18", zip="02100"), #Si il rencontre la même personne 2 fois, il la met 2 fois
                Row(name="Yvonne", age="7", zip="02100")
            ]
        )
        code_ville = spark.createDataFrame(
            [
                Row(zip="94270", city="LE KREMLIN BICETRE"),
                Row(zip="75013", city="PARIS"),
                Row(zip="25650", city="VILLE DU PONT"),
                Row(zip="02100", city="SAINT QUENTIN"),
                Row(zip="68210", city="DANNEMARIE")
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(name="Maty", age="25", zip="94270", city="LE KREMLIN BICETRE"),
                Row(name="Yvonne", age="18", zip="02100", city="SAINT QUENTIN"),
                Row(name="Yvonne", age="18", zip="02100", city="SAINT QUENTIN"),
            ]
        )

        actual = nom_ville(clients_majeurs(clients), code_ville)

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_departement(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name="Maty", age="25", zip="94270", city="LE KREMLIN BICETRE"),
                Row(name="Anna", age="55", zip="20190", city="AZILONE AMPAZA"),
                Row(name="Yvonne", age="18", zip="02100", city="SAINT QUENTIN"),
                Row(name="Yvonne", age="18", zip="02100", city="SAINT QUENTIN"),
                Row(name="Romain", age="30", zip="20191", city="UNKNOWN"),
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(name="Maty", age="25", zip="94270", city="LE KREMLIN BICETRE", departement="94"),
                Row(name="Anna", age="55", zip="20190", city="AZILONE AMPAZA", departement="2A"),
                Row(name="Yvonne", age="18", zip="02100", city="SAINT QUENTIN", departement="02"),
                Row(name="Yvonne", age="18", zip="02100", city="SAINT QUENTIN", departement="02"),
                Row(name="Romain", age="30", zip="20191", city="UNKNOWN", departement= "2B"),
            ]
        )

        actual = departement(input)

        self.assertCountEqual(actual.collect(), expected.collect())