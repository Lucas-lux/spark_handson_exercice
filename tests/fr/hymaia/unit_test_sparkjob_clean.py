import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.fr.hymaia.exo2.main import create_spark_session, read_data, filter_major_clients, join_clients_villes, add_departement_column, write_parquet
import os

class TestSparkJobs(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("test_spark_clean_job") \
            .master("local[*]") \
            .getOrCreate()
        
        self.sample_data = [
            Row(id=1, name="Alice", age=17, zip="75001"),
            Row(id=2, name="Bob", age=25, zip="75002"),
            Row(id=3, name="Charlie", age=18, zip="69001")
        ]
        self.clients_df = self.spark.createDataFrame(self.sample_data)

        self.villes_sample = [
            Row(zip="75001", city="Paris"),
            Row(zip="20115", city="Ajaccio"),
            Row(zip="20200", city="Bastia")
        ]
        self.villes_df = self.spark.createDataFrame(self.villes_sample)

    def tearDown(self):
        self.spark.stop()

    def test_create_spark_session(self):
        # Teste la création d'une session Spark
        spark = create_spark_session()
        self.assertIsNotNone(spark)
        self.assertEqual(spark.sparkContext.appName, "spark_clean_job")
        spark.stop()

    def test_read_data(self):
        # Teste la lecture des données avec des données échantillons
        clients_path = "tests/clients_sample.csv"
        villes_path = "tests/villes_sample.csv"
        self.clients_df.write.csv(clients_path, header=True, mode="overwrite")
        self.villes_df.write.csv(villes_path, header=True, mode="overwrite")

        clients_df, villes_df = read_data(self.spark, clients_path, villes_path)
        self.assertEqual(clients_df.count(), 3)
        self.assertEqual(villes_df.count(), 3)

        # Supprime les fichiers de test
        os.remove(clients_path)
        os.remove(villes_path)

    def test_filter_major_clients(self):
        major_clients_df = filter_major_clients(self.clients_df)
        result = major_clients_df.collect()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "Bob") 
        self.assertEqual(result[1]["name"], "Charlie")  

    def test_join_clients_villes(self):
        # Teste la jointure des clients et des villes par code postal
        joined_df = join_clients_villes(self.clients_df, self.villes_df)
        result = joined_df.collect()

        self.assertEqual(len(result), 2)  # Deux clients ont une correspondance avec les villes
        self.assertEqual(result[0]["city"], "Paris")
        self.assertEqual(result[1]["city"], "Lyon")

    def test_add_departement_column(self):
        result_df = add_departement_column(self.villes_df)
        result = result_df.collect()

        self.assertEqual(result[0]["departement"], "75")
        self.assertEqual(result[1]["departement"], "2A")
        self.assertEqual(result[2]["departement"], "2B")

    def test_add_departement_column_invalid_zip(self):
        villes_sample = [
            Row(zip=None),
            Row(zip="invalid_zip")
        ]
        villes_df = self.spark.createDataFrame(villes_sample)
        
        result_df = add_departement_column(villes_df)
        result = result_df.collect()

        self.assertEqual(result[0]["departement"], None)
        self.assertEqual(result[1]["departement"], None)

    def test_write_parquet(self):
        # Teste l'écriture des données en format Parquet
        output_path = "tests/output_parquet"
        write_parquet(self.clients_df, output_path)

        # Vérifie si les fichiers Parquet ont bien été écrits
        self.assertTrue(os.path.exists(output_path))

        # Supprime les fichiers de test
        if os.path.exists(output_path):
            os.rmdir(output_path)

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSparkJobs)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

def main(): 
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSparkJobs)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
