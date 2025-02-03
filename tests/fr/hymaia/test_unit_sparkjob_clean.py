import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.fr.hymaia.exo2.main import create_spark_session, read_data, filter_major_clients, join_clients_villes, add_departement_column, write_parquet
import os
import shutil

class TestSparkJobs(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("test_spark_clean_job") \
            .master("local[*]") \
            .getOrCreate()
            
        self.sample_data = [
            Row(id=1, name="Alice", age=25, zip="75001"),
            Row(id=2, name="Bob", age=30, zip="20180")  # Code postal corse
        ]
        
        self.villes_sample = [
            Row(zip="75001", city="Paris"),
            Row(zip="20180", city="Ajaccio")  # Ville corse
        ]
        
        self.clients_df = self.spark.createDataFrame(self.sample_data)
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
        clients_path = "tests/clients_sample.csv"
        villes_path = "tests/villes_sample.csv"
        
        # Nettoyage des répertoires
        for path in [clients_path, villes_path]:
            if os.path.exists(path):
                shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)
        
        # Création des données de test pour clients
        clients_sample = [
            Row(id=1, name="Alice", age=25, zip="75001"),
            Row(id=2, name="Bob", age=30, zip="75002"),
            Row(id=3, name="Charlie", age=35, zip="75003")
        ]
        clients_df = self.spark.createDataFrame(clients_sample)
        
        # Création des données de test pour villes avec 3 entrées
        villes_sample = [
            Row(zip="75001", city="Paris"),
            Row(zip="75002", city="Lyon"),
            Row(zip="75003", city="Marseille")
        ]
        villes_df = self.spark.createDataFrame(villes_sample)
        
        # Écriture des fichiers
        clients_df.write.csv(clients_path, header=True, mode="overwrite")
        villes_df.write.csv(villes_path, header=True, mode="overwrite")
        
        # Lecture des données
        clients_read, villes_read = read_data(self.spark, clients_path, villes_path)
        
        # Vérifications
        self.assertEqual(clients_read.count(), 3)
        self.assertEqual(villes_read.count(), 3)
        
        # Nettoyage
        shutil.rmtree(clients_path)
        shutil.rmtree(villes_path)

    def test_filter_major_clients(self):
        major_clients_df = filter_major_clients(self.clients_df)
        result = major_clients_df.orderBy("name").collect()
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[1]["name"], "Bob")   

    def test_join_clients_villes(self):
        # Mise à jour des données de test
        self.clients_sample = [
            Row(id=1, name="Alice", age=25, zip="75001"),
            Row(id=2, name="Bob", age=30, zip="75002")
        ]
        self.villes_sample = [
            Row(zip="75001", city="Paris"),
            Row(zip="75002", city="Paris")
        ]
        
        self.clients_df = self.spark.createDataFrame(self.clients_sample)
        self.villes_df = self.spark.createDataFrame(self.villes_sample)
        
        joined_df = join_clients_villes(self.clients_df, self.villes_df)
        result = joined_df.collect()
        
        self.assertEqual(len(result), 2)


    def test_add_departement_column(self):
        # Mise à jour des données de test avec les bons codes postaux
        self.villes_sample = [
            Row(zip="75001", city="Paris"),
            Row(zip="20180", city="Ajaccio"),  # Code postal 2A
            Row(zip="20200", city="Bastia")    # Code postal 2B
        ]
        self.villes_df = self.spark.createDataFrame(self.villes_sample)
        
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
        
        self.assertIsNone(result[0]["departement"])
        self.assertIsNone(result[1]["departement"])


    def test_write_parquet(self):
        output_path = "tests/output_parquet"
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
            
        # Ajout de la colonne departement avant l'écriture
        df_with_dept = add_departement_column(self.clients_df)
        write_parquet(df_with_dept, output_path)
        
        self.assertTrue(os.path.exists(output_path))
        shutil.rmtree(output_path)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSparkJobs)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

def main(): 
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSparkJobs)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
