import unittest
from pyspark.sql import SparkSession, Row
import os
from src.fr.hymaia.exo2.main import (
    create_spark_session,
    read_data,
    filter_major_clients,
    join_clients_villes,
    add_departement_column,
    write_parquet
)
from src.fr.hymaia.exo2.main2 import (
    read_clean_data,
    calculate_population_by_departement,
    write_csv
)

class TestSparkIntegration(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("test_spark_integration_job") \
            .master("local[*]") \
            .getOrCreate()

        self.clean_output_path = "tests/tmp/clean_data"
        self.aggregate_output_path = "tests/tmp/aggregate_data"

        self.clients_data = [
            Row(id=1, name="Alice", age=17, zip="75001"),
            Row(id=2, name="Bob", age=25, zip="75002"),
            Row(id=3, name="Charlie", age=18, zip="69001"),
        ]
        self.villes_data = [
            Row(zip="75001", city="Paris"),
            Row(zip="75002", city="Paris"),
            Row(zip="69001", city="Lyon")
        ]

    def tearDown(self):
        self.spark.stop()
        if os.path.exists(self.clean_output_path):
            for root, dirs, files in os.walk(self.clean_output_path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
        if os.path.exists(self.aggregate_output_path):
            for root, dirs, files in os.walk(self.aggregate_output_path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))

    def test_integration_jobs(self):
        # Phase 1 : Exécution du premier job Spark (nettoyage et transformation)
        
        # Créer DataFrames pour clients et villes
        clients_df = self.spark.createDataFrame(self.clients_data)
        villes_df = self.spark.createDataFrame(self.villes_data)

        # Filtrer les clients majeurs
        major_clients_df = filter_major_clients(clients_df)
        self.assertEqual(major_clients_df.count(), 2)  # Bob et Charlie sont majeurs

        # Joindre avec les données de villes
        joined_df = join_clients_villes(major_clients_df, villes_df)
        self.assertEqual(joined_df.count(), 2)  # Il doit y avoir 2 clients après la jointure

        # Ajouter la colonne de département
        result_df = add_departement_column(joined_df)

        # Écrire le résultat en Parquet
        write_parquet(result_df, self.clean_output_path)

        # Phase 2 : Exécution du second job Spark (agrégation par département)
        
        # Lire les données nettoyées
        clean_df = read_clean_data(self.spark, self.clean_output_path)
        self.assertGreater(clean_df.count(), 0)  # Vérifier que les données ont été lues

        # Calculer la population par département
        population_df = calculate_population_by_departement(clean_df)
        population_result = population_df.collect()
        
        # Écrire en CSV
        write_csv(population_df, self.aggregate_output_path)

        # Vérifier que le fichier CSV a été écrit
        self.assertTrue(os.path.exists(self.aggregate_output_path))

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSparkIntegration)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

def main(): 
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSparkIntegration)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
