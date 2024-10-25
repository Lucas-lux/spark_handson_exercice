import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.fr.hymaia.exo2.main2 import create_spark_session, read_clean_data, calculate_population_by_departement, write_csv
import os

class TestSparkAggregateJob(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("test_spark_aggregate_job") \
            .master("local[*]") \
            .getOrCreate()

        # Sample data
        self.clean_data_sample = [
            Row(name="Bob", age=25, zip="75002", departement="75"),
            Row(name="Alice", age=18, zip="69001", departement="69"),
            Row(name="Charlie", age=30, zip="75003", departement="75"),
            Row(name="David", age=22, zip="69002", departement="69"),
            Row(name="Eve", age=21, zip="20115", departement="2A")
        ]
        self.clean_df = self.spark.createDataFrame(self.clean_data_sample)

    def tearDown(self):
        self.spark.stop()

    def test_create_spark_session(self):
        spark = create_spark_session()
        self.assertIsNotNone(spark)
        self.assertEqual(spark.sparkContext.appName, "spark_aggregate_job")
        spark.stop()

    def test_read_clean_data(self):
        test_path = "tests/clean_data"
        self.clean_df.write.parquet(test_path, mode="overwrite")

        clean_df = read_clean_data(self.spark, test_path)
        self.assertEqual(clean_df.count(), len(self.clean_data_sample))

        if os.path.exists(test_path):
            for root, dirs, files in os.walk(test_path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))

    def test_calculate_population_by_departement(self):
        population_df = calculate_population_by_departement(self.clean_df)
        result = population_df.collect()

        self.assertEqual(result[0]["departement"], "75")
        self.assertEqual(result[0]["nb_people"], 2)
        self.assertEqual(result[1]["departement"], "69")
        self.assertEqual(result[1]["nb_people"], 2)
        self.assertEqual(result[2]["departement"], "2A")
        self.assertEqual(result[2]["nb_people"], 1)

    def test_calculate_population_by_departement_empty(self):
        empty_df = self.spark.createDataFrame([], self.clean_df.schema)
        population_df = calculate_population_by_departement(empty_df)
        result = population_df.collect()

        self.assertEqual(len(result), 0)

    def test_write_csv(self):
        output_path = "tests/output_csv"
        write_csv(self.clean_df, output_path)

        self.assertTrue(os.path.exists(output_path))

        if os.path.exists(output_path):
            for root, dirs, files in os.walk(output_path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSparkAggregateJob)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

def main(): 
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSparkAggregateJob)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
