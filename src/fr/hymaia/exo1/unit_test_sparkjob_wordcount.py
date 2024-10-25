import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from .main import wordcount 

class TestWordCount(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("test_wordcount") \
            .master("local[*]") \
            .getOrCreate()

        self.sample_data = [
            Row(text="hello world"),
            Row(text="hello spark world"),
            Row(text="spark with spark")
        ]
        self.df = self.spark.createDataFrame(self.sample_data)

    def tearDown(self):
        self.spark.stop()

    def test_wordcount(self):
        result_df = wordcount(self.df, 'text')
        result = result_df.orderBy("word").collect()

        expected_counts = {
            "hello": 2,
            "world": 2,
            "spark": 3,
            "with": 1
        }

        for row in result:
            self.assertEqual(row['count'], expected_counts[row['word']])

    def test_wordcount_empty(self):
        empty_df = self.spark.createDataFrame([], self.df.schema)
        result_df = wordcount(empty_df, 'text')
        result = result_df.collect()

        self.assertEqual(len(result), 0)

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestWordCount)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

def main(): 
    suite = unittest.TestLoader().loadTestsFromTestCase(TestWordCount)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)