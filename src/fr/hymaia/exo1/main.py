import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("JupyterPySpark") \
        .master("local[*]") \
        .getOrCreate()
    spark

    df = spark.read.csv('./src/resources/exo1/data.csv', header=True)
    df.printSchema()
    result = wordcount(df, 'text')
    result.show()
    output_path = 'data/exo1/output'
    result.write.partitionBy('count').parquet(output_path)

    parquet_df = spark.read.parquet(output_path)
    parquet_df.show(truncate=False)


    spark.stop()


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
        