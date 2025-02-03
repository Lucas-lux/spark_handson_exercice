from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
    if SparkSession._instantiatedSession is None:
        return SparkSession.builder \
            .appName("spark_aggregate_job") \
            .master("local[*]") \
            .getOrCreate()
    return SparkSession._instantiatedSession


# Lire le fichier Parquet du premier job
def read_clean_data(spark, clean_data_path):
    return spark.read.parquet(clean_data_path)

# Calculer la population par département
def calculate_population_by_departement(df):
    return df.groupBy("departement") \
             .count() \
             .withColumnRenamed("count", "nb_people") \
             .orderBy(col("nb_people").desc(), col("departement"))

# Écrire le résultat en CSV
def write_csv(df, output_path):
    df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

def main():
    spark = create_spark_session()

    # Chemin du fichier clean
    clean_data_path = "data/exo2/clean"
    output_path = "data/exo2/aggregate"

    # Lire les données clean
    clean_df = read_clean_data(spark, clean_data_path)

    # Calculer la population par département
    population_df = calculate_population_by_departement(clean_df)
    population_df.show()

    # Écrire en CSV
    write_csv(population_df, output_path)

    spark.stop()

if __name__ == "__main__":
    main()