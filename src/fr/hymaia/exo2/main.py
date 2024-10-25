from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def create_spark_session():
    return SparkSession.builder \
        .appName("spark_clean_job") \
        .master("local[*]") \
        .getOrCreate()

def read_data(spark, clients_path, villes_path):
    clients_df = spark.read.csv(clients_path, header=True, inferSchema=True)
    villes_df = spark.read.csv(villes_path, header=True, inferSchema=True)
    return clients_df, villes_df

def filter_major_clients(clients_df):
    return clients_df.where(col("age") >= 18)

def join_clients_villes(clients_df, villes_df):
    return clients_df.join(villes_df, "zip")

def add_departement_column(df):
    return df.withColumn("departement",
                         when(col("zip").substr(1, 2) == "20",
                              when(col("zip") <= "20190", "2A").otherwise("2B"))
                         .otherwise(col("zip").substr(1, 2)))

def write_parquet(df, output_path):
    df.write.mode("overwrite").partitionBy("departement").parquet(output_path)

def main():
    spark = create_spark_session()

    clients_path = "src/resources/exo2/clients_bdd.csv"
    villes_path = "src/resources/exo2/city_zipcode.csv"
    output_path = "data/exo2/clean"

    clients_df, villes_df = read_data(spark, clients_path, villes_path)

    major_clients_df = filter_major_clients(clients_df)

    joined_df = join_clients_villes(major_clients_df, villes_df)

    result_df = add_departement_column(joined_df)
    result_df.show()

    write_parquet(result_df, output_path)

    spark.stop()

if __name__ == "__main__":
    main()