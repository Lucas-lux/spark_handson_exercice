from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType

def create_spark_session():
    # Arrêt de la session existante
    if SparkSession._instantiatedSession:
        SparkSession._instantiatedSession.stop()
        SparkSession._instantiatedSession = None
    
    return SparkSession.builder \
        .appName("spark_clean_job") \
        .master("local[*]") \
        .getOrCreate()


def read_data(spark, clients_path, villes_path):
    clients_df = spark.read.option("header", "true").csv(clients_path)
    villes_df = spark.read.option("header", "true").csv(villes_path)
    
    # Conversion explicite des types
    clients_df = clients_df.withColumn("age", col("age").cast("integer"))
    clients_df = clients_df.withColumn("id", col("id").cast("integer"))
    
    return clients_df, villes_df


def filter_major_clients(df):
    return df.filter(col("age") >= 18)

def join_clients_villes(clients_df, villes_df):
    return clients_df.join(
        villes_df,
        clients_df.zip == villes_df.zip,
        "inner"
    ).select(
        clients_df["*"],
        villes_df.city
    ).distinct()  # Élimine les doublons




def add_departement_column(df):
    def extract_departement(zip_code):
        if not zip_code or not isinstance(zip_code, str):
            return None
        if len(zip_code) < 5:  # Vérifie la longueur minimale
            return None
        if not zip_code[:2].isdigit():
            return None
            
        # Gestion spéciale des codes corses
        if zip_code.startswith("20"):
            if int(zip_code) < 20200:
                return "2A"
            return "2B"
        return zip_code[:2]
    
    extract_departement_udf = udf(extract_departement, StringType())
    return df.withColumn("departement", extract_departement_udf("zip"))



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