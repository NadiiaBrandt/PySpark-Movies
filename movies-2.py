from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == "__main__":
    """
        
    """

    spark = SparkSession \
        .builder \
        .appName("1Run") \
        .getOrCreate()

    rat = spark.read.csv("Datasets/ratings.tsv", sep=r'\t', header=True, inferSchema=True)
    bas = spark.read.csv("Datasets/basics.tsv", sep=r'\t', header=True, inferSchema=True)

    spark.stop()
