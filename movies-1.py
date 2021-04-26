from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """

    spark = SparkSession \
        .builder \
        .appName("1Run") \
        .getOrCreate()

    rat = spark.read.csv("Datasets/ratings.tsv", sep=r'\t', header=True, inferSchema=True)
    bas = spark.read.csv("Datasets/basics.tsv", sep=r'\t', header=True, inferSchema=True)
    table = rat.join(bas, rat.tconst == bas.tconst, "inner"). \
        select(rat.tconst
               , bas.primaryTitle
               , rat.numVotes
               , rat.averageRating
               , bas.startYear) \
        .orderBy(rat.averageRating.desc()) \
        .where(rat.numVotes >= 100000)
    table.show(100)

    last10Y = table.filter(bas.startYear > 2010)
    last10Y.show(100)

    in60Y = table.filter(bas.startYear.between(1959, 1970))
    in60Y.show(100)

    table.write.csv("Output-1.csv")
    spark.stop()