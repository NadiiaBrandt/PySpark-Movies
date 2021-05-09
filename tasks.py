from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql import functions as f
import Utilit as u



def top_100(df):
    """
    Top 100 films of all years
    """
    return df \
        .filter((f.col("numVotes") >= 100000) & (f.col("titleType") == "movie")) \
        .select("tconst", "primaryTitle", "numVotes", "averageRating", "startYear") \
        .orderBy(f.col("averageRating").desc()) \
        .limit(100)


def top_in_last_10_years(df):
    """
       Top 100 films in last 10 years
    """
    return df \
        .filter(((f.col("numVotes") >= 100000) & (f.col("titleType") == "movie")) & (f.col("startYear") > datetime.now().year - 10)) \
        .select("tconst", "primaryTitle", "numVotes", "averageRating", "startYear") \
        .orderBy(f.col("averageRating").desc()) \
        .limit(100)


def popular_in_60(df):
    """
       Top 100 films in 60's
    """
    return df \
        .filter((f.col("startYear").between(1960, 1969)) & ((f.col("numVotes") >= 100000) & (f.col("titleType") == "movie"))) \
        .select("tconst", "primaryTitle", "numVotes", "averageRating", "startYear") \
        .orderBy(f.col("averageRating").desc()) \
        .limit(100)


def top_10_each_genre(df):
    """
        Top 10 films in each genre
    """
    return df \
        .select("tconst", "primaryTitle", "startYear", "genres", "averageRating", "numVotes") \
        .withColumn("genres", f.explode(f.split(f.col("genres"), ","))) \
        .withColumn("row_number", f.row_number().over(u.window("genres"))).filter(f.col("row_number") <= 10)


def top_10_in_each_decade(df):
    """
        Top 10 films in each genre for decades since 1950
    """

    genre_window = Window.partitionBy("genres") \
        .orderBy(f.col("averageRating").desc(), f.col("numVotes").desc())

    decade_window = Window.partitionBy("decades") \
        .orderBy(f.col("decades").desc())

    df = df \
        .withColumn("genres", f.explode(f.split("genres", ","))) \
        .withColumn("decades", (f.floor(f.col("startYear") / 10) * 10)) \
        .orderBy(f.col("averageRating").desc(), f.col("numVotes").desc()) \
        .withColumn("genre_rank", f.dense_rank().over(genre_window)) \
        .withColumn("decade_rank", f.dense_rank().over(decade_window))

    return df \
        .select("tconst", "primaryTitle", "startYear", "genres", "averageRating", "numVotes", "decades") \
        .filter(df.genre_rank <= 10) \
        .orderBy(f.col("decades").desc(), f.col("genres"), f.col("genre_rank"))


def top_actors(df):
    """
    The most demanded actors
    """
    df.orderBy(f.col("averageRating").desc(), f.col("numVotes").desc())
    df = u.inner_join(df, principals, "tconst").drop(principals.tconst)
    df = u.inner_join(df, name_basics, "nconst").drop(name_basics.nconst).filter(f.col("category").like("act%"))

    return df \
        .groupby("nconst", "primaryName").count() \
        .select("primaryName") \
        .orderBy(f.col("count").desc())


def directors_top_films(df):
    """
    Top 5 films by each director"s
    """

    df.drop(basics.tconst).orderBy(f.col("averageRating").desc(), f.col("numVotes").desc())
    df = u.inner_join(df, crew, "tconst") \
        .drop(crew.tconst) \
        .withColumn("director", f.explode(f.split("directors", ",")))
    df = u.inner_join(df, name_basics, df.director == name_basics.nconst) \
        .drop(name_basics.nconst) \
        .withColumn("f_rank", f.dense_rank().over(u.window("director")))

    return df.select("primaryName", "primaryTitle", "startYear", "averageRating", "numVotes") \
        .filter(f.col("f_rank") <= 5) \
        .orderBy(f.col("director"))


if __name__ == "__main__":
    """
       Counting top 100 movies
    """

    spark = SparkSession \
        .builder \
        .appName("Movies") \
        .getOrCreate()

    ratings = spark.read.csv("Datasets/ratings.tsv", sep=r"\t", header=True, inferSchema=True)
    basics = spark.read.csv("Datasets/basics.tsv", sep=r"\t", header=True, inferSchema=True)
    principals = spark.read.csv("Datasets/principals.tsv", sep=r"\t", header=True, inferSchema=True)
    name_basics = spark.read.csv("Datasets/name.basics.tsv", sep=r"\t", header=True, inferSchema=True)
    crew = spark.read.csv("Datasets/crew.tsv", sep=r"\t", header=True, inferSchema=True)

    sew = basics.join(ratings, basics.tconst == ratings.tconst) \
        .drop(ratings.tconst)

    u.write_csv(top_100(sew), "top_100")
    u.write_csv(top_in_last_10_years(sew), "top_in_last_10_years")
    u.write_csv(popular_in_60(sew), "top_in_60")
    u.write_csv(top_10_each_genre(sew), "top_in_genres")
    u.write_csv(top_10_in_each_decade(sew), "top_films_by_decade")
    u.write_csv(top_actors(sew), "top_actors")
    u.write_csv(directors_top_films(sew), "director_top_films")

    spark.stop()
