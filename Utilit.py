from pyspark.sql import Window
from pyspark.sql import functions as f


def window(column):
    """
    Window function
    """
    return Window.partitionBy(column) \
        .orderBy(f.col("averageRating").desc(),
                 f.col("numVotes").desc())


def inner_join(left_df, right_df, condition, how="inner"):
    return left_df.join(right_df, condition, how)


def write_csv(data_frame, file_name):
    """
    Write dataframe into csv
    """
    data_frame.coalesce(1).write \
        .option("header", True).mode("overwrite") \
        .save(f"outputs/{file_name}", format("csv"))