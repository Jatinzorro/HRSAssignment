from pyspark.sql import functions as F

def read_csv_file(spark_session, loc, header='false'):
    return spark_session.read.format('csv') \
        .option('header', header) \
        .load(loc)


def arrival_date_calculate(df):
    return df.withColumn('arrival_date',
                         F.to_date(
                             F.concat_ws(
                                 "-", F.col("arrival_date_year"), F.col("arrival_date_month"),
                                 F.col("arrival_date_day_of_month")
                             ), "yyyy-MMM-dd")
                         )


def departure_date_calculate(df):
    return df.withColumn('departure_date',
                         F.expr(
                             "date_add(arrival_date, cast(stays_in_weekend_nights as int)+cast(stays_in_week_nights as int))")
                         )


def family_breakfast_check(df):
    return df \
        .withColumn('no_of_children', F.col('children') + F.col('babies')) \
        .withColumn('with_family_breakfast', F.when(F.expr('no_of_children > 0'), F.lit('YES')).otherwise(F.lit('NO'))) \
        .drop('no_of_children')


def write_parquet_file(df, loc):
    df.write \
        .mode('overwrite') \
        .parquet(loc)