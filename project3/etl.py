import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, MapType, StringType
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.functions import from_unixtime, year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName('Data Lake') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3') \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + '/song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_df = df \
        .select('song_id', 'title', 'artist_id', 'year', 'duration') \
        .drop_duplicates(subset=['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_df.write.parquet(output_data + 'songs', mode='overwrite')

    # extract columns to create artists table
    artists_df = df \
        .select(
            'artist_id',
            col('artist_name').alias('name'),
            col('artist_location').alias('location'),
            col('artist_latitude').alias('latitude'),
            col('artist_longitude').alias('longitude')
        ) \
        .drop_duplicates(subset=['artist_id'])

    # write artists table to parquet files
    artists_df.write.parquet(output_data + 'artists', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    filtered_df = df.filter('page == "NextSong"')

    # extract columns for users table
    users_df = filtered_df.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        'gender',
        'level'
    )

    # write users table to parquet files
    users_df.write.parquet(output_data + 'users', mode='overwrite')

    'start_time': date_format(ts, 'MM/dd/yyyy H:m:s'),
    'hour': hour(ts),
    'day': dayofmonth(ts),
    'week': weekofyear(ts),
    'month': month(ts),
    'year': year(ts),
    'weekday': date_format(ts, 'E')

    time_df = filtered_df \
        .withColumn('start_time', f.from_unixtime(filtered_df.ts / 1000)) \
        .select(hour('start_time').alias('hour'),
                dayofmonth('start_time').alias('day'),
                weekofyear('start_time').alias('week'),
                month('start_time').alias('month'),
                year('start_time').alias('year'),
                date_format('start_time', 'E').alias('weekday')
                )

    # write time table to parquet files partitioned by year and month
    time_df.write.parquet(output_data + 'time', mode='overwrite')

    # read in song data to use for songplays table. Read from parquet as that
    # table is the dimension table the end-user will ultimately be using.
    song_data = output_data + 'songs'

    # read song data file
    song_df = spark.read.parquet(song_data)

    # join logs and songs
    joined_df = df.join(song_df, 'song_id')

    # extract columns from joined dataset to create songplays table
    songplays_df = joined_df.select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        col('sessionId').alias('session_id'),
        'location',
        col('userAgent').alias('user_agent')
    )

    songplays_df = songplays_df.withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_df.write.parquet(output_data + 'songplays', mode='overwrite')


def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://av-sparkify-data-lake/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
