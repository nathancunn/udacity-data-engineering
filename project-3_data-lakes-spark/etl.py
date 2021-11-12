import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Initiates a spark session

    Returns:
        SparkSession: A spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes song data stored at `input_data/song_data/*/*/*/*.json`
    and creates two tables and stores as parquet at `output_data`:
        - songs_table: The song_id, title, artist_id, release year, 
        and duration of each song
        - artists_table: The artist_id, name, location, latitude and longitude 
        for each distinct artist in the song data
    Args:
        spark (SparkSession): A spark session as created by create_spark_session()
        input_data (String): A string indicating the location of the song data, 
        e.g. an S3 bin
        output_data (String): A string indicating the location to store output, 
        e.g. an S3 bin
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')
    
    # read song data file
    df = spark.read.json(song_data)
    print(f"Loaded song_data from {input_data}")
    df.createOrReplaceTempView("song_data")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT song_id, title, artist_id, year, duration
    FROM song_data
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_path = os.path.join(output_data, "songs_table.parquet")
    (songs_table.
     write.
     mode("overwrite").
     partitionBy("year", "artist_id").
     parquet(songs_table_path))
    print(f"Stored song table on {songs_table_path}")
    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT 
        DISTINCT(artist_id) AS artist_id, 
        artist_name AS name, 
        artist_location AS location, 
        artist_latitude AS latitude, 
        artist_longitude AS longitude
    FROM song_data
    """)
    
    # write artists table to parquet files
    artists_table_path = os.path.join(output_data, "artists_table.parquet")
    (artists_table.
     write.
     mode("overwrite").
     parquet(artists_table_path))
    print(f"Stored artists table at {artists_table_path}")


def process_log_data(spark, input_data, output_data):
    """Processes log data stored at `input_data/log_data/*/*/*/*.json`
    and creates three tables and stores as parquet at `output_data`:
        - users_table: the id, first name, surname, gender, and membership level
        of all users recorded in the log table
        - time_table: the timestamp of a song play, and the 
        corresponding hour, day, week of year, month, year, and day of week
        for all unique song plays in the log data
        - songplays_table: the start time of a song play, the user playing it, 
        the IDs of the song, artist, and session, as well as the location and 
        user_agent of the user.

    Args:
        spark (SparkSession): A spark session as created by create_spark_session()
        input_data (String): A string indicating the location of the log data, 
        e.g. an S3 bin
        output_data (String): A string indicating the location to store output, 
        e.g. an S3 bin
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*', '*')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")
    df.createOrReplaceTempView("songplays")

    # extract columns for users table   
    users_table = spark.sql("""
    SELECT 
        DISTINCT(userId) AS user_id, 
        firstName AS first_name, 
        lastName AS last_name, 
        gender, 
        level
    FROM songplays
    """)
    
    # write users table to parquet files
    users_table_path = os.path.join(output_data, "users_table.parquet")
    (users_table.
     write.
     mode("overwrite").
     parquet(users_table_path))
    print(f"Stored users table at {users_table_path}")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: pd.Timestamp(x, unit = "ms"), TimestampType())
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: pd.Timestamp(x, unit = "ms"), TimestampType())
    df = df.withColumn("datetime", get_datetime("ts"))
    df.createOrReplaceTempView("log_table")
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT 
        DISTINCT(timestamp) AS start_time, 
        HOUR(timestamp) AS hour,
        day(timestamp) AS day,
        weekofyear(timestamp) AS week,
        month(timestamp) AS month,
        year(timestamp) AS year,
        dayofweek(timestamp) AS weekday
    FROM log_table
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table_path = os.path.join(output_data, "time_table.parquet")
    (time_table.
     write.
     mode("overwrite").
     partitionBy("year", "month").
     parquet(time_table_path))
    print(f"Stored time table at {time_table_path}")

    # read in song data to use for songplays table
    song_df_path = os.path.join(input_data, "song_data", "*", "*", "*")
    song_df = spark.read.json(song_df_path).alias("song_df")
    df = df.alias("df")
    
    joined_df = df.join(
        song_df, 
        col('df.artist') == col('song_df.artist_name'), 
        'inner',
    )

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = joined_df.select(
        col("timestamp").alias("start_time"),
        col("userId").alias("user_id"),
        col("level").alias("level"),
        col("song_id").alias("song_id"),
        col("artist_id").alias("artist_id"),
        col("sessionId").alias("session_id"),
        col("location").alias("location"),
        col("userAgent").alias("user_agent")
    ).withColumn('songplay_id', monotonically_increasing_id())
    
    # Add year and month to enable partitioning
    songplays_table = (songplays_table.
                       withColumn('year', year(songplays_table.start_time)).
                       withColumn('month', month(songplays_table.start_time)))

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path= os.path.join(output_data, "songplays_table.parquet")
    (songplays_table.
     write.
     mode("overwrite").
     partitionBy("year", "month").
     parquet(songplays_table_path))
    print(f"Stored songplays table at {songplays_table_path}")
                 


def main():
    """
    Allows for script to be run in the command line. 
    Requires the locations of the input_data and output_data respectively 
    to be specified.
    """
    import sys
    n = len(sys.argv)
    if n != 3:
        raise ValueError("Please specify an input s3 bin and output s3 bin")
    spark = create_spark_session()
    input_data = sys.argv[1]
    output_data = sys.argv[2]
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
