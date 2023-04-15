import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Creates SparkSession object.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song data specified in the input_data path and creates tables for songs and artists. 
    Writes songs table to parquet files partitioned by year and artist.
    write artists table to parquet files.
        
    Parameters
    ----------
    spark : SparkSession object
        SparkSession object to read and process the data.
    input_data : string
        path to read the dataset files.
    output_data : string
        path to write the processed data files.
    """
    # get filepath to song data file
    song_data = input_data + "song-data/A/A/A/*.json"
    
    song_schema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int())
        ])
    
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates(subset=["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songs_table'), partitionBy=['year',  'artist_id'])

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).dropDuplicates(subset=["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, 'artists_table'))


def process_log_data(spark, input_data, output_data):
    """
    Processes log data specified in the input_data path and creates tables for users, time, and songplays. 
    write users table to parquet files.
    write time table to parquet files partitioned by year and month.
    write songplays table to parquet files partitioned by year and month.
        
    Parameters
    ----------
    spark : SparkSession object
        SparkSession object to read and process the data.
    input_data : string
        path to read the dataset files.
    output_data : string
        path to write the processed data files.
    """
    # get filepath to log data file
    log_data = input_data + "log_data/2018/*/*.json" 

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(["userID", "firstName", "lastName", "gender", "level"]).dropDuplicates(subset=["userID"])
    
    # write users table to parquet files
    users_table = users_table.write.mode("overwrite").parquet(os.path.join(output_data, 'users_table'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
#    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
#    df = df.withColumn("datetime", get_datetime(df_log.ts))
    
    # extract columns to create time table
    time_table = df.selectExpr(
        "timestamp as start_time",
        "hour(timestamp) as hour",
        "dayofmonth(timestamp) as day",
        "weekofyear(timestamp) as week",
        "month(timestamp) as month",
        "year(timestamp) as year",
        "dayofweek(timestamp) as weekday"
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+ "time_table")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song-data/A/A/A/*.json")
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table 
    df.createOrReplaceTempView("log_table")
    song_df.createOrReplaceTempView("song_table")
    
    songplays_table = spark.sql("""SELECT l.songplay_id as songplay_id,
                                            l.timestamp as start_time,
                                            l.userID as user_id,
                                            l.level as level,
                                            s.song_id as song_id,
                                            s.artist_id as artist_id,
                                            l.sessionId as session_id,
                                            l.location as location,
                                            l.userAgent as user_agent,
                                            year(l.timestamp)as year,
                                            month(l.timestamp) as month
                                    FROM log_table as l
                                    JOIN song_table as s
                                    ON (l.artist = s.artist_name) AND (l.song = s.title) AND (l.length = s.duration)
                                    WHERE l.userID IS NOT NULL
                                        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+ "songplay")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://my-data-lake-bucket1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
