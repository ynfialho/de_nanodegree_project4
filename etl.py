import configparser
from datetime import datetime
import os
import gc

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime
from pyspark.sql import functions as f


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# Enable garbage collector
gc.enable()

def create_spark_session():
    """
    Spark session instance

    Detail: Set 30 partitions by default because of the challange data volume. 
            In production this value would be dynamic
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.sql.shuffle.partitions", 30)\
        .getOrCreate()
    return spark

@udf
def get_timestamp_sec(ts):
    """
    Function return timestamp in seconds
    Recive and return integer 
    """
    return ts//1000

def process_song_data(spark, input_data, output_data):
    """
        Description: This function loads song_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3
        
        Parameters:
            spark       : this is the Spark Session
            input_data  : the location of song_data from where the file is load to process
            output_data : the location where after processing the results will be stored
            
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # created song view to write SQL Queries
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT DISTINCT song.song_id, 
                            song.title,
                            song.artist_id,
                            song.year,
                            song.duration
                            FROM song_data_table song
                            WHERE song_id IS NOT NULL
                        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('append').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT song.artist_id, 
                                song.artist_name,
                                song.artist_location,
                                song.artist_latitude,
                                song.artist_longitude
                                FROM song_data_table song
                                WHERE song.artist_id IS NOT NULL
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode('append').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3. Also output from previous function is used in by spark.read.json command
        
        Parameters:
            spark       : this is the Spark Session
            input_data  : the location of song_data from where the file is load to process
            output_data : the location where after processing the results will be stored
            
    """
    # get filepath to log data file
    log_path = input_data + 'log_data/*.json'

    # read log data file
    df_crude = spark.read.json(log_path)
    
    # filter and create timestamp column by actions for song plays
    df = df_crude\
        .filter(df_crude.page == 'NextSong')\
        .withColumn("timestamp", from_unixtime(get_timestamp_sec(col("ts"))))
    
    # created log view to write SQL Queries
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT log.userId as user_id, 
                            log.firstName as first_name,
                            log.lastName as last_name,
                            log.gender as gender,
                            log.level as level,
                            log.timestamp
                            FROM log_data_table log
                            WHERE log.userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.mode('append').parquet(output_data+'users_table/')
    
    # extract columns to create time table
    time_table = spark.sql("""
                    SELECT DISTINCT
                        ts as start_time
                        , timestamp
                        , cast(date_format(timestamp, "HH") as INTEGER) as hour
                        , cast(date_format(timestamp, "dd") as INTEGER) as day
                        , weekofyear(timestamp) as week 
                        , month(timestamp) as month
                        , year(timestamp) as year
                        , dayofweek(timestamp) as weekday
                        FROM log_data_table
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('append').partitionBy("year", "month").parquet(output_data+'time_table/')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT DISTINCT log.ts as start_time,
                                month(timestamp) as month,
                                year(timestamp) as year,
                                log.userId as user_id,
                                log.level as level,
                                song.song_id as song_id,
                                song.artist_id as artist_id,
                                log.sessionId as session_id,
                                log.location as location,
                                log.userAgent as user_agent
                                FROM log_data_table log
                                JOIN song_data_table song
                                    on log.artist = song.artist_name 
                                    and log.song = song.title
                                    and log.length = song.duration
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('append').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    bucket_name = 'udacity-dend'
    output_prefix = 'datalake'
    input_data = "s3a://{}/".format(bucket_name)
    output_data = "s3a://{}/{}/".format(bucket_name, output_prefix)
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
