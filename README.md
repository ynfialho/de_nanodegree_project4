# Data Engineer Nanodegree - Data lake
This is a udacity's data engineer nanodegree project.

## About the project
This project aims to create a data lake that allows dimensional analysis of music played on a music streaming platform. This platform belongs to a fictional star, Sparkify, whose data comes from log files and song data sets.
To provide this analytical data was building an ETL pipeline that extracts its data from S3, processes it using Spark and loads the data back into S3 as a set of dimensional tables.

## Requirement
This project assumes that you have created IAM user who can read and write S3 buckets.

## Project template
* dl.cfg: contains AWS credentials
* etl.py: pipeline responsible for reading AWS S3 data, processing using Apache Spark framework and writing it back to S3
* README.md: this document

## Project Datasets
You'll be working with two datasets that reside in S3. Here are the S3 links for each:

Song data: `s3://udacity-dend/song_data`

Log data: `s3://udacity-dend/log_data`

### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
```bash
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.
```bash
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

## Run
```python
python etl.py
```

## Example queries
Data lake usage examples consider AWS Athena as a source of data availability

* Count of songs played in last 30 days
```sql
SELECT  
	sp.user_id,
	cast(t.timestamp as date) as dt_play,
	count(sp.song_id) count_songs
FROM songplays_table AS sp
JOIN time_table    AS t 
	ON t.start_time = sp.start_time
WHERE cast(t.timestamp as date) between date_add('day', -30, now()) and date_add('day', 0, now()) 
GROUP BY 1, 2
ORDER BY 2
```
