# Data Lake with Amazon Spark and S3
---
A UDACITY *Data Engineering Nanodegree* Project

![udacity](https://www.udacity.com/images/svgs/udacity-tt-logo.svg)

## About The Project
------

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task here is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## The Files

The project contains three files.

##### 1. sparkify-notebook.ipynb
This notebook contains the entire ETL process starting with creating a spark session, loading the datasets from Amazon S3, processing the songs data and logs metadata, creating the required analytical tables in the dimensional model, and finally storing the data to parquet files back on S3 bucket.

##### 2. dl.cfg
Contains the required credentials, if used, to access the created Amazon EMR cluster.

##### 3. etl.py-
Also contains the whole ETL process as the notebook.


## How to Run The Notebook

1. First, create a EMR cluster with Spark on Hadoop YARN option.

2. second, create a notebook on the created cluster and use the code in the sparkify-notebook.ipynb


## Project Datasets
---

The two datasets reside in S3. Here are the S3 links for each:
* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`

### Song Dataset

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

`song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json`

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

`{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`


### Log Dataset

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

`log_data/2018/11/2018-11-12-events.json`
`log_data/2018/11/2018-11-13-events.json`

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![log-data](https://video.udacity-data.com/topher/2019/February/5c6c3ce5_log-data/log-data.png)

## Schema for Song Play Analysis

The data tables were designed as a star schema to optimize queries on song play analysis.

- One fact table `songplays` . It contains the metrics and data upon which the analytics team can understand what songs users are listening to.
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    
- Four dimension tables `songs`, `artists`, `users`, and `time`.

**users** - users in the app
        user_id, first_name, last_name, gender, level
        
**songs** - songs in music database
        song_id, title, artist_id, year, duration
        
**artists** - artists in music database
        artist_id, name, location, lattitude, longitude
        
**time** - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

![schema]([https://www.udacity.com/images/svgs/udacity-tt-logo.svg](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/38715/1584109948/Song_ERD.png))


## Conclusion
Now, Sparkify startup has their music streaming app data in *S3 bucket data lake* ready on demand whenever the analytics team wants to run their analysis.
