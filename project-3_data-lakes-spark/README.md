# Sparkify data lake
This code provides a means of moving the Sparkify data from a data warehouse to a data lake.

## Getting started
First off, please populate your AWS credentials in `dl.cfg`
Then create an S3 bucket in the associated AWS account and note its access point.

The data warehouse is assumed to comprise the following structure:
```
song_data/{A-Z}/{A-Z}/{A-Z}/*.json
log_data/{year}/{month}/*.json
```

Where the files stored in song_data outline each of the songs stored in the database partitioned by the first three characters of the song_id. The data in log_data outline user behaviour logs partitioned by year and month.

## Creating the data lake
The data lake can be constructed in the command line

```
python etl.py s3://input-data s3://output-data
```

This will create the following tables in ``s3://output-data`` 

## Fact Table
### songplays 
- records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, 
    - start_time, 
    - user_id, 
    - level, 
    - song_id, 
    - artist_id, 
    - session_id, 
    - location,
    - user_agent

## Dimension Tables
### users 
- users in the app
    - user_id, 
    - first_name, 
    - last_name, 
    - gender, 
    - level

### songs 
- songs in music database
    - song_id, 
    - title,
    - artist_id, 
    - year, 
    - duration

### artists 
- artists in music database
    - artist_id, 
    - name, 
    - location, 
    - lattitude,
    - longitude

### time 
- timestamps of records in songplays broken down into specific units
    - start_time, 
    - hour, 
    - day, 
    - week, 
    - month, 
    - year, 
    - weekday





## OPTIONAL: Question for the reviewer
 
If you have any question about the starter code or your own implementation, please add it in the cell below. 

For example, if you want to know why a piece of code is written the way it is, or its function, or alternative ways of implementing the same functionality, or if you want to get feedback on a specific part of your code or get feedback on things you tried but did not work.

Please keep your questions succinct and clear to help the reviewer answer them satisfactorily. 

> **_There is a suggestion in the code to recreate both a timestamp column and a datetime column. It's not entirely clear the distinction between these. I would typically consider a timestamp as an epoch timestamp, but that's already the format that the data are in._**

> **_It's suggested to partition the songplays table by year and month, although these columns are not asked for in the table. Is this a mistake or is there some means of partitioning by a derived variable (I assume not?)_**

> **_The practice of creating a separate table for derived variables for time seems a little odd to me. I'd imagine this isn't commonly done as it seems kind of wasteful to store an entire table of derived values. Am I correct in thinking this is just an exercise for us to get used to generating these derived variables?I understand that partitioning by year/month can be useful which may require storing these variables._**
