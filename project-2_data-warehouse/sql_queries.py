import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')


# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events"'
staging_songs_table_drop = 'DROP TABLE IF EXISTS "staging_songs"'
songplay_table_drop = 'DROP TABLE IF EXISTS "songplay"'
user_table_drop = 'DROP TABLE IF EXISTS "users"'
song_table_drop = 'DROP TABLE IF EXISTS "song"'
artist_table_drop = 'DROP TABLE IF EXISTS "artist"'
time_table_drop = 'DROP TABLE IF EXISTS "time"'

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS "staging_events" (
event_id BIGINT IDENTITY(0,1) PRIMARY KEY,
artist VARCHAR,
auth VARCHAR(25),
firstName VARCHAR(25),
gender VARCHAR(1),
itemInSession INT,
lastName VARCHAR(25),
length DECIMAL,
level VARCHAR(4),
location VARCHAR(255),
method VARCHAR(4),
page VARCHAR(25),
registration BIGINT,
sessionId BIGINT sortkey distkey,
song VARCHAR(255),
status INT,
ts BIGINT,
userAgent VARCHAR(255),
userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS "staging_songs" (
num_songs INT,
artist_id VARCHAR(25) NOT NULL PRIMARY KEY sortkey distkey,
artist_latitude DECIMAL(9),
artist_longitude DECIMAL(9),
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR(25),
title VARCHAR,
duration FLOAT,
year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS "songplay" (
songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
user_id INT NOT NULL,
song_id VARCHAR(25),
artist_id VARCHAR(25),
itemInSession INT,
session_id INT,
userAgent VARCHAR,
ts BIGINT,
location VARCHAR,
session_level VARCHAR(4) NOT NULL
) diststyle auto;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS "users" (
user_id INT PRIMARY KEY SORTKEY,
firstName VARCHAR(25) NOT NULL,
surname VARCHAR(25),
gender VARCHAR(1),
current_level VARCHAR(4)
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS "song" (
song_id VARCHAR(25) PRIMARY KEY SORTKEY,
title VARCHAR(500) NOT NULL,
artist_id VARCHAR(25) NOT NULL,
duration DECIMAL(9),
year INT
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS "artist" (
artist_id VARCHAR(25) PRIMARY KEY SORTKEY,
artist_name VARCHAR NOT NULL,
artist_latitude DECIMAL(9),
artist_longitude DECIMAL(9),
artist_location VARCHAR
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS "time" (
start_time BIGINT PRIMARY KEY SORTKEY,
hour INT,                                    
day INT,
week INT,
weekday INT,
month INT,
year INT
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY "staging_events" FROM {config.get("S3", "log_data")}
CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}'
format as json {config.get("S3", "log_jsonpath")}
REGION 'us-west-2'
""")

staging_songs_copy = (f"""
COPY "staging_songs" FROM {config.get("S3", "song_data")}
CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}'
format as json 'auto'
REGION 'us-west-2'
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO "songplay" (
    user_id,
    song_id,
    artist_id,
    itemInSession,
    session_id,
    userAgent,
    ts,
    location,
    session_level
)
SELECT 
events.userId,
songs.song_id,
songs.artist_id,
events.itemInSession,
events.sessionId,
events.userAgent,
events.ts,
events.location,
events.level
FROM staging_events AS events
LEFT JOIN staging_songs AS songs
ON events.song = songs.title AND events.artist = songs.artist_name
WHERE events.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO "users" (
user_id,
firstName,
surname,
gender,
current_level)
WITH users AS (
  /* Selecting only the most recent entry as we are
  storing users most recent membership level */
  SELECT *, row_number() 
  OVER (PARTITION BY userId ORDER BY ts DESC) AS ix
  FROM staging_events
  WHERE page = 'NextSong'
)
SELECT userid, firstname, lastname, gender, level FROM users WHERE ix = 1;
""")

song_table_insert = ("""
INSERT INTO "song" (
song_id,
title,
artist_id,
duration,
year)
WITH songs AS (
  SELECT *, row_number() 
  OVER (PARTITION BY song_id) AS ix
  FROM staging_songs
)
SELECT  song_id,
        title,
        artist_id,
        duration,
        year
FROM songs 
WHERE ix = 1;
""")

artist_table_insert = ("""
INSERT INTO "artist" (
artist_id,
artist_name,
artist_latitude,
artist_longitude,
artist_location
)
WITH artists AS (
  SELECT *, row_number() 
  OVER (PARTITION BY artist_id) AS ix
  FROM staging_songs
)
SELECT  artist_id,
        artist_name,
        artist_latitude,
        artist_longitude,
        artist_location
FROM artists 
WHERE ix = 1;
""")

time_table_insert = ("""
INSERT INTO "time" (
    start_time,
    hour,
    day,
    week,
    weekday,
    month,
    year)
SELECT 
    start_time,
    EXTRACT(hour FROM start_dt),
    EXTRACT(day FROM start_dt),
    EXTRACT(week FROM start_dt),
    EXTRACT(weekday FROM start_dt),
    EXTRACT(month FROM start_dt),
    EXTRACT(year FROM start_dt)
FROM (
SELECT
    DISTINCT(ts) AS start_time,
    timestamp 'epoch' + start_time / 1000 * interval '1 second' as start_dt
FROM staging_events)
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
