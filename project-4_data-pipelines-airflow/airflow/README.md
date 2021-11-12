# Sparkify ETL Pipeline
This code sets up a DAG for automating and monitoring the data warehouse ETL pipeline for Sparkify.

## Setup
The data to be processed are stored at 
	- event data: `s3://udacity-dend/log_data` 
    - song data: `s3://udacity-dend/song_data`
    
Once Airflow is launched, you will need to specify some connection variables. Namely, your Amazon Web Services connection will need to be specified with ID `aws_credentials`, and your PostGres connection will need to be specified as an Amazon Redshift endpoint with ID `redshift`. The Redshift Clsuter must be publicly accessible.

There are two DAGs specified:
	- `create_database` is a helper DAG which need only be run once. This needn't be a DAG, but is useful in that takes advantage of the specification of the connectors specified above. This DAG will delete any tables and recreate them if necessary.
    - `udac_example_dag` specifies the full ETL process
    
## Tables
The tables created are in a star schema. The fact table, songplays, contains the following data:

| column       | type        | Description                                                      |
|--------------|-------------|------------------------------------------------------------------|
| songplay_id  | BIGINT (pk) | An auto generated ID for the songplay entry                      |
| user_id      | INT         | A unique identifier for the listening user                       |
| song_id      | VARCHAR     | A unique identifier for the song being listened to               |
| artist_id    | VARCHAR     | A unique identifier for the artist of the song                   |
| itemInSession| INT         | The item number in the session                                   |
| session_id   | INT         | An identifier for the session                                    |
| userAgent    | VARCHAR     | A string describing the user agent used                          |
| ts           | BIGINT      | A timestamp for when the song begun being listened to            |
| location     | VARCHAR     | Where the user is listening                                      |
| session_level| VARCHAR     | What membership tier was the user on at the time of this session |

Additional context on the songplay record can be extracted by joining on the relevant dimension tables.
The dimension tables are:
**users**
| column        | type     | Description                          |
|---------------|----------|--------------------------------------|
| user_id       | INT (pk) | A unique identifier for the user     |
| firstName     | VARCHAR  | The user's first name                |
| surname       | VARCHAR  | The user's surname                   |
| gender        | VARCHAR  | The user's gender                    |
| current_level | VARCHAR  | The user's current membership status |

**songs**
| column    | type         | Description                         |
|-----------|--------------|-------------------------------------|
| song_id   | VARCHAR (pk) | An identifier for the song          |
| title     | VARCHAR      | The song's title                    |
| artist_id | VARCHAR      | An identifier for the song's artist |
| duration  | DECIMAL      | The length of the song in seconds   |
| year      | INT          | The year of the song's release      |

**artists**
| column           | type         | Description                  |
|------------------|--------------|------------------------------|
| artist_id        | VARCHAR (pk) | An identifier for the artist |
| artist_name      | VARCHAR      | The artist's name            |
| artist_latitude  | DECIMAL      | Where the artist is located  |
| artist_longitude | DECIMAL      | Where the artist is located  |
| artist_location  | VARCHAR      | Where the artist is located  |

**time**
| column     | type        | Description                                                        |
|------------|-------------|--------------------------------------------------------------------|
| start_time | BIGINT (pk) | A timestamp in EPOCH format relating to the start of a song listen |
| hour       | INT         | The hour of start_time                                             |
| day        | INT         | The day of the month of start_time                                 |
| week       | INT         | The week of the year of start_time                                 |
| weekday    | INT         | The day of the week (where Sunday = 0)                             |
| month      | INT         | The month of the year                                              |
| year       | INT         | The year                                                           |


## Data quality checks
Once data have been processed a data quality check operator, performs some checks on the processed data, ensuring there are no NULL values and also checking that user membership status only contains two distinct values. 

## OPTIONAL: Question for the reviewer
 
If you have any question about the starter code or your own implementation, please add it in the cell below. 

For example, if you want to know why a piece of code is written the way it is, or its function, or alternative ways of implementing the same functionality, or if you want to get feedback on a specific part of your code or get feedback on things you tried but did not work.

Please keep your questions succinct and clear to help the reviewer answer them satisfactorily. 

> **_Your question_**