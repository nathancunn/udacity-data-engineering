# Sparkify data warehouse creator

These files enable the setup of a Redshift cluster, and the population of a star schema database for performing analysis on the songplay data.


## Steps
- 1. Populate the location of the S3 bucket your data are stored in into dwh.cfg
- 2. Create a new IAM user in your AWS account and give it AdministratorAccess
- 3. Populate the values in the dwh.cfg. If you do not have a Redshift cluster set up, run the notebook `cluster_setup.ipynb` which will populate the values under [IAM_ROLE]
- 4. Run the Python files `create_tables.py` to instantiate the tables, and `etl.py` to populate them.
- 5. Explore the data using SQL queries. Sample queries can be seen in `example_queries.ipynb`

## Tables
The tables created are in a star schema. The fact table, songplay, contains the following data:

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
**user**
| column        | type     | Description                          |
|---------------|----------|--------------------------------------|
| user_id       | INT (pk) | A unique identifier for the user     |
| firstName     | VARCHAR  | The user's first name                |
| surname       | VARCHAR  | The user's surname                   |
| gender        | VARCHAR  | The user's gender                    |
| current_level | VARCHAR  | The user's current membership status |

**song**
| column    | type         | Description                         |
|-----------|--------------|-------------------------------------|
| song_id   | VARCHAR (pk) | An identifier for the song          |
| title     | VARCHAR      | The song's title                    |
| artist_id | VARCHAR      | An identifier for the song's artist |
| duration  | DECIMAL      | The length of the song in seconds   |
| year      | INT          | The year of the song's release      |

**artist**
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


## OPTIONAL: Question for the reviewer
 
If you have any question about the starter code or your own implementation, please add it in the cell below. 

For example, if you want to know why a piece of code is written the way it is, or its function, or alternative ways of implementing the same functionality, or if you want to get feedback on a specific part of your code or get feedback on things you tried but did not work.

Please keep your questions succinct and clear to help the reviewer answer them satisfactorily. 

> **_Your question_**