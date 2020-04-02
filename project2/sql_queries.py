import configparser
import os

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events (
artist varchar,
auth varchar,
firstName varchar,
gender char,
itemInSession integer,
lastName varchar,
length numeric,
level varchar,
location varchar,
method varchar,
page varchar,
registration numeric,
sessionId integer,
song varchar,
status integer,
ts varchar,
userAgent varchar,
userId integer
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
song_id varchar,
num_songs integer,
title varchar,
artist_name varchar(max),
artist_latitude numeric,
year integer,
duration numeric,
artist_id varchar,
artist_longitude numeric,
artist_location varchar(max)
)
""")

user_table_create = ("""
CREATE TABLE users (
user_id integer NOT NULL UNIQUE,
first_name varchar,
last_name varchar,
gender char(1),
level varchar
-- PRIMARY KEY(user_id)
)
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id varchar NOT NULL UNIQUE,
name varchar NOT NULL,
location varchar,
latitude numeric,
longitude numeric
-- PRIMARY KEY(artist_id)
)
""")

song_table_create = ("""
CREATE TABLE songs (
song_id varchar NOT NULL UNIQUE,
title varchar NOT NULL,
artist_id varchar NOT NULL,
year integer,
duration numeric
-- PRIMARY KEY(song_id),
-- FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE time (
start_time timestamp NOT NULL,
hour integer NOT NULL,
day integer NOT NULL,
week integer NOT NULL,
month integer NOT NULL,
year integer NOT NULL,
weekday integer NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
songplay_id integer IDENTITY(0, 1) NOT NULL UNIQUE,
start_time timestamp,
user_id integer NOT NULL,
level varchar,
song_id varchar NOT NULL,
artist_id varchar NOT NULL,
session_id integer NOT NULL,
location varchar,
user_agent varchar
-- PRIMARY KEY(songplay_id),
-- FOREIGN KEY(user_id) REFERENCES users(user_id),
-- FOREIGN KEY(song_id) REFERENCES songs(song_id)
)
""")


# STAGING TABLES

IAM_ROLE = config['IAM']['REDSHIFT_ARN']

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
staging_events_copy = ("""
COPY staging_events
FROM '{}'
IAM_ROLE '{}'
JSON '{}'
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)


SONG_DATA = config['S3']['SONG_DATA']
staging_songs_copy = ("""
COPY staging_songs FROM
'{}'
IAM_ROLE '{}'
JSON 'auto'
TRUNCATECOLUMNS
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

# songplay_id will be automatically inserted since its an IDENTITY column
songplay_table_insert = ("""
INSERT INTO songplays (start_time,
                       user_id,
                       level,
                       song_id,
                       artist_id,
                       session_id,
                       location,
                       user_agent)
SELECT '1970-01-01'::date + CAST(ts AS bigint)/1000.0 * interval '1 second' AS start_time,
       userId AS user_id,
       level,
       song_id,
       artist_id,
       sessionId AS session_id,
       location,
       userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss
ON se.artist = ss.artist_name AND se.song = ss.title
WHERE page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId AS user_id,
       firstName AS first_name,
       lastName AS last_name,
       gender,
       level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id,
       artist_name AS name,
       artist_location AS location,
       artist_latitude AS latitude,
       artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
select '1970-01-01'::date + CAST(ts AS bigint)/1000.0 * interval '1 second' AS start_time,
       DATE_PART('hour', TIMESTAMP start_time) AS hour
       DATE_PART('day', TIMESTAMP start_time) AS day,
       DATE_PART('week', TIMESTAMP start_time) AS week,
       DATE_PART('month', TIMESTAMP start_time) AS year,
       EXTRACT(DOW FROM TIMESTAMP start_time) AS weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
