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
first_name varchar,
gender char,
last_name varchar,
level varchar,
location varchar,
session_id integer,
title varchar,
start_time timestamp,
user_id integer
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
artist_id varchar,
latitude integer,
longitude integer,
location varchar,
artist varchar,
song_id varchar,
title varchar,
duration numeric,
year integer
)
""")

user_table_create = ("""
CREATE TABLE users (
user_id integer NOT NULL UNIQUE,
first_name varchar,
last_name varchar,
gender char(1),
level integer
-- PRIMARY KEY(user_id)
)
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id varchar NOT NULL UNIQUE,
name varchar NOT NULL,
location varchar,
lattitude numeric,
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
weekday varchar NOT NULL
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










IAM_ROLE = os.environ['REDSHIFT_ARN']

# STAGING TABLES
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
staging_events_copy = ("""
COPY staging_events FROM
'{}'
IAM_ROLE '{}'
JSON '{}'
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)


SONG_DATA = config['S3']['SONG_DATA']
staging_songs_copy = ("""
COPY staging_songs FROM
'{}'
IAM_ROLE '{}'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
