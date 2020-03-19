# Sparkify Song Database
Welcome to the Sparkify song database repo. This project was created in order to serve business analytics: We want to gain insight into user patterns with regards to songs. More specifically, we are looking to analyze information related to song plays - What are users listening to, when are they listening to, who are they listening to, etc. Raw data is currently stored AS JSON. This project takes the raw JSON and through and ETL process loads it into a queryable PostGreSQL database.

## Schema Design
The database schema was set up in the form of a Star Schema. The `songplays` table is the center of the star; since the business need is to gain insight around song plays, it makes sense that `songplays` is the nexus. From `songplays`, end users are able to pull in various data about the songs, users, and artists, as related to song plays.

## Sample Queries
