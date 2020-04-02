Sparkify Data Warehousing on Redshift
=====================================
Welcome to the Sparkify Data Warehousing project. This project was created to be able to use Sparkify's log data (event log and song log) in analytics and BI ventures. Redshift was chosen for the data warehouse, as it allows parallelization, a PostGreSQL like syntax, and can read from AWS S3, which is where our log data is stored.

The ETL process in this project occurs in 2 steps:
- 1. The raw data is read from S3 into Redshift into two staging tables. This data is copied as is, allowing for future analytics ventures to transform the data as they see fit.
  - This data is stored in S3:
    - `s3://udacity-dend/log-data`
    - `s3://udacity-dend/song-data1
- 2. Once the data is in Redshift, it is converted using a SQL ETL process into analytics tables on the same Redshift cluster.

Analytics
---------
The analytics tables are organized in the form of a star schema: The `songplays` table serves as the fact table in the center of the star, while the `users`, `artists`, `songs`, and `time` tables are dimension tables that can be used in conjunction with the `songplays` table. This allows us to ask questions centered around songs users are playing, e.g.
- ##### What are the most-played artists?
  ```sql
  SELECT COUNT(*)
  FROM songplays
  JOIN artists ON artists.artist_id = songplays.artist_id
  GROUP BY artists.artist_id
  ORDER BY COUNT(*) DESC
  LIMIT 10
  ```
- ##### When are users the most active?
  ```sql
  SELECT COUNT(*)
  FROM songplays
  JOIN time ON songplays.start_time = artist.start_time
  GROUP BY hour
  ORDER BY COUNT(*) DESC
  LIMIT 10
  ```

Setup
-----
- Please set `AWS_KEY` and `AWS_SECRET` in your environment to be able to run the ETL.
- If you are planning on using `redshift_iac.py` to set up Redshift on AWS, please set `IPV4_ADDRESS` to the CIDR you are going to be interacting with the Redshift cluster from, in the format `0.0.0.0/0`
- If you are setting up Redshift manually, please set the following  variables in the config:
- - `IAM/REDSHIFT_ARN` should be set to the IAM Role Arn that is attached to Redshift and has at least S3ReadOnly access
- - `REDSHIFT/ENDPOINT` should be set to the given endpoint for your Redshift cluster
- - `REDSHIFT/VPC_ID` should be set to the ID of the VPC the Redshift cluster is part of (usually the default VPC)

Process
-------

- Run `redshift_iac.py` to create Redshift cluster and IAM role on AWS
- Run `create_tables.py` to create staging and analytics tables in Redshift
- Run `etl.py` to load data from S3 into the staging tables, and then into the analytics tables



The following variables are stored in the environment to avoid security issues:
- `AWS_KEY`
- `AWS_SECRET`
- `IPV4_ADDRESS`
