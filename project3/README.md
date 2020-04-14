Sparkify Data Lake
=========

Welcome to the Sparkify Data Lake project. We here at Sparkify love data. Data about our
users allows us to handcraft features and updates that meet our users' needs. To that end,
this data lake seeks to pool information from one of our most important datastreams (song plays).

Additionally, the project includes an ETL process to create a star schema out of the raw data,
demonstrating just one of the myriad ways the data can be consumed.

Storage & ETL
-------------
The data lake lives entirely in the AWS cloud:
- Raw data from our music streaming application is aggregated into S3 buckets.
- That data is then extracted and transformed by an EMR Cluster using Apache Spark
- The transformed data is then loaded back into different S3 buckets, ready for consumption.

We have found that S3 to EMR transfers are very network-efficient, reducing a lot of the latency
usually seen with processing data of our magnitude (on the order of 10s of gigabytes per day)

Star Schema
-----------
The transformed data from the work done by the EMR cluster is stored in parquet files, which
have been arranged in a traditional RBDMS star schema format:
- The center of the star is the songplays table - That is the primary axis along which consumers will ask questions
- There are then 4 "points" around the star, which can be joined with the songplays to ask specific questions centered around songplays:
- - users
- - artists
- - songs
- - time
