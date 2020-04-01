Redshift Data Warehousing
=========================

Setup
-----
- Please set `AWS_KEY` and `AWS_SECRET` in your environment to be able to run the codebase.
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
- AWS_KEY
- AWS_SECRET
- IPV4_ADDRESS

TODO
----
- Make sure setting environment variables from Python is working as expected
- Think about adding SortKeys
