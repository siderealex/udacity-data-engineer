Redshift Data Warehousing
=========================


Process
-------

- Run `redshift_iac.py` to create Redshift cluster and IAM role on AWS
- Run `create_tables.py` to create staging and analytics tables in Redshift
- Run `etl.py` to load data from S3 into the staging tables, and then into the analytics tables



The following variables are stored in the environment to avoid security issues:
- REDSHIFT_ARN
- REDSHIFT_ENDPOINT
- AWS_KEY
- AWS_SECRET
- IPV4_ADDRESS
- VPC_ID