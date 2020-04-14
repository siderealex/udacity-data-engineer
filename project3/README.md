Data Lake
=========

Spark w/ AWS S3 package:
------------------------
`brew install apache-spark`
`PYTHON_HOME =`
`SPARK_HOME =`
`PATH =`
`./bin/spark-submit --packages  "org.apache.hadoop:hadoop-aws:2.7.3" etl.py`

Troubleshooting:
- Try https://github.com/databricks/spark-redshift/issues/244 if download issues
- AWS Credentials: