from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

# s3_bucket should be the Airflow variable name which points to the correct S3 endpoint
# AwsHook should be set in the Airflow configuration: 'aws_credentials' with an access key and secret_key
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 table_name,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.s3_bucket = s3_bucket
        self.table_name = table_name

    def execute(self, context):
        self.log.info('Executing StageToRedshiftOperator')
        aws_hook = AwsHook('aws_credentials')
        credentials = aws_hook.get_credentials()
        sql_stmt = SqlQueries.s3_copy.format(self.table_name, Variable.get(self.s3_bucket), credentials.access_key, credentials.secret_key)
        redshift_hook = PostgresHook('redshift')

        self.log.info('Copying from s3 bucket {} into Redshift table {}'.format(self.table_name, Variable.get(self.s3_bucket)))
        redshift_hook.run(sql_stmt)
