from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 select_data_statement,
                 table_name,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.select_data_statement = select_data_statement
        self.table_name = table_name

    def execute(self, context):
        self.log.info('Executing LoadFactOperator')
        redshift_hook = PostgresHook('redshift')

        self.log.info('Loading Redshift Fact Table')
        redshift_hook.run(SqlQueries.table_to_table_insert.format(self.table_name, self.select_data_statement))
