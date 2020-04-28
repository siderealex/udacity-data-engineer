from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 select_data_statement,
                 table_name,
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.select_data_statement = select_data_statement
        self.table_name = table_name
        self.truncate = truncate

    def execute(self, context):
        self.log.info('Executing LoadDimensionOperator')
        redshift_hook = PostgresHook('redshift')

        if truncate:
            self.log.info('Truncating {}'.format(self.table_name))
            redshift_hook.run(SqlQueries.table_truncate.format(self.table_name))

        self.log.info('Loading into Redshift Dimension {}'.truncate(self.table_name))
        redshift_hook.run(table_to_table_insert.format(self.table_name, self.select_data_statement))
