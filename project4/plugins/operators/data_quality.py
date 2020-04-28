from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    # empty_check expects a list of tables that should be checked for empty
    # null_check expects a list of {table: [column1, column2]} pairs for which a null check will be performed
    @apply_defaults
    def __init__(self,
                 empty_check=[],
                 null_check={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.empty_check = empty_check
        self_null_check = null_check

    def execute(self, context):
        self.log.info('Executing DataQualityOperator')

        for table in self.empty_check:
            self.log.info('Checking {} for emptiness').format(table)
            result = SqlQueries.table_count.format(table)
            row_count = result.fetch()[0]
            if row_count == 0:
                error_message = 'Table {} has no rows!'.format(table)
                self.log.error(error_message)
                raise ValueError(error_message)
            else:
                self.log.info('Table {} has {} rows'.format(table, row_count))

        for table, columns in self.null_check.items():
            for column in columns:
                self.log.info('Checking {}.{} for NULL values'.format(table, column))
                result = SqlQueries.null_count.format(table, column)
                null_count = result.fetch()[0]
                if null_count > 0:
                    error_message ='{}.{} has {} null values!'.format(table, column, null_count)
                    self.log.error(error_message)
                    raise ValueError(error_message)
                else:
                    self.log.info('{}.{} is free of null values, hurray!'.format(table, column))
