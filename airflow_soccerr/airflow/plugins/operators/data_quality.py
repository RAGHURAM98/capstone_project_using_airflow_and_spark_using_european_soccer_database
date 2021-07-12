from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_queries=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.sql_queries = sql_queries
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for query in self.sql_queries:
            self.log.info(str(query['check_sql']))
            records = redshift_hook.get_records(str(query['check_sql']))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed for {query['table']} returned no results")
            num_records = records[0][0]
            if num_records != int(query['expected_result']):
                raise ValueError(f"Data quality check failed for {query['table']}.actual result was {records[0][0]} but expected result was {query['expected_result']}")
            self.log.info(f"Data quality on table {query['table']} check passed with {records[0][0]} records")