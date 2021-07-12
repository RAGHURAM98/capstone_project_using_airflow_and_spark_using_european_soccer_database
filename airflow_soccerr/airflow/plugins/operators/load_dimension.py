from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    copy_sql = """
        INSERT INTO {} 
        ({})
        {};
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 fields="",
                 sql_query="",
                 append_only="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.fields=fields
        self.sql_query = sql_query
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if not self.append_only:
            self.log.info("deleting data from Redshift table")
            redshift_hook.run("DELETE FROM {}".format(self.table))
        formatted_sql = LoadDimensionOperator.copy_sql.format(
                self.table,
                self.fields,
                self.sql_query
            )   
        self.log.info("inserting data to {}".format(self.table))
        redshift_hook.run(str(formatted_sql))