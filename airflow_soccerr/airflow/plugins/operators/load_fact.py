from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    copy_sql = """
        INSERT INTO {} 
        ({})
        {};
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 fields="",
                 append_only="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
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
            
        formatted_sql = LoadFactOperator.copy_sql.format(
                self.table,
                self.fields,
                self.sql_query
            )   
        self.log.info("inserting data to Redshift table")
        redshift_hook.run(str(formatted_sql))