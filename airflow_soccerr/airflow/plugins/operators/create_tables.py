from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableRedShiftOperator(BaseOperator):

    ui_color = '#7A3D69'

    @apply_defaults
    def __init__(self,
                 # Defining operators params (with defaults)
                 table='',
                 redshift_conn_id='',
                 create_table_sql='',
                 *args, **kwargs):

        super(CreateTableRedShiftOperator, self).__init__(*args, **kwargs)
        # Mapping params to passed in values
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_table_sql = create_table_sql

    def execute(self, context):
        self.log.info(f'Creating {self.table} table in redshift')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshift.run(self.create_table_sql)