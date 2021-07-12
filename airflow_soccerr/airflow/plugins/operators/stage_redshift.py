from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
import os



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS {}
        {}
        ;
    """


    @apply_defaults
    def __init__(self,
                 # Defining operator parameters here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket=Variable.get("bucket_name"),
                 s3_key="",
                 extrasql="",
                 region="us-east-1",
                 file_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.file_format = file_format
        self.s3_bucket= s3_bucket
        self.s3_key =s3_key
        self.extrasql=extrasql

    def execute(self, context):
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        s3_path = "s3://{}/lake/{}".format(self.s3_bucket, self.s3_key)
        os.environ["AWS_ACCESS_KEY_ID"] = Variable.get("s3_access_key")
        os.environ["AWS_SECRET_ACCESS_KEY"] = Variable.get("s3_secret_acess_key")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info("Deleting data from destination Redshift table")
        redshift_hook.run("DELETE FROM {}".format(self.table))
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.file_format,
                self.extrasql
            )
        self.log.info("Copying data from S3 to Redshift")
        redshift_hook.run(formatted_sql)