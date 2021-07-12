import boto3
import sqlite3
import os
import pandas as pd
from datetime import datetime, timedelta
import pyspark
import zipfile
from airflow import DAG

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable

from helpers import SqlQueries,DataQualitySqlQueries


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.create_tables import CreateTableRedShiftOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,  DoubleType,LongType
import pyspark
import pyspark
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import *
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'
from pyspark.sql import SQLContext
from pyspark import SparkContext

default_args = {
    'start_date': datetime(2021, 6, 17),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('soccerr_ETL',
          default_args=default_args,
          description='extract the zip file contents and wriite the files to s3',
          max_active_runs=1,
          schedule_interval='@weekly'
        )

def initialize():
    os.environ["AWS_ACCESS_KEY_ID"] = Variable.get("s3_access_key")
    os.environ["AWS_SECRET_ACCESS_KEY"] = Variable.get("s3_secret_acess_key")

    aws_hook = AwsHook("aws_credentials_id")
    credentials = aws_hook.get_credentials()

    os.environ["AWS_ACCESS_KEY_ID"] = credentials.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key
    #root=os.getcwd() 
    #Variable.set("root_dirs", root)
    #print(root)
    
start_operator = PythonOperator(
    task_id='start_operator',
    python_callable=initialize,
    dag=dag)

def s3_session():
    s3 = boto3.client('s3')
    return s3

def download_ZipFile(**kwargs):
    initialize()
    Objectpath= kwargs['Objectpath']
    localpath = kwargs['localpath']
    bucket=Variable.get("bucket_name")
    s3 = s3_session()
    s3.download_file(bucket,Objectpath, localpath)
    
def download_ZipFile_(Objectpath,localpath ):
    initialize()
    bucket=Variable.get("bucket_name")
    s3 = s3_session()
    s3.download_file(bucket,Objectpath, localpath)
    
download_ZipFile_from_s3 = PythonOperator(
    task_id='download_ZipFile_from_s3',
    python_callable=download_ZipFile,
    op_kwargs={
        "Objectpath" : 'sqllite_folder/soccerr_database.zip',
        "localpath" : Variable.get("root_dirs")+"/soccerr_database.zip"
    },
    dag=dag)


#download_ZipFile('sqllite_folder/soccerr_database.zip', Variable.get("root_dir")+"/soccerr_database.zip")      

def extract_zipFile(**kwargs):
    path= kwargs['path']
    extract_to = kwargs['extract_to']
    zip=zipfile.ZipFile(path)
    zip.extractall( extract_to)
    
def extract_zipFile_(path, extract_to):
    zip=zipfile.ZipFile(path)
    zip.extractall( extract_to)

extract_ZipFile = PythonOperator(
    task_id='extract_ZipFile',
    python_callable=extract_zipFile,
    op_kwargs={
        "path" : Variable.get("root_dirs")+"/soccerr_database.zip",
        "extract_to" : Variable.get("root_dirs")
    },
    dag=dag)
    

def upload_file_to_S3(**kwargs):
    initialize()
    objectpath= kwargs['Objectpath']
    localpath = kwargs['localpath']
    bucket_name=Variable.get("bucket_name")
    s3 = s3_session()
    s3.upload_file( localpath, bucket_name, objectpath)

def upload_file_to_S3_(localpath,objectpath ):
    initialize()
    bucket_name=Variable.get("bucket_name")
    s3 = s3_session()
    s3.upload_file( localpath, bucket_name, objectpath)

    
upload_sqlite_file_to_s3 = PythonOperator(
    task_id='upload_sqlite_file_to_s3',
    python_callable=upload_file_to_S3,
    op_kwargs={
        "Objectpath" : "sqllite_folder/database.sqlite",
        "localpath" : Variable.get("root_dirs")+'/database.sqlite'
    },
    dag=dag)

def connect_to_sqlite(path):
    return sqlite3.connect(path)

def create_spark_session():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    return sqlContext

# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df,sqlContext):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)

def write_table_data_to_s3(**kwargs):
    initialize()
    sqlContext=create_spark_session()
    tablename= kwargs['tablename']
    print(tablename)
    try:
        bucket_name=Variable.get("bucket_name")
        con = connect_to_sqlite(Variable.get("root_dirs")+'/database.sqlite')
        root= Variable.get("root_dirs") 
        filename= root + "/"+tablename+".csv"
        df_countries = pd.read_sql_query('Select * from '+tablename, con)
        print(df_countries.dtypes)
        df_countries=pandas_to_spark(df_countries,sqlContext)
        print(df_countries.printSchema())
        #df_countries.write.mode("overwrite").parquet('s3a://'+bucket_name+'/lake/'+ tablename)
        df_country=df_countries.toPandas()
        print(df_countries.dtypes)
        df_country.to_csv(filename, index=False, sep=";")
        upload_file_to_S3_(filename,"raw/"+tablename+".csv")

    except:
        download_ZipFile_('sqllite_folder/'+tablename+'soccerr_database.zip', Variable.get("root_dir")+tablename+"/soccerr_database.zip")     
        extract_zipFile_(Variable.get("root_dirs")+tablename+"/soccerr_database.zip",Variable.get("root_dir")+"/"+ tablename+"/")
        bucket_name=Variable.get("bucket_name")
        con = connect_to_sqlite(Variable.get("root_dir")+tablename+'/database.sqlite')
        root= Variable.get("root_dirs") 
        filename= root + "/"+tablename+".csv"
        df_countries = pd.read_sql_query('Select * from '+tablename, con)
        print(df_countries.dtypes)
        df_countries=pandas_to_spark(df_countries,sqlContext)
        print(df_countries.printSchema())
        #df_countries.write.mode("overwrite").parquet('s3a://'+bucket_name+'/lake/'+ tablename)
        df_country=df_countries.toPandas()
        print(df_countries.dtypes)
        df_country.to_csv(filename, index=False, sep=";")
        upload_file_to_S3_(filename,"raw/"+tablename+".csv")

        
write_file_to_csv_from_Player_Attributes = PythonOperator(
    task_id='write_file_to_csv_from_Player_Attributes',
    python_callable=write_table_data_to_s3,
    op_kwargs={
        "tablename" : "Player_Attributes"
    },
    dag=dag)

write_file_to_csv_from_Player = PythonOperator(
    task_id='write_file_to_csv_from_Player',
    python_callable=write_table_data_to_s3,
    op_kwargs={
        "tablename" : "Player"
    },
    dag=dag)

write_file_to_csv_from_Match = PythonOperator(
    task_id='write_file_to_csv_from_Match',
    python_callable=write_table_data_to_s3,
    op_kwargs={
        "tablename" : "Match"
    },
    dag=dag)

write_file_to_csv_from_League = PythonOperator(
    task_id='write_file_to_csv_from_League',
    python_callable=write_table_data_to_s3,
    op_kwargs={
        "tablename" : "League"
    },
    dag=dag)

write_file_to_csv_from_Country = PythonOperator(
    task_id='write_file_to_csv_from_Country',
    python_callable=write_table_data_to_s3,
    op_kwargs={
        "tablename" : "Country"
    },
    dag=dag)

write_file_to_csv_from_Team = PythonOperator(
    task_id='write_file_to_csv_from_Team',
    python_callable=write_table_data_to_s3,
    op_kwargs={
        "tablename" : "Team"
    },
    dag=dag)


write_file_to_csv_from_Team_Attributes = PythonOperator(
    task_id='write_file_to_csv_from_Team_Attributes',
    python_callable=write_table_data_to_s3,
    op_kwargs={
        "tablename" : "Team_Attributes"
    },
    dag=dag)


def CreatePointsTable(**kwargs):
    match_table_path= kwargs['match_table_path']
    league_table_path= kwargs['league_table_path']
    initialize()
    Points_table_path= kwargs['Points_table_path']
    sqlContext = create_spark_session()
    bucket_name = Variable.get("bucket_name")
    
    df_match=sqlContext.read.csv('s3a://'+bucket_name+'/'+ match_table_path,header=True, inferSchema=True)

    df_league=sqlContext.read.csv('s3a://'+bucket_name+'/'+league_table_path,header=True, inferSchema=True)

    df_match.createOrReplaceTempView("Match")
    df_league.createOrReplaceTempView("League")

    columns = ['match_api_id', 'home_team_goal', 'away_team_goal', 'season','result', 'name']

    matches_league = sqlContext.sql("""
                                Select m.match_api_id,
                                       m.home_team_goal,
                                       m.away_team_goal,
                                       m.season, 
                                       Case 
                                        When m.home_team_goal > m.away_team_goal Then 'Home'
                                        When m.home_team_goal < m.away_team_goal Then 'Away'
                                        ELSE 'Tie'
                                        END AS result,
                                l.name from Match m
                                JOIN League l ON l.id = m.league_id""").toDF(*columns)
    
    matches_league.write.mode("overwrite").parquet('s3a://'+bucket_name+'/'+ Points_table_path)

Create_points_table_from_match_and_league_and_store_it_in_s3 = PythonOperator(
    task_id='Create_points_table_from_match_and_league_and_store_it_in_s3',
    python_callable=CreatePointsTable,
    op_kwargs={
        'match_table_path':'raw/Match.csv',
        'league_table_path': 'raw/League.csv',
        'Points_table_path': 'lake/points'
    },
    dag=dag)

load_country_to_redshift = StageToRedshiftOperator(
    task_id='Load_Country',
    table="Country",
    s3_key = "raw/Country.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    extrasql="TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 delimiter as ';' ",
    dag=dag)

load_league_to_redshift = StageToRedshiftOperator(
    task_id='Load_League',
    table="League",
    s3_key = "raw/League.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    extrasql="TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 delimiter as ';' ",
    dag=dag
)




load_player_to_redshift = StageToRedshiftOperator(
    task_id='Load_Player',
    table="Player",
    s3_key = "raw/Player.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    extrasql="TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 delimiter as ';' ",
    aws_credential_id="aws_credentials",
    dag=dag)


load_player_attributes_to_redshift = StageToRedshiftOperator(
    task_id='Load_Player_attributes',
    table="Player_Attributes",
    s3_key = "raw/Player_Attributes.csv",
    file_format="CSV",
    extrasql="TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 delimiter as ';' ",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag)

load_team_to_redshift = StageToRedshiftOperator(
    extrasql="TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 delimiter as ';' ",
    task_id='Load_Team',
    table="Team",
    s3_key = "raw/Team.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag)


load_team_attributes_to_redshift = StageToRedshiftOperator(
    extrasql="TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 delimiter as ';' ",
    task_id='Load_team_attributes',
    table="Team_Attributes",
    s3_key = "raw/Team_Attributes.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag)


load_match_to_redshift = StageToRedshiftOperator(
    extrasql="TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL IGNOREHEADER 1 delimiter as ';' ",
    task_id='Load_match',
    table="Match",
    s3_key = "raw/Match.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag)

load_point_to_redshift = StageToRedshiftOperator(
    task_id='Load_point',
    table="Point",
    s3_key = "lake/Point.Paraquet",
    file_format="Paraquet",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_queries=DataQualitySqlQueries.dq_checks)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> download_ZipFile_from_s3
download_ZipFile_from_s3 >> extract_ZipFile

extract_ZipFile>> upload_sqlite_file_to_s3 
extract_ZipFile >> write_file_to_csv_from_Match
extract_ZipFile >> write_file_to_csv_from_Team_Attributes
extract_ZipFile >> write_file_to_csv_from_Team
extract_ZipFile >> write_file_to_csv_from_Country
extract_ZipFile >> write_file_to_csv_from_Player_Attributes
extract_ZipFile >> write_file_to_csv_from_Player
extract_ZipFile >> write_file_to_csv_from_League

upload_sqlite_file_to_s3 >> Create_points_table_from_match_and_league_and_store_it_in_s3
write_file_to_csv_from_Match >> Create_points_table_from_match_and_league_and_store_it_in_s3
write_file_to_csv_from_Team_Attributes >> Create_points_table_from_match_and_league_and_store_it_in_s3
write_file_to_csv_from_Team >> Create_points_table_from_match_and_league_and_store_it_in_s3
write_file_to_csv_from_Country >> Create_points_table_from_match_and_league_and_store_it_in_s3
write_file_to_csv_from_Player_Attributes >> Create_points_table_from_match_and_league_and_store_it_in_s3
write_file_to_csv_from_Player >> Create_points_table_from_match_and_league_and_store_it_in_s3
write_file_to_csv_from_League >> Create_points_table_from_match_and_league_and_store_it_in_s3

Create_points_table_from_match_and_league_and_store_it_in_s3 >> load_player_to_redshift
Create_points_table_from_match_and_league_and_store_it_in_s3 >> load_team_to_redshift
Create_points_table_from_match_and_league_and_store_it_in_s3 >> load_league_to_redshift
Create_points_table_from_match_and_league_and_store_it_in_s3 >> load_country_to_redshift


load_player_to_redshift >> load_match_to_redshift
load_team_to_redshift >> load_match_to_redshift
load_league_to_redshift >> load_match_to_redshift
load_country_to_redshift >> load_match_to_redshift

load_player_to_redshift >> load_player_attributes_to_redshift
load_team_to_redshift >> load_team_attributes_to_redshift

load_player_attributes_to_redshift >> load_point_to_redshift
load_team_attributes_to_redshift >> load_point_to_redshift
load_match_to_redshift >> load_point_to_redshift


load_point_to_redshift   >> run_quality_checks
run_quality_checks >> end_operator





