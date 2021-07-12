from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.create_tables import CreateTableRedShiftOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries,DataQualitySqlQueries

default_args = {
    'start_date': datetime(2021, 6, 17),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('soccer_create_table',
          default_args=default_args,
          description='Create tables in Redshift with Airflow',
          max_active_runs=1,
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

## Creating tables first in Redshift
League_table_create = CreateTableRedShiftOperator(
    task_id = 'create_Leagues_table',
    dag = dag,
    table = 'League',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.League_table_create
)

player_table_create = CreateTableRedShiftOperator(
    task_id = 'create_players_table',
    dag = dag,
    table = 'Player',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.Player_table_create
)

Player_atrributes_table_create = CreateTableRedShiftOperator(
    task_id = 'create_Player_Attributes_table',
    dag = dag,
    table = 'Player_Attributes',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.Player_atrributes_table_create
)

Team_table_create = CreateTableRedShiftOperator(
    task_id = 'create_team_table',
    dag = dag,
    table = 'Team',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.Team_table_create
)

Team_Attributes_table_create = CreateTableRedShiftOperator(
    task_id = 'create_Team_Attributes_table',
    dag = dag,
    table = 'Team_Attributes',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.Team_Attributes_table_create
)

Match_table_create = CreateTableRedShiftOperator(
    task_id = 'create_Match_table',
    dag = dag,
    table = 'Match',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.Match_table_create
)

Country_table_create = CreateTableRedShiftOperator(
    task_id = 'create_Country_table',
    dag = dag,
    table = 'Country',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.Country_table_create
)

point_table_create = CreateTableRedShiftOperator(
    task_id = 'point_table_create',
    dag = dag,
    table = 'Point',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.point_table_create
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Setup Task Dependacies

#start_operator >> ( Country_table_create, Team_table_create, player_table_create, League_table_create  ) >> (Player_atrributes_table_create, Team_Attributes_table_create, Match_table_create) >> point_table_create >> end_operator


start_operator >> Country_table_create
start_operator >> Team_table_create,
start_operator >> player_table_create
start_operator >> League_table_create


player_table_create >> Player_atrributes_table_create
Team_table_create >> Team_Attributes_table_create

Team_Attributes_table_create >> point_table_create
Player_atrributes_table_create >> point_table_create

Team_table_create >> Match_table_create
Country_table_create >> Match_table_create
player_table_create >> Match_table_create
League_table_create >> Match_table_create

Match_table_create >> point_table_create
point_table_create >> end_operator
