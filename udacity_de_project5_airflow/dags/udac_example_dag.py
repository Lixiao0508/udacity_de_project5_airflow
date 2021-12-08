# from datetime import datetime, timedelta
import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator) 
from helpers import SqlQueries

# AWS_KEY= os.environ.get('AWS_KEY')
# AWS_SECERET = os.environ.get('AWS_SECRET')


# dag_create_tables = DAG(
#     'create tables dag',
#     start_date=datetime.datetime.now()
# )


# default_args = {
# #     'owner': 'udacity',
#     start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
#     end_date=datetime.datetime(2018, 11, 30, 0, 0, 0, 0),
# }

dag = DAG('udac_example_dag',
#           default_args=default_args,
          start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
          end_date=datetime.datetime(2018, 11, 30, 0, 0, 0, 0),
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily', #'0 * * * *' #??
          max_active_runs=1
        )


#Operators/Tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# create_tables_task = PostgresOperator(
#     task_id="create_tables",
#     dag=dag_create_tables,
#     sql='create_tables.sql',
#     postgres_conn_id="redshift"
# )


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    
    #Give parameters to arguments that are used in StageToRedshiftOperator in stage_redshift.py
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table = "staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json
    copy_json_option='s3://udacity-dend/log_json_path.json'
    #"log_data/{execution_date.year}/{execution_date.month}/" 
    #"log_data/{execution_date.year}/{execution_date.month}/*.json" #execution_date = today's date?
    
)


# stage_songs_to_redshift = StageToRedshiftOperator( 
#     task_id='Stage_songs',
#     dag=dag,
    
#     #Give parameters to arguments that are used in StageToRedshiftOperator in stage_redshift.py
#     aws_credentials_id="aws_credentials",
#     redshift_conn_id="redshift",
#     table = "staging_songs",
#     s3_bucket="udacity-dend",
#     s3_key= "song_data" #song_data/A/*/*/*.json" 
# )


# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

# end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


#Task Dependencies 
#1. stage data to redshift
# start_operator >> create_tables_task 

# create_tables_task >> stage_events_to_redshift
# create_tables_task >> stage_songs_to_redshift


start_operator >> stage_events_to_redshift
# start_operator >> stage_songs_to_redshift


# #2.load data to fact table
# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table


# #3.load data to dim table
# load_songplays_table >> load_user_dimension_table
# load_songplays_table >> load_song_dimension_table
# load_songplays_table >> load_artist_dimension_table
# load_songplays_table >> load_time_dimension_table


# #4.data quality check
# load_user_dimension_table >> run_quality_checks
# load_song_dimension_table >> run_quality_checks
# load_artist_dimension_table >> run_quality_checks
# load_time_dimension_table >> run_quality_checks

# #5.end execution
# run_quality_checks >> end_operator
