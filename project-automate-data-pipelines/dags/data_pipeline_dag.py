from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from plugins.operators import (CreateTableOperator, StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from plugins.helpers import SqlQueries


s3_bucket = 'udacity-kndtran'
s3_events = 'log-data'
s3_songs = 'song-data/A/A/A'

# https://airflow.apache.org/docs/apache-airflow/1.10.3/_api/airflow/operators/index.html
default_args = {
    'owner': 'nugget',
    'start_date': datetime(2023, 6, 8),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False
}

dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE user_id is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE song_id is null", 'expected_result': 0}
    ]

with DAG('data_pipeline_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

    # Postgres only runs 1 query at a time.
    # This could be done with the PostgresOperator and run in parallel,
    # but there are too many tables.
    create_redshift_tables = CreateTableOperator(
        task_id='Create_tables',
        redshift_conn_id = "redshift",
        tables = ["staging_events", "staging_songs", \
                  "songplays", "users", "songs", "artists", "time"],
        dag=dag
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=s3_events,
        dag=dag
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=s3_songs,
        dag=dag
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.songplay_table_insert,
        table='songplays',
        mode='delete',
        dag=dag
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.user_table_insert,
        table='users',
        mode='delete',
        dag=dag
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.song_table_insert,
        table='songs',
        mode='delete',
        dag=dag
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.artist_table_insert,
        table='artists',
        mode='delete',
        dag=dag
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.time_table_insert,
        table='time',
        mode='delete',
        dag=dag
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = "redshift",
        dq_checks = dq_checks,
        dag=dag
    )

    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


    # Specify task dependencies
    start_operator >> create_redshift_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
        load_songplays_table >> \
            [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
            run_quality_checks >> end_operator
