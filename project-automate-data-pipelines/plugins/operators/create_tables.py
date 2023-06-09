from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers import SqlQueries

class CreateTableOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables,
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        # Postgres only runs 1 query at a time
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f'CreateTableOperator: creating table {table}')
            
            # Setup the queries
            if table == "staging_events":
                sql_drop_table = SqlQueries.staging_events_table_drop
                sql_create_table = SqlQueries.staging_events_table_create
            elif table == "staging_songs":
                sql_drop_table = SqlQueries.staging_songs_table_drop
                sql_create_table = SqlQueries.staging_songs_table_create
            elif table == "songplays":
                sql_drop_table = SqlQueries.songplay_table_drop
                sql_create_table = SqlQueries.songplay_table_create
            elif table == "users":
                sql_drop_table = SqlQueries.user_table_drop
                sql_create_table = SqlQueries.user_table_create
            elif table == "songs":
                sql_drop_table = SqlQueries.song_table_drop
                sql_create_table = SqlQueries.song_table_create
            elif table == "artists":
                sql_drop_table = SqlQueries.artist_table_drop
                sql_create_table = SqlQueries.artist_table_create
            elif table == "time":
                sql_drop_table = SqlQueries.time_table_drop
                sql_create_table = SqlQueries.time_table_create
            else:
                raise ValueError(f"Table not supported: {table}")

            # Remove old tables
            redshift.run(sql_drop_table)

            # Create the table
            redshift.run(sql_create_table)
