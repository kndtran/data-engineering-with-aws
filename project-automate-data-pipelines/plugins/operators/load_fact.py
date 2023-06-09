from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                sql="",
                table="",
                mode="delete",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.mode = mode

    def execute(self, context):
        self.log.info('LoadFactOperator: load data from staging to fact table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == 'append':
            sql = self.sql
        elif self.mode == 'delete':
            sql = f"truncate table {self.table}; " + self.sql
        redshift.run(sql)
