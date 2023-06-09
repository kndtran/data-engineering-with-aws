from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 dq_checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for dq_check in self.dq_checks:
            self.log.info(f'DataQualityOperator: running check SQL: {dq_check["check_sql"]}')
            records = redshift.get_records(dq_check['check_sql'])
            num_records = records[0][0]
            if num_records != dq_check['expected_result']:
                raise ValueError(f"Data quality check failed. Expected {dq_check['expected_result']} rows, got {num_records} rows.")
            self.log.info(f"Data quality check passed with expected {num_records} records")
