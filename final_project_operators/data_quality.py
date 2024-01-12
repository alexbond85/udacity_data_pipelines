from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id='',
                 test_dicts=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_dicts = test_dicts
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('Running data quality checks')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.test_dicts:
            self.log.info('No tests to perform.')
        for test in self.test_dicts:
            compute_sql = test['compute_sql']
            expected = test['expected']
            self.log.info(f"Running: {compute_sql}")
            records = redshift_hook.get_records(compute_sql)
            result = records[0][0]

            if result != expected:
                raise ValueError(f"Data quality error. Expected: {expected_result}, Actual: {result}.")
