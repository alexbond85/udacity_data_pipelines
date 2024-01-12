from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info(f'Start loading data into {self.table}')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_sql = f"INSERT INTO {self.table} {self.select_sql}"
        self.log.info(f"Running: {insert_sql}")
        redshift_hook.run(insert_sql)
        self.log.info(f"Finished: loading data into {self.table}")
