from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 is_append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.is_append = is_append

    def _sql_statement(self):
        insert_sql = f"INSERT INTO {self.table} {self.select_sql};"
        truncate_sql = f"TRUNCATE TABLE {self.table};"
        if self.is_append:
            return insert_sql
        else:
            return truncate_sql + insert_sql

    def execute(self, context):
        self.log.info(f"Loading data into {self.table}.")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql_statement = self._sql_statement()
        self.log.info(f"Running : {sql_statement}")
        redshift.run(sql_statement)
        self.log.info(f"Finished data into {self.table}.")



