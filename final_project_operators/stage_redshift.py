from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)  # indicator that s3_key field should be templated


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        self.log.info('StageToRedshiftOperator is executing')
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info("Copying data from S3 to Redshift")
        # Use context to render the templated s3_key field
        rendered_key = self.s3_key.format(**context)

        copy_sql = f"""
            COPY {self.table}
            FROM 's3://{self.s3_bucket}/{rendered_key}'
            ACCESS_KEY_ID '{aws_connection.login}'
            SECRET_ACCESS_KEY '{aws_connection.password}'
            JSON '{self.json_path}'
            TIMEFORMAT as 'epochmillisecs'
            REGION as 'auto';  # Allows Redshift to automatically detect the S3 bucket's region
        """
        self.log.info(f"Executing COPY command for table {self.table}")
        redshift.run(copy_sql)
        self.log.info(f"COPY command complete for table {self.table}")