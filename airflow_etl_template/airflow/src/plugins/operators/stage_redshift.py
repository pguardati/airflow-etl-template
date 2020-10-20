from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """Transfer data from S3 into Redshift staging tables"""
    ui_color = '#358140'

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id,
            aws_credentials_id,
            table,
            s3_path,
            json_format,
            *args,
            **kwargs
    ):
        """Get connection profile and the command to transfer the data
        Args:
            redshift_conn_id(str): name of connection of redshift in airflow's database
            aws_credentials_id (str): name of the connection of aws in airflow's database
            table(str): name of the redshift table where to transfer the data
            s3_path(str): name of the s3 path where to pick the source data
            json_format(str): how to parse the data - 'auto' or a path to a jsonpath
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.json_format = json_format

    def execute(self, context):
        self.log.info("Copying data from s3 to redshift..")
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run("""
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        """.format(
            self.table,
            self.s3_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.json_format
        ))
