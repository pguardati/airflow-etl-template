from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """Transfer data from a staging table into a fact table of the star schema
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
            self,
            conn_id,
            table,
            sql_copy_cmd,
            *args,
            **kwargs
    ):
        """Get connection profile, table name and command to transfer the data
        Args:
            conn_id(str): name of connection profile
            table(str): name of destination table
            sql_copy_cmd(str): sql command to transfer the data
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql_copy_cmd = sql_copy_cmd

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        self.log.info("Loading data from staging tables into {} table..".format(self.table))
        redshift_hook.run(self.sql_copy_cmd)
