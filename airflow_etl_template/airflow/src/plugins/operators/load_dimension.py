from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """Transfer data from a staging table into a dimension table of the star schema
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
            self,
            conn_id,
            table,
            sql_copy_cmd,
            truncate=True,
            *args,
            **kwargs
    ):
        """Get connection profile, table name and command to transfer the data
        Args:
            conn_id(str): name of connection profile
            table(str): name of destination table
            sql_copy_cmd(str): sql command to transfer the data
            truncate(bool): if True, delete data before insertion, else, append them
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql_copy_cmd = sql_copy_cmd
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        if self.truncate:
            self.log.info("Deleting current data before loading..")
            redshift_hook.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info("Loading data from staging tables into {} table..".format(self.table))
        redshift_hook.run(self.sql_copy_cmd)
