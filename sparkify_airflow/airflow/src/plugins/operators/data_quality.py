from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Run tests on redshift database
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
            self,
            conn_id,
            dq_checks,
            *args,
            **kwargs
    ):
        """Get connection and table names
        Args:
            conn_id(str): name of the connection
            dq_checks(list): list with format {"check_sql":sql_cmd, "expected_result":res},
            collecting sql commands to be executed and their expected result
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)

        error_count = 0
        failing_tests = []
        for check in self.dq_checks:
            # sample one test case
            sql_cmd = check.get('check_sql')
            exp_result = check.get('expected_result')
            # run tests
            redshift_hook.get_records(sql_cmd)
            records = redshift_hook.get_records(sql_cmd)[0]
            # collect errors and compare with expectation
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql_cmd)

        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
