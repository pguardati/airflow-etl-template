import configparser
from airflow import settings
from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook

from sparkify_airflow.constants import CONFIG_PATH_DWH_CURRENT, logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def create_conn(
        conn_id,
        conn_type,
        host=None,
        schema=None,
        login=None,
        password=None,
        port=None
):
    """Create connection profile in airflow database
    Args:
        conn_id (str): name of the connection profile
        conn_type (str): name of a connection type ex: Amazon Web Services
        host(str): ip address of the connection
        schema(str): database name of the connection
        login(str): username of the connection
        password(str): password associated to username
        port(str): port of access of the connection
    """
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        schema=schema,
        login=login,
        password=password,
        port=port
    )
    session = settings.Session()
    logger.info("Checking profile does not exists..")
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    if str(conn_name) == str(conn_id):
        logger.info(f"Connection {conn_id} already exists")
        return
    session.add(conn)
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f'Connection {conn_id} is created')


def main():
    """Setup aws and redshift connection profiles i
    in airflow database using config files"""

    logger.info("Reading configuration from file..")
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH_DWH_CURRENT)
    key = config.get("AWS", "KEY")
    secret = config.get("AWS", "SECRET")
    host = config.get('CLUSTER', 'HOST')
    db_name = config.get("CLUSTER", "DB_NAME")
    db_user = config.get("CLUSTER", "DB_USER")
    db_password = config.get("CLUSTER", "DB_PASSWORD")
    db_port = config.get("CLUSTER", "DB_PORT")

    logger.info("Creating connection profiles..")
    create_conn(
        conn_id="aws_credentials",
        conn_type="Amazon Web Services",
        login=key,
        password=secret,
    )
    create_conn(
        conn_id="redshift",
        conn_type="Postgres",
        host=host,
        schema=db_name,
        port=db_port,
        login=db_user,
        password=db_password
    )


if __name__ == "__main__":
    main()
