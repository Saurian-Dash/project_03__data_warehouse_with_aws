import logging
import psycopg2
import time

import core.logger.log as log

from core.queries.sql import (
    drop_table,
    list_tables,
)
from settings.envs import (
    AWS_REGION,
    DWH_DB_NAME,
    DWH_DB_PASSWORD,
    DWH_DB_PORT,
    DWH_DB_USER,
    DWH_DB_PUBLIC_VAULT,
    DWH_DB_RAW_VAULT,
)

logger = log.setup_custom_logger(__name__)


class PostgreSQLOperator:

    def __init__(self):

        self.aws_region = AWS_REGION
        self.cur = None
        self.conn = None
        self.dwh_db_name = DWH_DB_NAME
        self.dwh_db_port = DWH_DB_PORT
        self.dwh_db_user = DWH_DB_USER
        self.dwh_db_vaults = (DWH_DB_PUBLIC_VAULT, DWH_DB_RAW_VAULT)

    @property
    def dwh_db_tables(self):

        return self.get_tables()

    def create_connection(self, endpoint, autocommit=False):
        """
        Create connection to database endpoint with specified auto-commit
        settings. On connection, a cursor and connection object are attached
        as properties to this class.

        Args:
            endpoint (string): Endpoint of Redshift cluster to be accessed by
            the application. The cluster endpoint is attached to the Redshift
            client upon successful creation of a cluster.

            autocommit (boolean): Whether database changes should be committed
            automatically or not, this application uses manual commits.

        Returns:
            None
        """
        envs = {
            'host': endpoint,
            'dbname': self.dwh_db_name,
            'password': DWH_DB_PASSWORD,
            'port': self.dwh_db_port,
            'user': self.dwh_db_user,
        }
        try:
            self.conn = psycopg2.connect(**envs)
        except psycopg2.Error as e:
            raise e

        self.conn.set_session(autocommit=autocommit)
        self.cur = self.conn.cursor()

        logger.debug(
            f"Connected to host: {envs.get('host')}"
            f", database: {envs.get('dbname')}"
        )

    def execute_query(self, query, *args):
        """
        Execute a single SQL query with specified parameters. A None object is
        returned if the query does not retrieve records.

        Args:
            query (string): The SQL query to execute.

            args (tuple): A tuple containing a list of arguments to pass to a
            string formatted SQL query. This application favours parametrised
            SQL queries with keyword arguments, thus this argument is not used.

        Returns:
            tuple
        """
        try:
            self.cur.execute(query=query, vars=args)
            self.conn.commit()
        except psycopg2.Error as e:
            raise e

        try:
            result = self.cur.fetchall()
        except psycopg2.ProgrammingError:
            return None

        return result

    def get_tables(self):
        """
        Execute a SQL query to return a list of all tables in the database.

        Returns:
            list
        """
        tables = []

        for schema in self.dwh_db_vaults:
            result = self.execute_query(query=list_tables(schema=schema))
            if result:
                for row in result:
                    tables.append(row)

        return tables

    def setup_vaults(self, query):
        """
        Execute an SQL query to create a database schema for each of the data
        warehouse vaults associated with this class. Data warehouse vaults are
        defined in the application config files.

        Args:
            query (string): The SQL CREATE SCHEMA query to execute.

        Returns:
            None
        """
        for schema in self.dwh_db_vaults:
            self.execute_query(query=query(schema=schema))

            logger.info(f"Data warehouse vault '{schema}' created")

    def execute_tasks(self, manifest):
        """
        Execute a set of commands contained within a task in a manifest. A
        task is a dictionary containing the details of a database operation,
        this application uses these to carry out operations, such as creating
        tables or transforming data before loading to the dimensional model.

        Args:
            manifest (list): A manifest is a list, of dictionary objects
            containing the details of a task. These keyword arguments are
            passed to the task's parametrised SQL query which this function
            executes.

        Returns:
            None
        """
        for task in manifest:
            query = task['query']
            self.execute_query(query=query(**task))

            logger.info(
                f"Data warehouse task '{task['query'].__name__}' completed"
            )

    def drop_tables(self):
        """
        Iterate over all data warehouse tables and execute a DROP TABLE SQL
        query.

        Returns:
            None
        """
        for row in self.dwh_db_tables:
            schema, table = row
            self.execute_query(query=drop_table(schema=schema, table=table))

            logger.info(f"Data warehouse table '{schema}.{table}' dropped")

    def copy_s3_data(self, manifest, role_arn):
        """
        Execute a SQL query to copy raw data from S3 to staging tables in the
        Redshift cluster.

        Args:
            manifest (list): This application uses manifests, a manifest is a
            list, of dictionies containing the details of a task. These
            keyword arguments are passed to the task's parametrised SQL query
            which this function executes.

            role_arn: (string): The IAM role arn which enables the Redshift
            cluster to read from S3, this is available as a property of the
            IAMOperator.

        Returns:
            None
        """
        for task in manifest:

            start_time = time.time()
            query = task['query']

            logger.info(
                f"Copying S3 data from {task['bucket']} to '{task['vault']}"
                f".{task['table']}'"
            )

            self.execute_query(query=query(role_arn=role_arn, **task))

            end_time = round(time.time() - start_time, 2)
            logger.info(
                f"S3 data copied from {task['bucket']} to '{task['vault']}"
                f".{task['table']}' in {end_time} secs"
            )

    def close_connection(self):
        """
        Close current database session. This function is invoked at the end of
        the ETL operation.

        Returns:
            None
        """
        self.conn.close()
        logger.info('Connection closed')
