from core.logger import log
from core.manifests.copy_data import copy_data
from core.manifests.create_tables import create_tables
from core.manifests.data_modelling import transform_data
from core.operators.iam import IAMOperator
from core.operators.postgres import PostgreSQLOperator
from core.operators.redshift import RedshiftOperator
from core.queries.sql import (
    create_schema,
)

logger = log.setup_custom_logger(__name__)


def run(dry_run=True):
    """
    Orchestrates the application's "Operator" objects to create an AWS
    infrastructure and Redshift cluster. This function sets up all of the required
    AWS role permissions and spins up a Redshift cluster. Data is then loaded from
    S3 to staging tables, before it is cleaned and delivered to the dimensional
    model.

    Args:
        dry_run (bool): Set to True to teardown the Redshift cluster and AWS
        infrastructure upon completion. This argument should be passed in the
        terminal when executing the application (see app.py).

    Returns:
        None
    """

    logger.info('ETL operation starting')

    if dry_run:
        logger.info(
            'Dry run mode enabled, database will be torn down upon completion'
        )

    # instantiate operators
    iam = IAMOperator()
    red = RedshiftOperator()
    sql = PostgreSQLOperator()

    # setup aws infrastructure
    iam.create_role()
    iam.attach_role_policies()
    red.create_redshift_cluster(role_arn=iam.dwh_role_arn)

    # create postgresql connection
    sql.create_connection(endpoint=red.cluster_endpoint)

    # create data warehouse vaults
    sql.setup_vaults(query=create_schema)

    # drop existing tables
    sql.drop_tables()

    # create new tables
    sql.execute_tasks(manifest=create_tables)

    # load data to raw_vault tables
    sql.copy_s3_data(manifest=copy_data, role_arn=iam.dwh_role_arn)

    # clean and load data to public_vault tables
    sql.execute_tasks(manifest=transform_data)

    # close database connection
    logger.info(f'Cluster endpoint: {red.cluster_endpoint}')
    sql.close_connection()

    if dry_run:
        # teardown AWS infrastructure and Redshift cluster
        iam.teardown()
        red.teardown()

    logger.info('ETL operation completed')


if __name__ == '__main__':

    run()
