import boto3

from core.logger import log
from settings.envs import (
    AWS_KEY,
    AWS_REGION,
    AWS_SECRET,
    DWH_CLUSTER_IDENTIFIER,
    DWH_CLUSTER_TYPE,
    DWH_NODE_TYPE,
    DWH_NUM_NODES,
    DWH_DB_NAME,
    DWH_DB_PASSWORD,
    DWH_DB_USER,
)

logger = log.setup_custom_logger(__name__)


class RedshiftOperator:

    def __init__(self):

        self.dwh_cluster_id = DWH_CLUSTER_IDENTIFIER
        self.dwh_cluster_type = DWH_CLUSTER_TYPE
        self.dwh_db_name = DWH_DB_NAME
        self.dwh_db_user = DWH_DB_USER
        self.dwh_node_type = DWH_NODE_TYPE
        self.dwh_num_nodes = DWH_NUM_NODES
        self.client = self.create_redshift_client()

    @property
    def cluster_endpoint(self):

        return self.get_cluster_endpoint()

    @property
    def cluster_info(self):

        return self.get_cluster_info()

    @property
    def cluster_region(self):

        return self.get_current_region()

    @property
    def cluster_status(self):

        return self.get_cluster_status()

    def create_redshift_client(self):
        """
        Creates a Redshift client with the credentials declared in the
        application config files. The application uses this client to
        create and teardown AWS Redshift clusters.

        Returns:
            boto3.client
        """
        client = boto3.client(
            service_name='redshift',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET,
        )

        logger.info('Client created')

        return client

    def create_redshift_cluster(self, role_arn):
        """
        Creates a Redshift cluster with the configuration declared in the
        application config files. Upon creation, the application will wait
        until the cluster becomes available before proceeding with database
        operations.

        Args:
            role_arn (string): A manifest is a list, of dictionary objects
            containing the details of a task. These keyword arguments are
            passed to the task's parametrised SQL query which this function
            executes.

        Returns:
            None
        """
        if self.cluster_status == 'deleting':
            logger.info(
                f"Please wait, '{self.dwh_cluster_id}' is still deleting..."
            )
            self.wait_for_cluster()

        logger.info(f"Creating '{self.dwh_cluster_id}'")

        try:
            self.client.create_cluster(
                ClusterType=self.dwh_cluster_type,
                NodeType=self.dwh_node_type,
                NumberOfNodes=int(self.dwh_num_nodes),
                DBName=self.dwh_db_name,
                ClusterIdentifier=self.dwh_cluster_id,
                MasterUsername=self.dwh_db_user,
                MasterUserPassword=DWH_DB_PASSWORD,
                IamRoles=[role_arn],
                PubliclyAccessible=True,
            )
        except self.client.exceptions.ClusterAlreadyExistsFault:
            logger.info(
                f"'{self.dwh_cluster_id}' already exists!"
            )
        else:
            self.wait_for_cluster()
            self.get_cluster_endpoint()

        logger.info(f"'{self.dwh_cluster_id}' is available")

    def get_current_region(self):
        """
        Return the current region of the Redshift client. The result of this
        method is assigned as a property of this class.

        Returns:
            string
        """
        session = boto3.session.Session()
        current_region = session.region_name

        return current_region

    def get_cluster_endpoint(self):
        """
        Return the endpoint of the Redshift cluster created by the application.
        The result of this method is assigned as a property of this class. Take
        note of this endpoint to establish a connection to sparkifydb with your
        DBMS. The user information is declared in the config files.

        Returns:
            string
        """
        try:
            endpoint = self.cluster_info['Clusters'][0]['Endpoint']['Address']
        except TypeError:
            return None
        except self.client.exceptions.ClusterNotFoundFault:
            return None

        return endpoint

    def get_cluster_info(self):
        """
        Return a dictionary of the cluster information. The result of this
        method is assigned as a property of this class.

        Returns:
            json
        """
        try:
            info = self.client.describe_clusters(
                ClusterIdentifier=self.dwh_cluster_id,
            )
        except TypeError:
            return None
        except self.client.exceptions.ClusterNotFoundFault:
            return None

        return info

    def get_cluster_status(self):
        """
        Return the status of the cluster. This method is used to check the
        status of the cluster to determine whether the application should
        wait for availability.

        Returns:
            string
        """
        try:
            status = self.cluster_info['Clusters'][0]['ClusterStatus']
        except TypeError:
            return None
        except self.client.exceptions.ClusterNotFoundFault:
            return None

        return status

    def wait_for_cluster(self):
        """
        Conditionally waits for cluster availability. If the cluster is in the
        process of being created, the application will wait until it becomes
        available before proceeding. Likewise, if the cluster is being deleted,
        the application will wait until it is deleted before attempting to
        create it again.

        Returns:
            None
        """
        if self.cluster_status in ['creating', 'available']:
            waiter_type = 'cluster_available'
        else:
            waiter_type = 'cluster_deleted'

        logger.info(
            f"Waiting for '{self.dwh_cluster_id}'..."
        )

        waiter = self.client.get_waiter(waiter_type)
        waiter.wait(
            ClusterIdentifier=self.dwh_cluster_id,
            WaiterConfig={
                'Delay': 5,
                'MaxAttempts': 100,
            }
        )

    def delete_cluster(self):
        """
        Deletes the cluster created by the application. This method is invoked
        from the teardown() method which is executed when the application is
        in dry_run mode.

        Returns:
            json
        """
        try:
            response = self.client.delete_cluster(
                ClusterIdentifier=self.dwh_cluster_id,
                SkipFinalClusterSnapshot=True,
            )
        except self.client.exceptions.NoSuchEntityException:
            logger.info(f"Cluster '{self.dwh_cluster_id}' already deleted!")
        else:
            logger.info(f"Cluster '{self.dwh_cluster_id}' deleted")

            return response

    def teardown(self):
        """
        Invokes delete_cluster() to delete the cluster created by this
        application. This method is invoked when the application is in
        `dry_run` mode; the AWS infrastructure is torn down upon completion
        of the ETL process.

        Returns:
            None
        """
        self.delete_cluster()

        logger.info('Teardown complete')
