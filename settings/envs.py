import configparser as configparser


# read config file
config = configparser.ConfigParser()
config.read('settings/dwh.cfg')

# aws credentials
AWS_KEY = config.get('IAM', 'AWS_KEY')
AWS_REGION = config.get('IAM', 'AWS_REGION')
AWS_SECRET = config.get('IAM', 'AWS_SECRET')
AWS_DB_ROLE = config.get('IAM', 'AWS_DB_ROLE')

# redshift credentials
DWH_DB_USER = config.get('REDSHIFT', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('REDSHIFT', 'DWH_DB_PASSWORD')
DWH_DB_PORT = config.get('REDSHIFT', 'DWH_DB_PORT')
DWH_DB_NAME = config.get('REDSHIFT', 'DWH_DB_NAME')

DWH_CLUSTER_TYPE = config.get('REDSHIFT', 'DWH_CLUSTER_TYPE')
DWH_CLUSTER_IDENTIFIER = config.get('REDSHIFT', 'DWH_CLUSTER_IDENTIFIER')
DWH_NODE_TYPE = config.get('REDSHIFT', 'DWH_NODE_TYPE')
DWH_NUM_NODES = config.get('REDSHIFT', 'DWH_NUM_NODES')

# data warehouse vaults
DWH_DB_PUBLIC_VAULT = config.get('REDSHIFT', 'DWH_DB_PUBLIC_VAULT')
DWH_DB_RAW_VAULT = config.get('REDSHIFT', 'DWH_DB_RAW_VAULT')

# s3 data lake
S3_LOG_DATAPATH = config.get('S3', 'S3_LOG_DATAPATH')
S3_LOG_JSONPATH = config.get('S3', 'S3_LOG_JSONPATH')
S3_SONG_DATAPATH = config.get('S3', 'S3_SONG_DATAPATH')
