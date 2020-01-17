from core.queries.sql import (
    copy_json_from_s3,
)
from settings.envs import (
    AWS_REGION,
    DWH_DB_PUBLIC_VAULT,
    DWH_DB_RAW_VAULT,
    S3_LOG_DATAPATH,
    S3_LOG_JSONPATH,
    S3_SONG_DATAPATH,
)


copy_data = [
    {
        "query": copy_json_from_s3,
        "bucket": S3_LOG_DATAPATH,
        "region": AWS_REGION,
        "vault": DWH_DB_RAW_VAULT,
        "table": "raw__log_data",
        "jsonpaths": S3_LOG_JSONPATH,
    },
    {
        "query": copy_json_from_s3,
        "bucket": S3_SONG_DATAPATH,
        "region": AWS_REGION,
        "vault": DWH_DB_RAW_VAULT,
        "table": "raw__song_data",
    }
]
