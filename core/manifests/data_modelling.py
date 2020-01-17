from core.queries.sql import (
    transform_table_dim_artists,
    transform_table_dim_songs,
    transform_table_dim_time,
    transform_table_dim_users,
    transform_table_fact_songplays,
)
from settings.envs import (
    DWH_DB_PUBLIC_VAULT,
    DWH_DB_RAW_VAULT,
)


transform_data = [
    {
        "query": transform_table_dim_artists,
        "raw_vault": DWH_DB_RAW_VAULT,
        "public_vault": DWH_DB_PUBLIC_VAULT,
        "raw_table": "raw__song_data",
        "public_table": "dim_artists",
    },
    {
        "query": transform_table_dim_songs,
        "raw_vault": DWH_DB_RAW_VAULT,
        "public_vault": DWH_DB_PUBLIC_VAULT,
        "raw_table": "raw__song_data",
        "public_table": "dim_songs",
    },
    {
        "query": transform_table_dim_time,
        "raw_vault": DWH_DB_RAW_VAULT,
        "public_vault": DWH_DB_PUBLIC_VAULT,
        "raw_table": "raw__log_data",
        "public_table": "dim_time",
    },
    {
        "query": transform_table_dim_users,
        "raw_vault": DWH_DB_RAW_VAULT,
        "public_vault": DWH_DB_PUBLIC_VAULT,
        "raw_table": "raw__log_data",
        "public_table": "dim_users",
    },
    {
        "query": transform_table_fact_songplays,
        "raw_vault": DWH_DB_RAW_VAULT,
        "public_vault": DWH_DB_PUBLIC_VAULT,
        "raw_table": ("raw__log_data", "raw__song_data"),
        "public_table": "fact_songplays",
    },
]
