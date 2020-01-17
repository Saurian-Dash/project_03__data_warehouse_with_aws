from core.queries.sql import (
    create_table_dim_artists,
    create_table_dim_songs,
    create_table_dim_time,
    create_table_dim_users,
    create_table_fact_songplays,
    create_table_raw_log_data,
    create_table_raw_song_data,
)
from settings.envs import (
    DWH_DB_PUBLIC_VAULT,
    DWH_DB_RAW_VAULT,
)


create_tables = [
    {
        "query": create_table_raw_log_data,
        "vault": DWH_DB_RAW_VAULT,
        "table": "raw__log_data",
    },
    {
        "query": create_table_raw_song_data,
        "vault": DWH_DB_RAW_VAULT,
        "table": "raw__song_data",
    },
    {
        "query": create_table_dim_artists,
        "vault": DWH_DB_PUBLIC_VAULT,
        "table": "dim_artists",
    },
    {
        "query": create_table_dim_songs,
        "vault": DWH_DB_PUBLIC_VAULT,
        "table": "dim_songs",
    },
    {
        "query": create_table_dim_time,
        "vault": DWH_DB_PUBLIC_VAULT,
        "table": "dim_time",
    },
    {
        "query": create_table_dim_users,
        "vault": DWH_DB_PUBLIC_VAULT,
        "table": "dim_users",
    },
    {
        "query": create_table_fact_songplays,
        "vault": DWH_DB_PUBLIC_VAULT,
        "table": "fact_songplays",
    },
]
