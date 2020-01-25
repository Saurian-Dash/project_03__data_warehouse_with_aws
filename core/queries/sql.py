from psycopg2 import sql

from settings.envs import (
    DWH_DB_RAW_VAULT,
    DWH_DB_PUBLIC_VAULT,
)


# create data warehouse schema
def create_schema(schema):

    schema = sql.Identifier(schema)

    return sql.SQL(
        "CREATE SCHEMA IF NOT EXISTS {schema};"
    ).format(schema=schema)


# drop raw_vault tables
def drop_table(schema, table):

    return sql.SQL(
        "DROP TABLE IF EXISTS {schema}.{table};"
    ).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table)
    )


# show all tables in a schema
def list_tables(schema):

    schema = sql.Literal(schema)

    return sql.SQL(
        """
        SELECT
            table_schema,
            table_name
        FROM information_schema.tables
        WHERE table_schema = {schema}
            AND table_type = 'BASE TABLE';
        """
    ).format(schema=schema)


# copy data from s3
def copy_json_from_s3(
        bucket,
        region,
        role_arn,
        table,
        jsonpaths='auto',
        vault=DWH_DB_RAW_VAULT,
        **kwargs):

    return sql.SQL(
        """
        COPY {vault}.{table}
        FROM {bucket}
        CREDENTIALS {role_arn}
        REGION {region}
        COMPUPDATE ON
        FORMAT AS JSON {jsonpaths}
        EMPTYASNULL
        BLANKSASNULL;
        """
    ).format(
        vault=sql.Identifier(vault),
        table=sql.Identifier(table),
        bucket=sql.Literal(bucket),
        role_arn=sql.Literal(f'aws_iam_role={role_arn}'),
        region=sql.Literal(region),
        jsonpaths=sql.Literal(jsonpaths),
    )


# create raw vault tables
def create_table_raw_log_data(vault=DWH_DB_RAW_VAULT, **kwargs):

    return sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {vault}.raw__log_data (
            artist VARCHAR(255),
            auth VARCHAR(255),
            first_name VARCHAR(255),
            gender VARCHAR(255),
            item_in_session VARCHAR(255),
            last_name VARCHAR(255),
            length VARCHAR(255),
            level VARCHAR(255),
            location VARCHAR(255),
            method VARCHAR(255),
            page VARCHAR(255),
            registration VARCHAR(255),
            session_id VARCHAR(255),
            song VARCHAR(255),
            status VARCHAR(255),
            ts VARCHAR(255),
            user_agent VARCHAR(255),
            user_id VARCHAR(255)
        ) BACKUP NO;
        """
    ).format(vault=sql.Identifier(vault))


def create_table_raw_song_data(vault=DWH_DB_RAW_VAULT, **kwargs):

    return sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {vault}.raw__song_data (
            artist_id VARCHAR(255),
            artist_latitude VARCHAR(255),
            artist_location VARCHAR(255),
            artist_longitude VARCHAR(255),
            artist_name VARCHAR(255),
            duration VARCHAR(255),
            num_songs VARCHAR(255),
            song_id VARCHAR(255),
            title VARCHAR(255),
            year VARCHAR(255)
        ) BACKUP NO;
        """
    ).format(vault=sql.Identifier(vault))


# create public_vault tables
def create_table_dim_artists(vault=DWH_DB_PUBLIC_VAULT, **kwargs):

    return sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {vault}.dim_artists (
            artist_id VARCHAR(50) NOT NULL PRIMARY KEY DISTKEY ENCODE RAW,
            name VARCHAR(200) ENCODE ZSTD,
            location VARCHAR(200) ENCODE ZSTD,
            latitude NUMERIC ENCODE AZ64,
            longitude NUMERIC ENCODE AZ64
        )
        SORTKEY (artist_id);
        """
    ).format(vault=sql.Identifier(vault))


def create_table_dim_songs(vault=DWH_DB_PUBLIC_VAULT, **kwargs):

    return sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {vault}.dim_songs (
            song_id VARCHAR(50) NOT NULL PRIMARY KEY DISTKEY ENCODE RAW,
            artist_id VARCHAR(50) ENCODE ZSTD,
            title VARCHAR(200) ENCODE ZSTD,
            year SMALLINT ENCODE AZ64,
            duration NUMERIC ENCODE AZ64
        )
        SORTKEY (song_id);
        """
    ).format(vault=sql.Identifier(vault))


def create_table_dim_time(vault=DWH_DB_PUBLIC_VAULT, **kwargs):

    return sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {vault}.dim_time (
            time_id BIGINT NOT NULL PRIMARY KEY DISTKEY ENCODE RAW,
            start_time TIMESTAMP ENCODE AZ64,
            hour SMALLINT ENCODE AZ64,
            day SMALLINT ENCODE AZ64,
            week SMALLINT ENCODE AZ64,
            month SMALLINT ENCODE AZ64,
            year SMALLINT ENCODE AZ64,
            weekday SMALLINT ENCODE AZ64
        )
        SORTKEY (time_id);
        """
    ).format(vault=sql.Identifier(vault))


def create_table_dim_users(vault=DWH_DB_PUBLIC_VAULT, **kwargs):

    return sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {vault}.dim_users (
            user_id BIGINT NOT NULL PRIMARY KEY DISTKEY ENCODE RAW,
            first_name VARCHAR(100) ENCODE ZSTD,
            last_name VARCHAR(100) ENCODE ZSTD,
            gender CHAR(1) ENCODE ZSTD,
            level VARCHAR(50) ENCODE ZSTD
        )
        SORTKEY (user_id);
        """
    ).format(vault=sql.Identifier(vault))


def create_table_fact_songplays(vault=DWH_DB_PUBLIC_VAULT, **kwargs):

    return sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {vault}.fact_songplays (
            songplay_id BIGINT NOT NULL PRIMARY KEY IDENTITY(0, 1) ENCODE RAW,
            time_id BIGINT NOT NULL DISTKEY ENCODE AZ64,
            start_time TIMESTAMP ENCODE AZ64,
            user_id BIGINT ENCODE AZ64,
            level VARCHAR(50) ENCODE ZSTD,
            song_id VARCHAR(50) ENCODE ZSTD,
            artist_id VARCHAR(50) ENCODE ZSTD,
            session_id BIGINT ENCODE AZ64,
            location VARCHAR(200) ENCODE ZSTD,
            user_agent VARCHAR(255) ENCODE ZSTD
        )
        SORTKEY (time_id);
        """
    ).format(vault=sql.Identifier(vault))


# transform tables for dimensional model
def transform_table_dim_artists(
        raw_table=None,
        public_table=None,
        raw_vault=DWH_DB_RAW_VAULT,
        public_vault=DWH_DB_PUBLIC_VAULT,
        **kwargs):

    return sql.SQL(
        """
        BEGIN;

        CREATE TEMP TABLE t1 AS
        SELECT DISTINCT
            artist_id :: VARCHAR,
            artist_name :: VARCHAR AS name,
            artist_location :: VARCHAR AS location,
            artist_latitude :: NUMERIC AS latitude,
            artist_longitude :: NUMERIC AS longitude
        FROM {raw_vault}.{raw_table}
        WHERE artist_id IS NOT NULL;

        INSERT INTO {public_vault}.{public_table} (
            artist_id,
            name,
            location,
            latitude,
            longitude
        )
        SELECT
            artist_id,
            name,
            location,
            latitude,
            longitude
        FROM t1;

        DROP TABLE IF EXISTS t1;

        COMMIT;
        """
    ).format(
        raw_vault=sql.Identifier(raw_vault),
        raw_table=sql.Identifier(raw_table),
        public_vault=sql.Identifier(public_vault),
        public_table=sql.Identifier(public_table),
    )


def transform_table_dim_songs(
        raw_table=None,
        public_table=None,
        raw_vault=DWH_DB_RAW_VAULT,
        public_vault=DWH_DB_PUBLIC_VAULT,
        **kwargs):

    return sql.SQL(
        """
        BEGIN;

        CREATE TEMP TABLE t1 AS
        SELECT DISTINCT
            song_id :: VARCHAR,
            artist_id :: VARCHAR,
            title :: VARCHAR,
            CASE
                WHEN year = '0' THEN NULL
                ELSE year :: SMALLINT
            END AS year,
            duration :: NUMERIC
        FROM {raw_vault}.{raw_table}
        WHERE song_id IS NOT NULL;

        INSERT INTO {public_vault}.{public_table} (
            song_id,
            artist_id,
            title,
            year,
            duration
        )
        SELECT
            song_id,
            artist_id,
            title,
            year,
            duration
        FROM t1;

        DROP TABLE IF EXISTS t1;

        COMMIT;
        """
    ).format(
        raw_vault=sql.Identifier(raw_vault),
        raw_table=sql.Identifier(raw_table),
        public_vault=sql.Identifier(public_vault),
        public_table=sql.Identifier(public_table),
    )


def transform_table_dim_time(
        raw_table=None,
        public_table=None,
        raw_vault=DWH_DB_RAW_VAULT,
        public_vault=DWH_DB_PUBLIC_VAULT,
        **kwargs):

    return sql.SQL(
        """
        BEGIN;

        INSERT INTO {public_vault}.{public_table} (
            time_id,
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT DISTINCT
            ts :: BIGINT AS time_id,
            TIMESTAMP 'epoch' + time_id / 1000 * INTERVAL '1 second'
            AS start_time,
            EXTRACT (HOUR FROM start_time) :: SMALLINT AS hour,
            EXTRACT(DAY FROM start_time) :: SMALLINT AS day,
            EXTRACT(WEEK FROM start_time) :: SMALLINT AS week,
            EXTRACT(MONTH FROM start_time) :: SMALLINT AS month,
            EXTRACT(YEAR FROM start_time) :: SMALLINT AS year,
            EXTRACT(DOW FROM start_time) :: SMALLINT AS weekday
        FROM {raw_vault}.{raw_table}
        WHERE page = 'NextSong'
            AND ts IS NOT NULL;

        COMMIT;
        """
    ).format(
        raw_vault=sql.Identifier(raw_vault),
        raw_table=sql.Identifier(raw_table),
        public_vault=sql.Identifier(public_vault),
        public_table=sql.Identifier(public_table),
    )


def transform_table_dim_users(
        raw_table=None,
        public_table=None,
        raw_vault=DWH_DB_RAW_VAULT,
        public_vault=DWH_DB_PUBLIC_VAULT,
        **kwargs):

    return sql.SQL(
        """
        BEGIN;

        CREATE TEMP TABLE t1 AS
        SELECT
            user_id :: BIGINT,
            Max(ts) :: BIGINT AS ts
        FROM {raw_vault}.{raw_table}
        WHERE user_id IS NOT NULL
        GROUP BY user_id;

        CREATE TEMP TABLE t2 AS
        SELECT DISTINCT
            user_id :: BIGINT,
            first_name :: VARCHAR,
            last_name :: VARCHAR,
            gender :: VARCHAR,
            level :: VARCHAR,
            ts :: BIGINT
        FROM {raw_vault}.{raw_table} t2
        WHERE t2.user_id IS NOT NULL
            AND t2.ts = (
                SELECT ts FROM t1
                WHERE t1.user_id = t2.user_id
            );

        INSERT INTO {public_vault}.{public_table} (
            user_id,
            first_name,
            last_name,
            gender,
            level
        )
        SELECT
            user_id,
            first_name,
            last_name,
            gender,
            level
        FROM t2;

        DROP TABLE IF EXISTS t1;
        DROP TABLE IF EXISTS t2;

        COMMIT;
        """
    ).format(
        raw_vault=sql.Identifier(raw_vault),
        raw_table=sql.Identifier(raw_table),
        public_vault=sql.Identifier(public_vault),
        public_table=sql.Identifier(public_table),
    )


def transform_table_fact_songplays(
        raw_table=None,
        public_table=None,
        raw_vault=DWH_DB_RAW_VAULT,
        public_vault=DWH_DB_PUBLIC_VAULT,
        **kwargs):

    raw_log_data, raw_song_data = raw_table

    return sql.SQL(
        """
        BEGIN;

        CREATE TEMP TABLE t1 AS
        SELECT DISTINCT
            song_id :: VARCHAR,
            artist_id :: VARCHAR,
            artist_name :: VARCHAR,
            title :: VARCHAR
        FROM {raw_vault}.{raw_song_data} t1
        WHERE song_id IS NOT NULL
        ORDER BY artist_name, title;

        CREATE TEMP TABLE t2 AS
        SELECT
            ts :: BIGINT AS time_id,
            TIMESTAMP 'epoch' + time_id / 1000 * INTERVAL '1 second'
            AS start_time,
            user_id :: BIGINT,
            level :: VARCHAR,
            t1.song_id :: VARCHAR AS song_id,
            t1.artist_id :: VARCHAR AS artist_id,
            session_id :: BIGINT,
            location :: VARCHAR,
            user_agent :: VARCHAR
        FROM {raw_vault}.{raw_log_data} t2
        LEFT JOIN t1 ON
            t1.artist_name = t2.artist
            AND t1.title = t2.song
        WHERE t2.page = 'NextSong'
            AND time_id IS NOT NULL
        ORDER BY time_id;

        INSERT INTO {public_vault}.{public_table} (
            time_id,
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )
        SELECT
            time_id,
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        FROM t2;

        DROP TABLE IF EXISTS t1;
        DROP TABLE IF EXISTS t2;

        COMMIT;
        """
    ).format(
        raw_vault=sql.Identifier(raw_vault),
        raw_log_data=sql.Identifier(raw_log_data),
        raw_song_data=sql.Identifier(raw_song_data),
        public_vault=sql.Identifier(public_vault),
        public_table=sql.Identifier(public_table),
    )
