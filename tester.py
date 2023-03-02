import duckdb
import pandas as pd
import line_profiler
import polars as pl
import pyarrow.parquet as pq
import s3fs

profile = line_profiler.LineProfiler()

S3_FILE = "s3://lake-dev/warehouse/hackernews.db/items/*.snappy.parquet"

AWS_S3_ACCESS_KEY=''
AWS_S3_SECRET_KEY=''
AWS_S3_ENDPOINT='localhost:9000'

@profile
def the_duck_sql(duckdb_s3_sql):
    duckdb.sql(duckdb_s3_sql)
    rs = duckdb.sql(
        f"""
        SELECT max(id), avg(parent), count(*)
        FROM '{S3_FILE}'
        """
    ).fetchall()
    return {
        'max_id': rs[0][0],
        'avg_parent': rs[0][1],
        'count': rs[0][2],
    }

@profile
def the_duck_programmatic(duckdb_s3_sql):
    duckdb.sql(duckdb_s3_sql)
    duck = duckdb.sql(f"SELECT * FROM '{S3_FILE}'")

    count = duck.count('*')
    avg_parent = duck.mean('parent')
    max_id = duck.max('id')

    return {
        'max_id': max_id,
        'avg_parent': avg_parent,
        'count': count
    }

@profile
def the_pandas(storage_options: dict):
    df = pd.read_parquet(
        S3_FILE,
        storage_options=storage_options
    )
    max_id = df.id.max()
    avg_parent = df.parent.mean()
    count = df.count()

    return {
        'max_id': max_id,
        'avg_parent': avg_parent,
        'count': count
    }

@profile
def the_polar(arrow_opts):
    polars_arrow_fs = s3fs.S3FileSystem(**arrow_opts)
    dataset = pq.ParquetDataset(S3_FILE, filesystem=polars_arrow_fs)
    pl_df = pl.from_arrow(dataset.read())
    rs = pl_df.select([
        pl.col("id").count(),
        pl.col("parent").mean(),
        pl.count()
    ])
    return rs.to_dicts()[0]

if __name__ == '__main__':
    duckdb.sql(
        """
        INSTALL httpfs;
        LOAD httpfs;
        """
    )
    duckdb_s3_sql = f"""
    set s3_endpoint='{AWS_S3_ENDPOINT}';
    set s3_access_key_id='{AWS_S3_ACCESS_KEY}';
    set s3_secret_access_key='{AWS_S3_SECRET_KEY}';
    set s3_use_ssl=false;
    set s3_region='';
    set s3_url_style='path';
    """

    arrow_opts = {
        "key": AWS_S3_ACCESS_KEY,
        "secret": AWS_S3_SECRET_KEY,
        "client_kwargs": {"endpoint_url": f"http://{AWS_S3_ENDPOINT}"}
    }
    
    the_duck_sql(duckdb_s3_sql)
    the_duck_programmatic(duckdb_s3_sql)
    the_pandas(storage_options=arrow_opts)
    the_polar(arrow_opts)
    
    profile.print_stats()

