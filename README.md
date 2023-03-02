## Describe
To test performance on reading data from S3 and do simple aggregation for Python libraries: `pandas`, `polars` and `duckdb`

## How to:
### Edit the code:
Update S3_FILE: the path of s3 data  
Update AWS_S3_ACCESS_KEY and AWS_S3_SECRET_KEY

### Install packages:
```
pip install -r requirements.txt
```

### Run the test
```
kernprof -l -v tester.py
```

-> Results:
- In brief:
```
Pandas: Total time: 0.195698s
DuckDB: Total time: 0.0209346s  -> 10x faster than pandas
Polars: Total time: 0.046093s   -> 5x faster than pandas
```

- Details:
```
Timer unit: 1e-09 s

Total time: 0.0209346 s
File: tester.py
Function: the_duck_sql at line 15

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    15                                           @profile
    16                                           def the_duck_sql(duckdb_s3_sql):
    17         1     512848.0 512848.0      2.4      duckdb.sql(duckdb_s3_sql)
    18         1    9762616.0 9762616.0     46.6      rs = duckdb.sql(
    19         1       2864.0   2864.0      0.0          f"""
    20                                                   SELECT max(id), avg(parent), count(*)
    21         1       2375.0   2375.0      0.0          FROM '{S3_FILE}'
    22                                                   """
    23         1   10636336.0 10636336.0     50.8      ).fetchall()
    24         1       5797.0   5797.0      0.0      return {
    25         1       5657.0   5657.0      0.0          'max_id': rs[0][0],
    26         1       3352.0   3352.0      0.0          'avg_parent': rs[0][1],
    27         1       2793.0   2793.0      0.0          'count': rs[0][2],
    28                                               }

Total time: 0.0276167 s
File: tester.py
Function: the_duck_programmatic at line 30

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    30                                           @profile
    31                                           def the_duck_programmatic(duckdb_s3_sql):
    32         1     641985.0 641985.0      2.3      duckdb.sql(duckdb_s3_sql)
    33         1    7874445.0 7874445.0     28.5      duck = duckdb.sql(f"SELECT * FROM '{S3_FILE}'")
    34                                           
    35         1    6354549.0 6354549.0     23.0      count = duck.count('*')
    36         1    6454214.0 6454214.0     23.4      avg_parent = duck.mean('parent')
    37         1    6281564.0 6281564.0     22.7      max_id = duck.max('id')
    38                                           
    39         1       4679.0   4679.0      0.0      return {
    40         1       1397.0   1397.0      0.0          'max_id': max_id,
    41         1       1956.0   1956.0      0.0          'avg_parent': avg_parent,
    42         1       1886.0   1886.0      0.0          'count': count
    43                                               }

Total time: 0.195698 s
File: tester.py
Function: the_pandas at line 45

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    45                                           @profile
    46                                           def the_pandas(storage_options: dict):
    47         1  192850043.0 192850043.0     98.5      df = pd.read_parquet(
    48         1       1885.0   1885.0      0.0          S3_FILE,
    49         1       1886.0   1886.0      0.0          storage_options=storage_options
    50                                               )
    51         1     473876.0 473876.0      0.2      max_id = df.id.max()
    52         1     303811.0 303811.0      0.2      avg_parent = df.parent.mean()
    53         1    2059143.0 2059143.0      1.1      count = df.count()
    54                                           
    55         1       2304.0   2304.0      0.0      return {
    56         1       1816.0   1816.0      0.0          'max_id': max_id,
    57         1       1396.0   1396.0      0.0          'avg_parent': avg_parent,
    58         1       1397.0   1397.0      0.0          'count': count
    59                                               }

Total time: 0.046093 s
File: tester.py
Function: the_polar at line 61

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    61                                           @profile
    62                                           def the_polar(arrow_opts):
    63         1      73334.0  73334.0      0.2      polars_arrow_fs = s3fs.S3FileSystem(**arrow_opts)
    64         1   20141585.0 20141585.0     43.7      dataset = pq.ParquetDataset(S3_FILE, filesystem=polars_arrow_fs)
    65         1   24806943.0 24806943.0     53.8      pl_df = pl.from_arrow(dataset.read())
    66         1     650855.0 650855.0      1.4      rs = pl_df.select([
    67         1      58597.0  58597.0      0.1          pl.col("id").count(),
    68         1      16133.0  16133.0      0.0          pl.col("parent").mean(),
    69         1       8171.0   8171.0      0.0          pl.count()
    70                                               ])
    71         1     337405.0 337405.0      0.7      return rs.to_dicts()[0]

Wrote profile results to tester.py.lprof
Timer unit: 1e-06 s
```
