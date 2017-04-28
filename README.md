# BigQuery-Pandas-Lite
BigQuery library for python with pandas.DataFrame.

## Requirement
- Python 3.5 or later

## Awesome point
### 1. BigQuery-Pandas-Lite is less memory intensive
  - ```bqlite.read_bq``` is less memory intensive than ```pandas.io.gbq.read_gbq```
  - About pandas(v0.19.2): Memory usage of ```final_df = concat(dataframe_list, ignore_index=True)``` part is very large

### 2. BigQuery-Pandas-Lite can write NULL value
  - If pandas.dataframe of loading has nan or None ```bqlite.to_bq``` load to bigQuery as NULL value.
  - About ```google.cloud.bigquery.table.Table.insert_data``` can not write NULL value.

## Install

### Use setup.py
```shell
# Master branch
$ git clone https://github.com/tomomotofactory/BigQuery-Pandas-Lite.git
$ python setup.py install
```

### Use pip
```shell
# Master branch
$ pip install git+https://github.com/tomomotofactory/BigQuery-Pandas-Lite.git
# Specific tag (or branch, commit hash)
$ pip install git+https://github.com/tomomotofactory/BigQuery-Pandas-Lite@v1.0.0
```

## How to use

### read from bigQuery
```python
from bqlite import BQLite

bq = BQLite()
df = bq.read_bq(sql='SELECT * FROM `nyc-tlc.green.trips_2014`',
                project='input_your_project_id')

# print pandas.DataFrame
print(df)
```

### write to bigQuery
```python
from bqlite import BQLite

bq = BQLite()
bq.to_bq(load_df=load_pandas_dataframe, project_name='your_project',
         dataset_name='your_dataset', table_name='your_table')
```

### create table
```python
from bqlite import BQLite

bq = BQLite()
bq.create_table(schema_df=schema_pandas_dataframe, project_name='your_project',
                dataset_name='your_dataset', table_name='your_table')
```

### delete table
```python
from bqlite import BQLite

bq = BQLite()
bq.delete_table(project_name='your_project', dataset_name='your_dataset', table_name='your_table')
```

### using credential parameter
```python
from bqlite import BQLite
import google.auth

credentials, df_project = google.auth.default()
bq = BQLite(credentials=credentials)

df = bq.read_bq(sql='SELECT * FROM `nyc-tlc.green.trips_2014`',
                project='input_your_project_id')
```

### using json_key_filepath parameter
```python
from bqlite import BQLite

bq = BQLite(json_key_filepath='bigquery-key.json')

df = bq.read_bq(sql='SELECT * FROM `nyc-tlc.green.trips_2014`',
                project='input_your_project_id')
```
