import google.auth
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from .bqlite_table import BQLiteTable
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import time
import logging
import datetime


class BQLite:
    """
    Simple bigQuery client class.
    """

    def __init__(self, credentials=None, json_key_filepath=None):
        """
        setup parameters of credentials.
        :param credentials: credentials object. Only supports credentials from google-auth-library-python.
        :param json_key_filepath: json key filepath about credentials.
        """
        self.__credentials = credentials
        self.__json_key_filepath = json_key_filepath

        logging.basicConfig()
        self.__logger = logging.getLogger(__name__)
        self.__logger.setLevel(logging.INFO)

    def create_table(self, schema_df: pd.DataFrame, project_name: str, dataset_name: str, table_name: str):
        """
        create BigQuery`s table. BigQuery`s dataset of table must exist already.
        :param schema_df: dataframe for generating schema of bigQuery
        :param project_name: BigQuery`s project name
        :param dataset_name: BigQuery`s dataset name
        :param table_name: BigQuery`s table name
        """
        client = self.__prepare_client(project_name)
        dataset = BQLite.__prepare_dataset(client, dataset_name)

        table = dataset.table(table_name)
        if table.exists(client=client):
            raise Exception('Table {}:{} already exist.'.format(dataset_name, table_name))

        table = dataset.table(table_name)
        schema = BQLite.__to_bq_schema(schema_df)
        table.schema = schema

        table.create(client=client)

    def delete_table(self, project_name: str, dataset_name: str, table_name: str):
        """
        delete BigQuery`s table. BigQuery`s table must exist already.
        :param project_name: BigQuery`s project name
        :param dataset_name: BigQuery`s dataset name
        :param table_name: BigQuery`s table name
        """
        client = self.__prepare_client(project_name)
        dataset = BQLite.__prepare_dataset(client, dataset_name)

        table = dataset.table(table_name)
        if not table.exists(client=client):
            raise Exception('Table {}:{} does not exist.'.format(dataset_name, table_name))

        table.delete(client=client)

    def read_bq(self, sql, project_name, use_legacy_sql=False, use_query_cache=False, max_results=None) -> pd.DataFrame:
        """
        read data by sql from BigQuery.
        :param sql: select query
        :param project_name: target BigQuery project name
        :param use_legacy_sql: query parameter
        :param use_query_cache: query parameter
        :param max_results: query parameter
        :return: dataFrame of query result
        """
        client = self.__prepare_client(project_name)

        # run sql
        query = client.run_sync_query(sql)
        query.use_legacy_sql = use_legacy_sql
        query.use_query_cache = use_query_cache
        query.maxResults = max_results
        query.run(client=client)

        if not query.complete:
            BQLite.__wait_for_job(query.job)

        # get result
        rows, total_rows, page_token = query.fetch_data()
        rows = list(map(list, zip(*rows)))
        while page_token:
            tmp_rows, total_rows, page_token = query.fetch_data(page_token=page_token)
            tmp_rows = list(map(list, zip(*tmp_rows)))
            for row_index in range(len(rows)):
                rows[row_index].extend(tmp_rows[row_index])

        return BQLite.__to_df(rows, query.schema)

    def to_bq(self, load_df, project_name, dataset_name, table_name):
        """
        write data to BigQuery. If BigQuery`s table doesn`t exist method create BigQuery`s table.
        :param load_df:
        :param project_name:
        :param dataset_name:
        :param table_name:
        """
        client = self.__prepare_client(project_name)
        dataset = BQLite.__prepare_dataset(client, dataset_name)

        table = dataset.table(table_name)
        if table.exists(client):
            table.reload(client)
        else:
            self.__logger.info('Table {}:{} does not exist.'.format(dataset_name, table_name))
            table.schema = BQLite.__to_bq_schema(load_df)
            table.create()
            self.__logger.info('Create Table {}:{}.'.format(dataset_name, table_name))

        # cast to original table class for using null value.
        table.__class__ = BQLiteTable
        rows = load_df.values
        errors = table.insert_data(rows, client=client)
        if errors:
            raise Exception(errors)
        self.__logger.info('Loaded data into {}:{}'.format(dataset_name, table_name))

    def read_to_bq(self, sql, project_name, write_dataset_name, write_table_name, write_disposition='WRITE_TRUNCATE',
                   use_legacy_sql=False, use_query_cache=False, max_results=None, wait_timeout=1800):
        """
        write data to BigQuery by sql.
        :param sql: select query
        :param project_name: target BigQuery project name
        :param write_dataset_name: write dataset name
        :param write_table_name: write table name
        :param write_disposition: query option about writing
        :param use_legacy_sql: query option about select
        :param use_query_cache: query option about select
        :param max_results: query option about select
        :param wait_timeout: waiting time for finishing job (sec)
        """
        client = self.__prepare_client(project_name)

        write_dataset = client.dataset(write_dataset_name)
        write_table = write_dataset.table(write_table_name)
        job_name = write_dataset_name + '_' + write_table_name + "_read_to_bq" +\
                   datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        # run sql
        query = client.run_async_query(job_name, sql)
        query.use_legacy_sql = use_legacy_sql
        query.use_query_cache = use_query_cache
        query.maxResults = max_results
        query.destination = write_table
        query.write_disposition = write_disposition
        query.begin(client)

        retry_count = wait_timeout/10
        while retry_count > 0 and query.state != 'DONE':
            retry_count -= 1
            time.sleep(10)
            query.reload()

        if query.state != 'DONE':
            self.__logger.warn('Timeout. Write data into {}:{}'.format(write_dataset_name, write_table_name))

    def __prepare_client(self, project_name: str):
        return bigquery.Client(project=project_name, credentials=self.__generate_credential())

    def __generate_credential(self):
        if self.__credentials is not None:
            pass

        elif self.__json_key_filepath is not None:
            self.__credentials = service_account.Credentials.from_service_account_file(self.__json_key_filepath)

        else:
            # no setup parameter case use default credentials
            self.__logger.info('no setup parameter case get credentials by google_auth.default()')
            self.__credentials, df_project = google.auth.default()

        return self.__credentials

    @staticmethod
    def __prepare_dataset(client, dataset_name: str):
        dataset = client.dataset(dataset_name)
        if not dataset.exists():
            raise Exception('Dataset {} does not exist.'.format(dataset_name))
        return dataset

    @staticmethod
    def __to_df(rows, schema) -> pd.DataFrame:

        def to_flg(value) -> bool:
            return value == 'true'

        def cast_all_column(values, values_type, row_no=None):
            cast_mapping = {
                'STRING':    (str,          None),
                'DATETIME':  (str,          None),
                'DATE':      (str,          None),
                'TIME':      (str,          None),
                'FLOAT':     (float,        np.dtype(float)),
                'INTEGER':   (float,        np.dtype(float)),
                'BOOLEAN':   (to_flg,       np.dtype(bool)),
                'TIMESTAMP': (pd.Timestamp, 'datetime64[ns]')
            }
            cast_function, new_values_type = cast_mapping.get(values_type, (None, None))

            if cast_function is None:
                raise Exception('Not Support to type:' + values_type + '.')
            values = cast_column(values, cast_function, row_no)

            return values, new_values_type

        def cast_column(values, cast_function, row_no=None):
            if row_no is None:
                # is not repeated
                for tmp_row_no in range(len(values)):
                    if values[tmp_row_no] is not None:
                        values[tmp_row_no] = cast_function(values[tmp_row_no])
                return values

            else:
                # is repeated
                repeat_values = [None] * len(row_values)
                if row_no is None:
                    raise Exception('Repeated column need _no_in_row parameter.')

                for i, value in enumerate(values):
                    if row_no < len(value) and value[row_no] is not None:
                        repeat_values[i] = cast_function(value[row_no])

                return repeat_values

        if len(rows) == 0:
            return pd.DataFrame()

        columns_data = []
        for index, col_dict in enumerate(schema):
            col_name = col_dict.name
            col_type = col_dict.field_type
            col_mode = col_dict.mode
            row_values = rows[0]
            del rows[0], col_dict

            if col_mode == 'REPEATED':
                max_row_length = max([len(row_value) for row_value in row_values])
                for no_in_row in range(max_row_length):
                    col_values, col_new_type = cast_all_column(row_values, col_type, row_no=no_in_row)
                    columns_data.append((col_name+"_"+str(no_in_row), pd.Series(data=col_values, dtype=col_new_type)))
                    del col_values

            else:
                row_values, col_new_type = cast_all_column(row_values, col_type)
                columns_data.append((col_name, pd.Series(data=row_values, dtype=col_new_type)))
                del col_name, row_values

        return pd.DataFrame.from_items(columns_data)

    @staticmethod
    def __to_bq_schema(schema_df: pd.DataFrame):
        type_mapping = {
            'i': 'INTEGER',
            'b': 'BOOLEAN',
            'f': 'FLOAT',
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }

        fields = []
        for column_name, dtype in schema_df.dtypes.iteritems():
            fields.append(SchemaField(column_name, type_mapping.get(dtype.kind, 'STRING')))

        return fields

    @staticmethod
    def __wait_for_job(job):
        while True:
            job.reload()
            if job.state == 'DONE':
                if job.error_result:
                    raise RuntimeError(job.error_result)
                return
            time.sleep(1)
