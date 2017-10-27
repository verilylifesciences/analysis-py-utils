# Copyright 2017 Verily Life Sciences Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Library for interacting with BigQuery.

Sample usage:

    client = bq.Client(project_id)
    result = client.Query(query)

NOTE: All API calls to BigQuery that call the REST API should be wrapped in execute_with_retries.
This is because these calls are prone to failing randomly with 500 or 503 errors. You can see if
something calls the REST API from the bq python client library:
https://googlecloudplatform.github.io/google-cloud-python/stable/bigquery/usage.html
In the documentation for each function, there should be a link to the REST API documentation if it
is a REST API call.
"""

# This is a workaround to address
# https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2366
from __future__ import absolute_import

from collections import OrderedDict
import datetime
import json
import logging
import os
import time
from typing import List  # noqa: F401
import uuid

from google.cloud import bigquery, storage

from verily.bigquery_wrapper.bq_base import (DEFAULT_MAX_API_CALL_TRIES, MAX_TABLES,
                                             BigqueryBaseClient, execute_with_retries,
                                             wait_for_job)


class TimeoutException(Exception):
    def __init__(self, message):
        super(TimeoutException, self).__init__(message)


# The call to datasets to list all tables requires you to set a maximum number of tables
# (or use the unspecified API default). We always want to list all the tables so we set
# a really high number for it. If ever there would be a dataset with more than 5000 tables,
# this constant should be adjusted.
MAX_TABLES = 5000

# This is the default timeout for any Bigquery operations executed in this file.
DEFAULT_TIMEOUT_SEC = 600

# Bigquery has a limit of max 10000 rows to insert per request
MAX_ROWS_TO_INSERT = 10000


class Client(BigqueryBaseClient):
    """Stores credentials and pointers to a BigQuery project.

    Args:
      project_id: The id of the project to associate with the client.
    """

    def __init__(self, project_id, default_dataset=None, maximum_billing_tier=None,
                 default_max_api_call_tries=DEFAULT_MAX_API_CALL_TRIES):
        self.gclient = bigquery.Client(project=project_id)
        self.default_max_api_call_tries = default_max_api_call_tries
        super(Client, self).__init__(project_id, default_dataset, maximum_billing_tier)

    def get_query_results(self, query, max_results=None, use_legacy_sql=False,
                          max_wait_sec=DEFAULT_TIMEOUT_SEC):
        """Returns a list or rows, each of which is a list of values.

        Since we want to execute the query with retries, we need to surround the whole method with
        a retry loop since we need a new query job with a new uuid each time.

        Args:
            query: A string with a complete SQL query.
            max_results: Maximum number of rows to return. If None, return all rows
            use_legacy_sql: Whether to use legacy SQL
            max_wait_sec: The maximum number of seconds to wait for the query to complete.

        Returns:
            A list of lists of values.
        """
        return execute_with_retries(
            lambda: self._get_query_results_helper(
                query, max_results=max_results, use_legacy_sql=use_legacy_sql,
                max_wait_sec=DEFAULT_TIMEOUT_SEC),
            self.default_max_api_call_tries)

    def _get_query_results_helper(self, query, max_results, use_legacy_sql, max_wait_sec):
        query_job = self.gclient.run_async_query(str(uuid.uuid4()), query)
        query_job.use_legacy_sql = use_legacy_sql
        if self.maximum_billing_tier:
            query_job.maximum_billing_tier = self.maximum_billing_tier
        query_job.begin()
        # This command is already wrapped in retries, so set max_tries to 1.
        wait_for_job(query_job, query=query, max_wait_sec=max_wait_sec, max_tries=1)
        query_results = bigquery.query.QueryResults('', self.gclient)
        query_results._properties['jobReference'] = {
            'jobId': query_job.name,
            'projectId': query_job.project
        }

        # This command is already wrapped in retries, so set max_tries to 1.
        return execute_with_retries(lambda: list(query_results.fetch_data(max_results=max_results)),
                                    max_tries=1)

    def create_table_from_query(self,
                                query,
                                table_path,
                                write_disposition='WRITE_EMPTY',
                                use_legacy_sql=False,
                                max_wait_sec=DEFAULT_TIMEOUT_SEC,
                                expected_schema=None):
        """Creates a table in BigQuery from a specified query.

        Since we want to execute the query with retries, we need to surround the whole method with
        a retry loop since we need a new query job with a new uuid each time.

        Args:
          query: The query to run.
          table_path: The path to the table (in the client's project) to write
              the results to.
          write_disposition: One of 'WRITE_TRUNCATE', 'WRITE_APPEND',
              'WRITE_EMPTY'. Default is WRITE_EMPTY.
          use_legacy_sql: Whether the query is written in standard or legacy sql.
          max_wait_sec: Seconds to wait for the query before timing out. Set to None for async.
          expected_schema: The expected schema of the resulting table (only required for mocking).
        """
        return execute_with_retries(
            lambda: self._create_table_from_query_helper(
                query, table_path, write_disposition=write_disposition,
                use_legacy_sql=use_legacy_sql, max_wait_sec=max_wait_sec,
                expected_schema=expected_schema),
            max_tries=self.default_max_api_call_tries)

    def _create_table_from_query_helper(self,
                                        query,
                                        table_path,
                                        write_disposition='WRITE_EMPTY',
                                        use_legacy_sql=False,
                                        max_wait_sec=DEFAULT_TIMEOUT_SEC,
                                        expected_schema=None):  # pylint: disable=unused-argument

        if write_disposition not in ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']:
            raise ValueError('write_disposition must be one of WRITE_TRUNCATE, '
                             'WRITE_APPEND, or WRITE_EMPTY')
        _, dataset, table = self.parse_table_path(table_path)
        query_job = self.gclient.run_async_query(str(uuid.uuid4()), query)
        query_job.destination = self.gclient.dataset(dataset).table(table)
        query_job.write_disposition = write_disposition
        query_job.use_legacy_sql = use_legacy_sql
        query_job.allow_large_results = True
        if self.maximum_billing_tier:
            query_job.maximum_billing_tier = self.maximum_billing_tier
        query_job.begin()
        if max_wait_sec:
            # This command is already wrapped in retries, so set max_tries to 1.
            wait_for_job(query_job, query=query, max_wait_sec=max_wait_sec, max_tries=1)
        return query_job

    def create_tables_from_dict(self, table_names_to_schemas, dataset_id=None,
                                replace_existing_tables=True):
        """Creates a set of tables from a dictionary of table names to their schemas.

        Args:
          table_names_to_schemas: A dictionary of:
            key: The table name.
            value: A list of SchemaField objects.
          dataset_id: The dataset in which to create tables. If not specified, use default dataset.
          replace_existing_tables: If True, delete and re-create tables. Otherwise, leave
            pre-existing tables alone and only create those that don't yet exist.
        """

        dataset_id = dataset_id if dataset_id else self.dataset
        for name, schema in table_names_to_schemas.iteritems():
            table = self.gclient.dataset(dataset_id).table(name, schema)
            if self.table_exists(table):
                if replace_existing_tables:
                    self.delete_table(table)
                else:
                    logging.warning('Table {} already exists. Skipping.'.format(name))
                    continue
            self.create_table(table)

    def create_dataset_by_name(self, name, expiration_hours=None):
        """Create a new dataset within the current project.

        Args:
          name: The name of the new dataset.
          expiration_hours: The default expiration time for tables within the dataset.
        """
        if name not in self.get_datasets():
            dataset = self.gclient.dataset(name)
            if expiration_hours:
                dataset.default_table_expiration_ms = expiration_hours * (60 * 60 * 1000)
            self.create_dataset(dataset)

    def delete_dataset_by_name(self, name, delete_all_tables=False,
                               max_wait_sec=DEFAULT_TIMEOUT_SEC):
        """Delete a dataset within the current project.

        Args:
          name: The name of the dataset to delete.
          delete_all_tables: If True, will delete all tables in the dataset before attempting to
              delete the dataset. You can't delete a dataset until it contains no tables.
          max_wait_sec: The maximum amount of time to wait to delete the datasets.
        """
        if delete_all_tables:
            for table in list(self.tables(name)):
                self.delete_table_by_name(table)
        # Give BigQuery time to delete the tables before attempting to delete the dataset.
        start_time = datetime.datetime.utcnow()
        deadline = start_time + datetime.timedelta(seconds=max_wait_sec)
        while datetime.datetime.utcnow() < deadline:
            if len(self.tables(name)) == 0:
                self.delete_dataset(self.gclient.dataset(name))
                return
            time.sleep(2)
        raise RuntimeError("Couldn't delete dataset " + name + " .")

    def delete_table_by_name(self, table_path):
        """Delete a table within the current project.

        Args:
          table_path: A string of the form '<dataset id>.<table name>'.
        """

        dataset_id, table_id = table_path.split('.')
        self.delete_table(self.gclient.dataset(dataset_id).table(table_id))

    def tables(self, dataset_id, max_tries=None):
        """Returns a list of table names in a given dataset.

        Args:
          dataset_id: The dataset to query.
          max_tries: Optional number of times to try making the list_tables API call.

        Returns:
          A list of table names (strings).
        """
        dataset = self.gclient.dataset(dataset_id)
        # Update the dataset so that it loads in all the tables. This syncs up the stored state
        # of the dataset in this program to the actual state of the dataset in BigQuery.
        start_time = datetime.datetime.utcnow()
        deadline = start_time + datetime.timedelta(seconds=20)
        dataset_updated = False
        while not dataset_updated:
            try:
                self.reload_dataset(dataset)
                dataset_updated = True
            except Exception as ex:
                if datetime.datetime.now() > deadline:
                    raise ex
                # Rate limit for metadata updates are one operation every two seconds, so
                # we wait a couple seconds and try again.
                time.sleep(2)

        max_tries = max_tries or self.default_max_api_call_tries
        tables = execute_with_retries(lambda: list(dataset.list_tables(max_results=MAX_TABLES)),
                                      max_tries)
        # Each entry in the list returned by list_tables() is of the form
        # <project>:<dataset>.<table> so this converts them to <dataset>.<table>
        # This is used by the test fixture tear down, so if a change causes this
        # to start failing, unit tests will break.
        return [t.table_id.split(':')[1] for t in tables]

    def _get_bq_table(self, table_name, dataset_id=None, project_id=None):
        """Get a bigquery.table object for the given table name/path
        Args:
            table_name: Name of the table
            dataset_id: Dataset ID
            project_id: Project ID
        Returns:
            A bigquery.table object
        """
        if not dataset_id:
            if not self.dataset:
                raise ValueError("Neither dataset_id or the default dataset id is specified")
            dataset_id = self.dataset

        if not project_id or project_id == self.project_id:
            return self.gclient.dataset(dataset_id).table(table_name)

        return bigquery.Client(project_id).dataset(dataset_id).table(table_name)

    def get_schema(self, dataset_id, table_name, project_id=None):
        """Returns the schema of a table.

        Args:
          dataset_id: The dataset to query.
          table_name: The name of the table.
          project_id: The project ID of the table.
        Returns:
          A list of SchemaFields representing the schema.
        """

        table = self._get_bq_table(table_name, dataset_id, project_id)

        # Note: The reload() method below does not return the data in the table, it only returns the
        # table resource, which describes the structure of this table.
        self.reload_table(table)
        return table.schema

    def get_datasets(self, max_tries=None):
        """Returns a list of dataset ids in the current project.

        Args:
            max_tries: Optional number of retries for list_datasets API call.
        Returns:
            A list of dataset ids/names (strings).
        """
        max_tries = max_tries or self.default_max_api_call_tries
        datasets = execute_with_retries(
            lambda: list(self.gclient.list_datasets(max_results=MAX_TABLES)), max_tries)

        # Each entry in the list returned by list_datasets() is of the form
        # <project>:<dataset> so this removes the project id.
        # This is used by the test fixture setup, so if a change causes this
        # to start failing, unit tests will break.
        return [ds.name for ds in datasets]

    def populate_table(self, table_path, schema, data=[], max_wait_sec=DEFAULT_TIMEOUT_SEC,
                       max_tries=1):
        """Creates a table and populates it with a list of rows.

        The data is written using the BQ streaming API, which does not immediately make the data
        available for pulling the table. However, it is immediately available for querying, and a
        table created from a query is immediately available for pulling. So, we stream the data
        into a temporary table, then query the temp table to create the requested table.

        If max_wait_sec is reached and there's still no data in the temporary table, attempt to
        insert data again up to max_tries times.

        Args:
            table_path: A string of the form '<dataset id>.<table name>'.
            schema: A list of SchemaFields to represent the table's schema.
            data: A list of rows, each of which is a list of values.
            max_wait_sec: The maximum number of seconds to wait for the table to be populated.
            max_tries: The maximum number of tries each time max_wait_sec is reached.
        """
        tmp_path = table_path + "_tmp"
        _, dataset_id, tmp_table_id = self.parse_table_path(tmp_path)

        tmp_table = self.gclient.dataset(dataset_id).table(tmp_table_id, schema)
        self.delete_and_recreate_table(tmp_table)
        select_all_from_tmp_table_query = 'SELECT * FROM `' + tmp_path + '`'
        if data:
            # The BigQuery streaming API does not guarantee the data will be queryable immediately,
            # so we need to wait for data to show up in the temporary table before creating another
            # table from it.
            # The retry and timout logic fixes flaky test failures.
            num_tries = 0
            success = False
            while num_tries < max_tries:
                # We don't pass through a schema because we trust the table to be created correctly.
                self.append_rows(tmp_path, data)

                deadline = datetime.datetime.utcnow() + datetime.timedelta(seconds=max_wait_sec)
                while True:
                    if self.get_query_results(select_all_from_tmp_table_query):
                        success = True
                        break
                    elif datetime.datetime.utcnow() > deadline:
                        num_tries += 1
                        logging.warning(
                            'Could not populate table, retrying. Attempt {} of {}.'.format(
                                num_tries, max_tries))
                        break
                    time.sleep(1)
                if success:
                    break
            if not success:
                raise RuntimeError('Max number of retries reached.')

        self.create_table_from_query(select_all_from_tmp_table_query, table_path)
        # Delete the temporary table after the data is inserted.
        self.delete_table(tmp_table)

    def append_rows(self, table_path, data, columns=None, max_tries=None):
        """Appends the rows contained in data to the table at table_path.

        Args:
          table_path: A string of the form '<dataset id>.<table name>'.
          data: A list of rows, each of which is a list of values.
          columns: Optionally, a list of pairs (<column name>, <value type>) to describe the
              table's expected schema. If this is present, it will check the table's schema against
              the provided schema.
          max_tries: Optional number of tries for insert_data API call.

        Raises:
            RuntimeError: if the schema passed in as columns doesn't match the schema of the
                already-created table represented by table_path, or if the table doesn't exist,
                or if there are errors inserting the rows.
        """
        _, dataset_id, table_id = self.parse_table_path(table_path)
        table = self.gclient.dataset(dataset_id).table(table_id)
        self.reload_table(table)
        table_schema = [(x.name, x.field_type)
                        for x in self.get_schema(dataset_id, table_id, self.project_id)]

        if not self.table_exists(table):
            raise RuntimeError("The table " + table_id + " doesn't exist.")
        if columns is not None and table_schema != columns:
            raise RuntimeError("The incoming schema doesn't match the existing table's schema.")

        error_list = []
        start = 0
        num_rows = len(data)
        # Bigquery has a limit of max 10k to insert per request
        while start < num_rows:
            end = start + MAX_ROWS_TO_INSERT
            max_tries = max_tries or self.default_max_api_call_tries
            error_list.extend(execute_with_retries(lambda: table.insert_data(data[start:end]),
                                                   max_tries))
            start = end

        # If there are errors in the insert, the table will never be modified, so we want to surface
        # those errors. The returned value from table.insert_data is a list of dictionaries, where
        # the dictionary values contain a list of dictionaries with the actual errors so it's
        # complicated to get results in a readable format.
        if error_list:
            row_errors = []
            for error_entry in error_list:
                # The index entry of each dictionary contains the row number.
                line_number = error_entry['index']
                formatted_errors_list = []
                # The error entry of each dictionary contains a list of dictionaries containing
                # the actual errors.
                for err_dict in error_entry['errors']:
                    formatted_errors_list.append('{} for column {}'
                                                 .format(err_dict['message'],
                                                         err_dict['location']))
                row_errors.append('Line {}:\n {}'.format(line_number,
                                                         '\n\t'.join(formatted_errors_list)))

            raise RuntimeError('Could not insert rows. Reported errors:\n' + '\n'.join(row_errors))

    def _compare_schemas(self, schema_a, schema_b):
        if len(schema_a) != len(schema_b):
            raise RuntimeError('Schema {} is not the same length as schema {}.'
                               .format(str(schema_a), str(schema_b)))

        for i in range(len(schema_a)):
            if schema_a[0] != schema_b[0]:
                raise RuntimeError('Column index {} is named {} in the first schema, but {} in the'
                                   'second schema.'.format(i, schema_a[0], schema_b[0]))
            a_datatype = schema_a[1]
            b_datatype = schema_b[1]
            if a_datatype != b_datatype:
                for dt_class in self.DATATYPE_CLASSES:
                    if a_datatype in dt_class and b_datatype not in dt_class:
                        raise RuntimeError("For column {}, {} is not the same type as {}."
                                           .format(schema_a[0], a_datatype, b_datatype))

    def export_table_to_bucket(self,
                               table_path,
                               bucket_name,
                               dir_in_bucket='',
                               output_format='csv',
                               compression=False,
                               output_ext='',
                               max_wait_sec=DEFAULT_TIMEOUT_SEC):
        """Export a bigquery table to a file in the given bucket. The output file has the same name
           as the table.
        Args:
            table_path: Path of the table
            bucket_name: Name of the bucket to store the spreadsheet
            dir_in_bucket: The directory in the bucket to store the output files
            output_format: Format of output. It must be among 'csv', 'json' and 'avro'
            compression: Whether to use GZIP compression. Avro cannot be used with GZIP compression
            output_ext: An optional extention to output file. So that we can tell output files from
                        different exports
            max_wait_sec: Maximum time to wait. Export table to storage takes significantly longer
                          than query a table
        """
        # A mapping table from supported formats to bigquery required formats.
        bigquery_required_formats = {'csv': 'CSV', 'json': 'NEWLINE_DELIMITED_JSON', 'avro': 'AVRO'}

        if output_format not in bigquery_required_formats:
            raise ValueError('Invalid output format: {}. Must be among {}'.format(
                output_format, bigquery_required_formats.keys()))

        if compression and output_format == 'avro':
            raise ValueError('{} cannot be combined with GZIP compression'.format(output_format))

        project_id, dataset_id, table_name = self.parse_table_path(table_path)

        src_table = self._get_bq_table(table_name, dataset_id, project_id)

        # Generate the destination of the table content
        output_filename = table_name
        if output_ext:
            output_filename += '_' + output_ext
        output_filename += '.' + output_format
        if compression:
            output_filename += '.gz'
        path = os.path.join(dir_in_bucket, output_filename)

        destination = 'gs://{}/{}'.format(bucket_name, path.lstrip().lstrip('/'))

        # Export table content into a file in the bucket
        execute_with_retries(
            lambda: self._run_export_table_to_bucket_job(
                bigquery_required_formats, compression, destination, max_wait_sec, output_format,
                src_table), max_tries=self.default_max_api_call_tries)

    def _run_export_table_to_bucket_job(self, bigquery_required_formats, compression, destination,
                                        max_wait_sec, output_format, src_table):
        job_name = str(uuid.uuid4())
        job = self.gclient.extract_table_to_storage(job_name, src_table, destination)
        job.destination_format = bigquery_required_formats[output_format]
        job.compression = 'GZIP' if compression else 'NONE'

        job.begin()
        # This command is already wrapped in retries, so set max_tries to 1.
        wait_for_job(job, max_wait_sec=max_wait_sec, max_tries=1)

    def export_schema_to_bucket(self, table_path, bucket_name, dir_in_bucket='', output_ext=''):
        """Export a bigquery table's schema to a json file in the given bucket. The output file's
        name is <BQ table name>-schema.json
        Args:
            table_path: Path of the table
            bucket_name: Name of the bucket to store the spreadsheet. The bucket must be in project
                         self.project_id
            dir_in_bucket: The directory in the bucket to store the output files
            output_ext: An optional extention to output file. So that we can tell output files from
                        different exports
        """
        table_project, dataset_id, table_name = self.parse_table_path(table_path)

        # Generate the destination of the table schema
        schema_filename = table_name
        if output_ext:
            schema_filename += '_' + output_ext
        schema_filename += '-schema.json'
        schema_path = os.path.join(dir_in_bucket, schema_filename).lstrip().lstrip('/')

        # Export schema as a json file to the bucket
        schema = [
            OrderedDict([('name', field.name), ('type', field.field_type)])
            for field in self.get_schema(dataset_id, table_name, table_project)
        ]

        schema_blob = storage.blob.Blob(schema_path,
                                        storage.Client(self.project_id).bucket(bucket_name))

        schema_blob.upload_from_string(json.dumps(schema, indent=2, separators=(',', ':')))

    def delete_and_recreate_table(self, table):
        if self.table_exists(table):
            self.delete_table(table)
        self.create_table(table)
        self.reload_table(table)

    def dataset_exists(self, dataset, max_tries=None):
        # type: (dataset) -> bool
        """Checks if a dataset exists.

        Args:
            dataset: The BQ dataset object to check.
            max_tries: The maximum number of times to try calling the API before giving up. Defaults
                to self.default_max_api_call_tries.
        """
        max_tries = max_tries or self.default_max_api_call_tries
        return execute_with_retries(lambda: dataset.exists(), max_tries)

    def table_exists(self, table, max_tries=None):
        # type: (table) -> bool
        """Checks if a table exists.

        Args:
            table: The BQ table object to check.
            max_tries: The maximum number of times to try calling the API before giving up. Defaults
                to self.default_max_api_call_tries.
        """
        max_tries = max_tries or self.default_max_api_call_tries
        return execute_with_retries(lambda: table.exists(), max_tries)

    def delete_dataset(self, dataset, max_tries=None):
        # type: (dataset) -> None
        """Deletes a dataset.

        Args:
            dataset: The BQ dataset object to delete.
            max_tries: The maximum number of times to try calling the API before giving up. Defaults
                to self.default_max_api_call_tries.
        """
        max_tries = max_tries or self.default_max_api_call_tries
        return execute_with_retries(lambda: dataset.delete(), max_tries)

    def delete_table(self, table, max_tries=None):
        # type: (table) -> None
        """Deletes a table.

        Args:
            table: The BQ table object to delete.
            max_tries: The maximum number of times to try calling the API before giving up. Defaults
                to self.default_max_api_call_tries.
        """
        max_tries = max_tries or self.default_max_api_call_tries
        return execute_with_retries(lambda: table.delete(), max_tries)

    def create_dataset(self, dataset, max_tries=None):
        # type: (dataset) -> None
        """Creates a dataset.

        Args:
            dataset: The BQ dataset object to create.
            max_tries: The maximum number of times to try calling the API before giving up. Defaults
                to self.default_max_api_call_tries.
        """
        max_tries = max_tries or self.default_max_api_call_tries
        return execute_with_retries(lambda: dataset.create(), max_tries)

    def create_table(self, table, max_tries=None):
        # type: (table) -> None
        """Creates a table.

        Args:
            table: The BQ table object to representing the table to create.
            max_tries: The maximum number of times to try calling the API before giving up. Defaults
                to self.default_max_api_call_tries.
        """
        max_tries = max_tries or self.default_max_api_call_tries
        return execute_with_retries(lambda: table.create(), max_tries)

    def reload_dataset(self, dataset, max_tries=None):
        # type: (dataset) -> None
        """Reloads a dataset.

        Args:
            dataset: The BQ dataset object to reload.
            max_tries: The maximum number of times to try calling the API before giving up. Defaults
                to self.default_max_api_call_tries.
        """
        max_tries = max_tries or self.default_max_api_call_tries
        return execute_with_retries(lambda: dataset.reload(), max_tries)

    def reload_table(self, table, max_tries=None):
        # type: (table) -> None
        """Reloads a table.

        Args:
            table: The BQ table object to reload.
            max_tries: The maximum number of times to try calling the API before giving up. Defaults
                to self.default_max_api_call_tries.
        """
        max_tries = max_tries or self.default_max_api_call_tries
        return execute_with_retries(lambda: table.reload(), max_tries)

    def fetch_data_from_table(self, table, max_tries=None):
        # type: (table) -> List[tuple]
        """Fetches data from the given table.

        Args:
            table: The BQ table object representing the table from which to fetch data.
            max_tries: The maximum number of times to try calling the API before giving up. Defaults
                to self.default_max_api_call_tries.
        Returns:
            List of tuples, where each tuple is a row of the table.
        """
        max_tries = max_tries or self.default_max_api_call_tries
        return execute_with_retries(lambda: list(table.fetch_data()), max_tries)
