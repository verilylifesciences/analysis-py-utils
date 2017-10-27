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
"""Base class for a library for interacting with BigQuery."""

import datetime
import logging
import time

from google.cloud.exceptions import InternalServerError, ServiceUnavailable


# Bigquery uses . to separate project, dataset, and table parts.
TABLE_PATH_DELIMITER = '.'

# The call to datasets to list all tables requires you to set a maximum number of tables
# (or use the unspecified API default). We always want to list all the tables so we set
# a really high number for it. If ever there would be a dataset with more than 5000 tables,
# this constant should be adjusted. Also applies to max datasets to list in a project.
MAX_TABLES = 5000

# Constants for classes of datatypes. Some Bigquery functions accept or return legacy SQL types
# while others accept or return standard SQL types. These sets map equivalences between them.
INTEGER_CLASS = set(['INTEGER', 'INT64'])
FLOAT_CLASS = set(['FLOAT', 'FLOAT64'])
TIME_CLASS = set(['TIMESTAMP', 'DATE', 'TIME', 'DATETIME'])
BOOLEAN_CLASS = set(['BOOLEAN', 'BOOL'])
DATATYPE_CLASSES = [INTEGER_CLASS, FLOAT_CLASS, TIME_CLASS, BOOLEAN_CLASS]

# Default maximum number of tries for BQ API calls.
DEFAULT_MAX_API_CALL_TRIES = 3


class TimeoutException(Exception):
    def __init__(self, message):
        super(TimeoutException, self).__init__(message)


class BigqueryBaseClient(object):
    """Stores credentials and pointers to a BigQuery project.

    Args:
      project_id: The id of the project to associate with the client.
      default_dataset: If specified, use this dataset as the default.
      maximum_billing_tier: The maximum billing tier of this client.
      default_max_api_call_tries: The maximum number of tries for any REST API call.
    """

    def __init__(self, project_id, default_dataset=None, maximum_billing_tier=None):
        self.maximum_billing_tier = maximum_billing_tier
        self.project_id = project_id
        self.dataset = default_dataset

    def get_query_results(self, query, max_results=None, use_legacy_sql=False, max_wait_sec=60):
        """Returns a list or rows, each of which is a list of values.

        Args:
          query: A string with a complete SQL query.
          max_results: Maximum number of rows to return. If None, return all rows
          use_legacy_sql: Whether to use legacy SQL
          max_wait_sec: The maximum number of seconds to wait for the query to complete.
              Default is 60.
        Returns:
          A list of lists of values.
        """
        raise NotImplementedError("get_query_results is not implemented.")

    def create_table_from_query(self,
                                query,
                                table_path,
                                write_disposition='WRITE_EMPTY',
                                use_legacy_sql=False,
                                max_wait_sec=60,
                                expected_schema=None):
        """Creates a table in BigQuery from a specified query.

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
        raise NotImplementedError("create_table_from_query is not implemented.")

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
        raise NotImplementedError("create_tables_from_dict is not implemented.")

    def create_dataset_by_name(self, name, expiration_hours=None):
        """Create a new dataset within the current project.

        Args:
          name: The name of the new dataset.
          expiration_hours: The default expiration time for tables within the dataset.
        """
        raise NotImplementedError("create_dataset_by_name is not implemented.")

    def delete_dataset_by_name(self, name, delete_all_tables=False):
        """Delete a dataset within the current project.

        Args:
          name: The name of the dataset to delete.
          delete_all_tables: If True, will delete all tables in the dataset before attempting to
              delete the dataset. You can't delete a dataset until it contains no tables.
        """
        raise NotImplementedError("delete_dataset_by_name is not implemented.")

    def delete_table_by_name(self, table_path):
        """Delete a table within the current project.

        Args:
          table_path: A string of the form '<dataset id>.<table name>'.
        """
        raise NotImplementedError("delete_table_by_name is not implemented.")

    def tables(self, dataset_id):
        """Returns a list of table names in a given dataset.

        Args:
          dataset_id: The dataset to query.

        Returns:
          A list of table names (strings).
        """
        raise NotImplementedError("tables is not implemented.")

    def get_schema(self, dataset_id, table_name, project_id=None):
        """Returns the schema of a table.

        Args:
          dataset_id: The dataset to query.
          table_name: The name of the table.
          project_id: The project ID of the table.
        Returns:
          A list of tuples (column_name, value_type)
        """
        raise NotImplementedError("get_schema is not implemented.")

    def get_datasets(self):
        """Returns a list of dataset ids in the current project.

        Returns:
          A list of dataset ids/names (strings).
        """
        raise NotImplementedError("get_datasets is not implemented.")

    def populate_table(self, table_path, columns, data=[], max_wait_sec=60, max_tries=1):
        """Create a table and populate it with a list of rows.

        Args:
            table_path: A string of the form '<dataset id>.<table name>'.
            columns: A list of pairs (<column name>, <value type>).
            data: A list of rows, each of which is a list of values.
            max_wait_sec: The maximum number of seconds to wait for the table to be populated.
            max_tries: The maximum number of tries each time max_wait_sec is reached.
        """
        raise NotImplementedError("populate_table is not implemented.")

    def append_rows(self, table_path, data, columns=None):
        """Appends the rows contained in data to the table at table_path.

        Args:
          table_path: A string of the form '<dataset id>.<table name>'.
          data: A list of rows, each of which is a list of values.
          columns: Optionally, a list of pairs (<column name>, <value type>) to describe the
              table's expected schema. If this is present, it will check the table's schema against
              the provided schema.

        Raises:
            RuntimeError: if the schema passed in as columns doesn't match the schema of the
                already-created table represented by table_path, or if the table doesn't exist.
        """
        raise NotImplementedError("append_rows is not implemented.")

    def export_table_to_bucket(self,
                               table_path,
                               bucket_name,
                               dir_in_bucket='',
                               output_format='csv',
                               compression=False,
                               output_ext='',
                               max_wait_sec=600):
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
        raise NotImplementedError("export_table_to_bucket is not implemented.")

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
        raise NotImplementedError("export_schema_to_bucket is not implemented.")

    def parse_table_path(self, table_path, delimiter=TABLE_PATH_DELIMITER, replace_dashes=False):
        """Parses a path to a Bigquery Table into a project id, dataset and table name.

        If the project id is left out of the path, the project id that was passed into the class
        constructor is returned.

        Args:
            table_path: A table name or a path of the form 'dataset.table' or
            'project.dataset.table'.

        Returns:
            Strings project, dataset, table.
        """
        project_id = self.project_id
        dataset_id = self.dataset
        parts = table_path.split(delimiter)
        if self.dataset and len(parts) == 1:
            table_id = table_path
        elif len(parts) == 2:
            dataset_id, table_id = parts
        elif len(parts) == 3:
            project_id, dataset_id, table_id = parts
        else:
            raise RuntimeError('Invalid Bigquery path: ' + table_path)

        if replace_dashes:
            project_id = project_id.replace('-', '_')

        return project_id, dataset_id, table_id

    def path(self,
             table_path,
             dataset_id=None,
             project_id=None,
             delimiter=TABLE_PATH_DELIMITER,
             replace_dashes=False):
        """Extend a table name to a full Bigquery path using the default project and dataset.

        Args:
            table_name: The name of the table, possibly without project or dataset.
            dataset_id: Optional name of a dataset.
            project_id: Optional name of a project.

        Returns:
            A complete path of the form project.dataset.table_name
        """
        if not dataset_id:
            dataset_id = self.dataset
        if not project_id:
            project_id = self.project_id

        if replace_dashes:
            project_id = project_id.replace('-', '_')

        parts = table_path.split(delimiter)
        if len(parts) == 3:
            return table_path
        elif len(parts) == 2:
            return '{}{}{}'.format(project_id, delimiter, table_path)
        elif len(parts) == 1:
            return '{}{}{}{}{}'.format(project_id, delimiter, dataset_id, delimiter, table_path)
        raise RuntimeError('Invalid Bigquery path: ' + table_path)

    def dataset_exists(self, dataset):
        # type: (dataset) -> bool
        """Checks if a dataset exists.

        Args:
            dataset: The BQ dataset object to check.
        """
        raise NotImplementedError('dataset_exists is not implemented.')

    def table_exists(self, table):
        # type: (table) -> bool
        """Checks if a table exists.

        Args:
            table: The BQ table object to check.
        """
        raise NotImplementedError('table_exists is not implemented.')

    def delete_dataset(self, dataset):
        # type: (dataset) -> None
        """Deletes a dataset.

        Args:
            dataset: The BQ dataset object to delete.
        """
        raise NotImplementedError('delete_dataset is not implemented.')

    def delete_table(self, table):
        # type: (table) -> None
        """Deletes a table.

        Args:
            table: The BQ table object to delete.
        """
        raise NotImplementedError('delete_table is not implemented.')

    def create_dataset(self, dataset):
        # type: (dataset) -> None
        """Creates a dataset.

        Args:
            dataset: The BQ dataset object to create.
        """
        raise NotImplementedError('create_dataset is not implemented.')

    def create_table(self, table):
        # type: (table) -> None
        """Creates a table.

        Args:
            table: The BQ table object to representing the table to create.
        """
        raise NotImplementedError('create_table is not implemented.')

    def reload_dataset(self, dataset):
        # type: (dataset) -> None
        """Reloads a dataset.

        Args:
            dataset: The BQ dataset object to reload.
        """
        raise NotImplementedError('reload_dataset is not implemented.')

    def reload_table(self, table):
        # type: (table) -> None
        """Reloads a table.

        Args:
            table: The BQ table object to reload.
        """
        raise NotImplementedError('reload_table is not implemented.')

    def fetch_data_from_table(self, table):
        # type: (table) -> List[tuple]
        """Fetches data from the given table.

        Args:
            table: The BQ table object representing the table from which to fetch data.
        Returns:
            List of tuples, where each tuple is a row of the table.
        """
        raise NotImplementedError('fetch_data_from_table is not implemented.')


def wait_for_job(job, max_wait_sec=None, query="", max_tries=DEFAULT_MAX_API_CALL_TRIES):
    """Returns when a given job is marked complete.

    Args:
        job: A gcloud._AsyncJob.
        query: Optionally, the query this job is executing.
        max_tries: Maximum number of tries for the call to check the job status.

    Raises:
       TimeoutException: If the job takes longer than max_wait_secs.
    """

    # If no default is specified then set it to 10 minutes
    if not max_wait_sec:
        max_wait_sec = 600

    start_time = datetime.datetime.utcnow()
    deadline = start_time + datetime.timedelta(seconds=max_wait_sec)
    while datetime.datetime.utcnow() < deadline:
        if is_job_done(job, query, max_tries):
            return True
        time.sleep(1)
    raise TimeoutException('Job ' + job.name + ' timed out.')


def is_job_done(job, query="", max_tries=DEFAULT_MAX_API_CALL_TRIES):
    """Returns True only if the job passed in is finished.

    Args:
        job: A gcloud._AsyncJob.
        query: Optionally, the query this job is executing.
        max_tries: Maximum number of tries for the call to check the job status.

    Returns:
       True if the job is in state DONE, False otherwise.

    Raises:
        RuntimeError: If the job finished and returned an error result.
    """
    execute_with_retries(lambda: job.reload(), max_tries)
    if job.state == 'DONE':
        if job.error_result:
            # The errors the job returns are a list of dictionaries. We treat it as a string
            # so we can put it in the exception message.
            msg = str(job.errors)
            if query:
                # This crazyness puts line numbers next to the SQL.
                lines = query.split('\n')
                longest = max(len(l) for l in lines)
                # Print out a 'ruler' above and below the SQL so we can judge columns.
                ruler = ' ' * 4 + '|'  # Left pad for the line numbers (4 digits plus ':')
                for _ in range(longest / 10):
                    ruler += ' ' * 4 + '.' + ' ' * 4 + '|'
                header = '-----Offending Sql Follows-----'
                padding = ' ' * ((longest - len(header)) / 2)
                msg += '\n\n{}{}\n\n{}\n{}\n{}'.format(padding, header, ruler, '\n'.join(
                    '{:4}:{}'.format(n + 1, line) for n, line in enumerate(lines)), ruler)
            raise RuntimeError(msg)
        return True
    return False


def execute_with_retries(fn_to_execute, max_tries=DEFAULT_MAX_API_CALL_TRIES):
    """Execute the given function with retries.

    Retry on common unpredictable BigQuery backend failures.

    Args:
        fn_to_execute: The function to execute with retries.
        max_tries: The maximum number of total tries.
    Returns:
        Return values of fn_to_execute
    Raises:
        InternalServerError: if max_tries is reached.
    """
    tries = 0
    while tries < max_tries:
        try:
            return fn_to_execute()
        except (InternalServerError, ServiceUnavailable) as e:
            tries += 1
            logging.warning('Server error encountered, retrying. Attempt {} of {}. '
                            'Error message: {}'.format(tries, max_tries, e))
    raise InternalServerError('Maximum number of retries exceeded.')
