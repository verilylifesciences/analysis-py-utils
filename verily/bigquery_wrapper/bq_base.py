# Copyright 2019 Verily Life Sciences Inc. All Rights Reserved.
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

# Workaround for https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2366
from __future__ import absolute_import

import logging

from google.api_core.future import polling
from google.api_core.retry import Retry
from google.cloud.bigquery import retry as bq_retry

# Bigquery uses . to separate project, dataset, and table parts.
BQ_PATH_DELIMITER = '.'

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

# Values in the QueryJob.error_result dict that correspond to query validation errors.
VALIDATION_ERROR_REASONS = ['invalidQuery', 'notFound', 'duplicate']

# The default timeout for any BigQuery operations executed in real BQ.
DEFAULT_TIMEOUT_SEC = 1200


def _transient_string_in_exception_message(exc):
    # type: (Exception) -> bool
    """Determines whether an exception's message contains a common message for transient errors.

    The exception's message containing one of these substrings is sufficient to determine that it is
    transient, but there can be transient exceptions whose messages do not contain these substrings.
    """
    return ('The job encountered an internal error during execution' in str(exc) or
            'Retrying the job may solve the problem' in str(exc))

# Retry object for errors encountered in making API calls (executing jobs, etc.)
DEFAULT_RETRY_FOR_API_CALLS = Retry(
    # The predicate takes an exception and returns whether it is transient.
    predicate=lambda exc: (bq_retry.DEFAULT_RETRY._predicate(exc) or
                           _transient_string_in_exception_message(exc)),
    deadline=DEFAULT_TIMEOUT_SEC)

# Retry object for errors encountered while polling jobs in progress.
# See https://github.com/googleapis/google-cloud-python/issues/6301
DEFAULT_RETRY_FOR_ASYNC_JOBS = Retry(
            # The predicate takes an exception and returns whether it is transient.
            predicate=lambda exc: (polling.DEFAULT_RETRY._predicate(exc) or
                                   _transient_string_in_exception_message(exc)),
    deadline=DEFAULT_TIMEOUT_SEC)


class BigqueryBaseClient(object):
    """Stores credentials and pointers to a BigQuery project.

    Args:
      project_id: The id of the project to associate with the client.
      default_dataset_id: If specified, use this dataset as the default.
      maximum_billing_tier: The maximum billing tier of this client.
      default_max_api_call_tries: The maximum number of tries for any REST API call.
    """

    def __init__(self, project_id, default_dataset_id=None, maximum_billing_tier=None):
        self.maximum_billing_tier = maximum_billing_tier
        self.project_id = project_id
        self.default_dataset_id = default_dataset_id

    def get_delimiter(self):
        """ Returns the delimiter used to separate project, dataset, and table in a table path. """
        raise NotImplementedError("get_delimiter is not implemented.")

    def get_query_results(self, query, use_legacy_sql=False, max_wait_secs=None):
        # type: (str, Optional[Bool], Optional[int]) -> List[Tuple[Any]]
        """Returns a list or rows, each of which is a tuple of values.

        Args:
            query: A string with a complete SQL query.
            use_legacy_sql: Whether to use legacy SQL
            max_wait_secs: The maximum number of seconds to wait for the query to complete. If not
                set, the class default will be used.

        Returns:
            A list of tuples of values.
        """
        raise NotImplementedError("get_query_results is not implemented.")

    def create_table_from_query(self,
                                query,  # type: str
                                table_path,  # type: str
                                write_disposition='WRITE_EMPTY',  # type: Optional[str]
                                use_legacy_sql=False,  # type: Optional[bool]
                                max_wait_secs=None,  # type: Optional[int]
                                expected_schema=None  # type: Optional[List[SchemaField]]
                                ):
        # type: (...) -> None
        """Creates a table in BigQuery from a specified query.

        Args:
          query: The query to run.
          table_path: The path to the table (in the client's project) to write
              the results to.
          write_disposition: Specifies behavior if table already exists. See options here:
              https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs under
              configuration.query.writeDisposition
          use_legacy_sql: Whether the query is written in standard or legacy sql.
          max_wait_secs: Seconds to wait for the query before timing out. If not
                set, the class default will be used.
          expected_schema: The expected schema of the resulting table (only required for mocking).
        """
        raise NotImplementedError("create_table_from_query is not implemented.")

    def create_tables_from_dict(self,
                                table_names_to_schemas,  # type: Dict[str, List[SchemaField]]
                                dataset_id=None,  # type: Optional[str]
                                replace_existing_tables=False  # type: Optional[bool]
                                ):
        # type: (...) -> None
        """Creates a set of tables from a dictionary of table names to their schemas.

        Args:
          table_names_to_schemas: A dictionary of:
            key: The table name.
            value: A list of SchemaField objects.
          dataset_id: The dataset in which to create tables. If not specified, use default dataset.
          replace_existing_tables: If True, delete and re-create tables. Otherwise, checks to see
              if any of the requested tables exist. If they do, it will raise a RuntimeError.

        Raises:
            RuntimeError if replace_existing_tables is False and any of the tables requested for
                creation already exist
        """
        raise NotImplementedError("create_tables_from_dict is not implemented.")

    def create_dataset_by_name(self, name, expiration_hours=None):
        # type: (str, Optional[float]) -> None
        """Create a new dataset within the current project.

        Args:
          name: The name of the new dataset.
          expiration_hours: The default expiration time for tables within the dataset.
        """
        raise NotImplementedError("create_dataset_by_name is not implemented.")

    def delete_dataset_by_name(self, name, delete_all_tables=False):
        # type: (str, Optional[bool]) -> None
        """Delete a dataset within the current project.

        Args:
          name: The name of the dataset to delete.
          delete_all_tables: If True, will delete all tables in the dataset before attempting to
              delete the dataset. You can't delete a dataset until it contains no tables.

        Raises:
            RuntimeError if there are still tables in the dataset and you try to delete it (with
                delete_all_tables set to False)
        """
        raise NotImplementedError("delete_dataset_by_name is not implemented.")

    def delete_table_by_name(self, table_path):
        # type: (str) -> None
        """Delete a table within the current project.

        Args:
          table_path: A string of the form '<dataset id>.<table name>' or
              '<project id>.<dataset_id>.<table_name>'
        """
        raise NotImplementedError("delete_table_by_name is not implemented.")

    def tables(self, dataset_id):
        # type: (str) -> List[str]
        """Returns a list of table names in a given dataset.

        Args:
          dataset_id: The name of the dataset to query.

        Returns:
          A list of table names (strings).
        """
        raise NotImplementedError("tables is not implemented.")

    def get_schema(self, dataset_id, table_name, project_id=None):
        # type: (str, str, Optional[str]) -> List[SchemaField]
        """Returns the schema of a table.

        Args:
          dataset_id: The dataset to query.
          table_name: The name of the table.
          project_id: The project ID of the table.
        Returns:
          A list of SchemaFields representing the schema.
        """
        raise NotImplementedError("get_schema is not implemented.")

    def get_datasets(self):
        # type: (None) -> List[str]
        """Returns a list of dataset ids in the default project.

        Returns:
            A list of dataset ids/names (strings).
        """
        raise NotImplementedError("get_datasets is not implemented.")

    def populate_table(self, table_path, schema, data=[], make_immediately_available=False,
                       replace_existing_table=False):
        # type: (str, List[SchemaField], Optional[List[Any]], Optional[bool], Optional[bool]) -> None
        """Creates a table and populates it with a list of rows.

        If make_immediately_available is False, the table will be created using streaming inserts.
        Note that streaming inserts are immediately available for querying, but not for exporting or
        copying, so if you need that capability you should set make_immediately_available to True.
        https://cloud.google.com/bigquery/streaming-data-into-bigquery

        If the table is already created, it will raise a RuntimeError, unless replace_existing_table
        is True.

        Args:
          table_path: A string of the form '<dataset id>.<table name>'
              or '<project id>.<dataset id>.<table name>'.
          schema: A list of SchemaFields to represent the table's schema.
          data: A list of rows, each of which corresponds to a row to insert into the table.
          make_immediately_available: If False, the table won't immediately be available for
              copying or exporting, but will be available for querying. If True, after this
              operation returns, it will be available for copying and exporting too.
          replace_existing_table: If set to True, the table at table_path will be deleted and
              recreated if it's already present.

        Raises:
            RuntimeError if the table at table_path is already there and replace_existing_table
                is False
        """
        raise NotImplementedError("populate_table is not implemented.")

    #TODO(Issue 9): Implement a make_immediately_available flag for this function.
    def append_rows(self, table_path, data, schema=None):
        # type: (str, List[Tuple[Any]], Optional[List[SchemaField]]) -> None
        """Appends the rows contained in data to the table at table_path.
        Args:
          table_path: A string of the form '<dataset id>.<table name>'
              or '<project id>.<dataset id>.<table name>'.
          data: A list of rows, each of which is a list of values.
          schema: Optionally, a list of SchemaFields to describe the
              table's expected schema. If this is present, it will check the table's schema against
              the provided schema.

        Raises:
            RuntimeError: if the schema passed in as columns doesn't match the schema of the
                already-created table represented by table_path, or if the table doesn't exist,
                or if there are errors inserting the rows.
        """
        raise NotImplementedError("append_rows is not implemented.")

    def copy_table(self, source_table_path,  # type: str
                   destination_table_name,  # type: str
                   destination_dataset=None,  # type: Optional[str]
                   destination_project=None,  # type: Optional[str]
                   replace_existing_table=False  # type: bool
                   ):
        # type: (...) -> None
        """
        Copies the table at source_table_path to the location
        destination_project.destination_dataset.destination_table_name. If the destination project
        or dataset aren't set, the class default will be used.

        Args:
            source_table_path: The path of the table to copy.
            destination_table: The name of the table to copy to.
            destination_dataset: The name of the destination dataset. If unset, the client default
                dataset will be used.
            destination_project: The name of the destination project. If unset, the client default
                project will be used.
            replace_existing_table: If True, if the destination table already exists, it will delete
                it and copy the source table in its place.

        Raises:
            RuntimeError if the destination table already exists and replace_existing_table is False
            or the destination dataset does not exist
        """
        raise NotImplementedError("copy_table is not implemented.")

    def export_table_to_bucket(self,
                               table_path,  # type: str
                               bucket_name,  # type: str
                               dir_in_bucket='',  # type: Optional[str]
                               output_format='csv',  # type: Optional[str]]
                               compression=False,  # type: Optional[bool]
                               output_ext='',  # type: Optional[str]
                               max_wait_secs=None,  # type: Optional[int]
                               support_multifile_export=True  #type: bool
                               ):
        # type: (...) -> None
        """
        Export a BigQuery table to a file in the given bucket. The output file has the same name
        as the table.

        Args:
            table_path: Path of the table
            bucket_name: Name of the bucket to store the spreadsheet
            dir_in_bucket: The directory in the bucket to store the output files
            output_format: Format of output. It must be among 'csv', 'json' and 'avro'
            compression: Whether to use GZIP compression. Avro cannot be used with GZIP compression
            output_ext: An optional extension to output file. So that we can tell output files from
                different exports
            max_wait_secs: Maximum time to wait. Export table to storage takes significantly longer
                than query a table. If not set, it will use the class default.
            support_multifile_export: If True, and the table is large enough, then the table will be
                exported as several files suffixed with a shard number. If False, it will be exported
                as a single file.
        Raises:
            RuntimeError if there is a problem with the export job.
        """
        raise NotImplementedError("export_table_to_bucket is not implemented.")

    def export_schema_to_bucket(self,
                                table_path,  # type: str
                                bucket_name,  # type: str
                                dir_in_bucket='',  # type: Optional[str]
                                output_ext=''  # type: Optional[str]]
                                ):
        # type: (...) -> None
        """
        Export a BigQuery table's schema to a json file in the given bucket. The output file's
        name is <BQ table name>-schema.json

        Args:
            table_path: Path of the table
            bucket_name: Name of the bucket to store the spreadsheet. The bucket must be in project
                self.project_id
            dir_in_bucket: The directory in the bucket to store the output files
            output_ext: An optional extension to output file. So that we can tell output files from
                different exports
        """
        raise NotImplementedError("export_schema_to_bucket is not implemented.")

    def import_table_from_bucket(self,
                                 table_path,  # type: str
                                 source_uri,  # type: str
                                 schema=None, # type: List[SchemaField]
                                 input_format='csv',  # type: Optional[str]]
                                 write_disposition='WRITE_APPEND',  # type: str
                                 skip_leading_row=False, # type: bool
                                 max_wait_secs=None  # type: Optional[int]
                                ):
        # type: (...) -> None
        """
        Imports data from a file in a bucket to a BigQuery table.

        Args:
            table_path: The path to the table that the data should be loaded in. If the table
                doesn't already exist, it will be created.
            source_uri: The URI for the file in the bucket to load.
            schema: The BigQuery schema for the data. If not provided, BigQuery will try to infer.
            input_format: The format of the input file. Can be 'csv', 'avro', or 'json'.
            write_disposition: The write disposition to use, see writeDisposition on this page:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
            skip_leading_row: If True, it will skip the first row of data in the
                file (presumably a header)
            max_wait_secs: The amount of time to  wait for the table to import. This operation can
                be slower than other operations in this class. If unset, it will use the
                class default timeout.

        Raises:
            RuntimeError if there is a problem executing the load job.
        """
        raise NotImplementedError('import_table_from_bucket is not implemented.')

    def import_table_from_file(self,
                               table_path,  # type: str
                               opened_source_file,  # type: File
                               schema,  # type: List[SchemaField]
                               input_format='csv',  # type: Optional[str]]
                               write_disposition='WRITE_APPEND',
                               skip_leading_row=False,  # type: bool
                               max_wait_secs=None  # type: Optional[int]
                              ):
        # type: (...) -> None
        """
        Imports data from a local file to a BigQuery table.

        Args:
            table_path: The path to the table that the data should be loaded in. If the table
                doesn't already exist, it will be created.
            opened_source_file: The source file, already opened in read-binary mode.
            schema: The BigQuery schema for the data. If unset, BigQuery will try to infer.
            input_format: The format of the input file. Can be 'csv', 'avro', or 'json'.
            write_disposition: The write disposition to use, see writeDisposition on this page:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
            skip_leading_row: If True, it will skip the first row of data in the
                file (presumably a header)
            max_wait_secs: The amount of time to  wait for the table to import. This operation can
                be slower than other operations in this class. If unset, it will use the
                class default timeout.

        Raises:
            RuntimeError if there is a problem executing the load job.
        """
        raise NotImplementedError("import_table_from_file is not implemented.")

    def parse_table_path(self,
                         table_path,  # type: str
                         delimiter=None,  # type: Optional[str]
                         replace_dashes=False  # type: Optional[bool]
                         ):
        # type: (...) -> (str, str, str)
        """Parses a path to a Bigquery Table into a project id, dataset and table name.

        If the project id is left out of the path, the project id that was passed into the class
        constructor is returned.

        Args:
            table_path: A table name or a path of the form 'dataset.table' or
                'project.dataset.table'.
            delimiter: The delimiter used in the table path
            replace_dashes: Whether to replace dashes with underscores for BigQuery compatibility.

        Returns:
            Strings project, dataset, table.
        """
        # If the delimiter isn't passed in, use the default for the implementation.
        if not delimiter:
            delimiter = self.get_delimiter()

        project_id = self.project_id
        dataset_id = self.default_dataset_id
        parts = table_path.split(delimiter)
        if self.default_dataset_id and len(parts) == 1:
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
             table_path,  # type: str
             dataset_id=None,  # type: Optional[str]
             project_id=None,  # type: Optional[str]
             delimiter=None,  # type: Optional[str]
             replace_dashes=False  # type: Optional[str]
             ):
        """Extend a table name to a full BigQuery path using the default project and dataset.

        Args:
            table_path: The partial or full table path.
            dataset_id: Optional name of a dataset.
            project_id: Optional name of a project.
            delimiter: The delimiter used in the table path
            replace_dashes: Whether to replace dashes with underscores for BigQuery compatibility.

        Returns:
            A complete path of the form project.dataset.table_name
        """

        # If the delimiter isn't passed in, use the default for the implementation.
        if not delimiter:
            delimiter = self.get_delimiter()

        if not dataset_id:
            dataset_id = self.default_dataset_id
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

    def dataset_exists(self,
                       dataset  # type: Dataset, DatasetReference
                       ):
        # type: (...) -> bool
        """Checks if a dataset exists.

        Args:
            dataset: The BQ dataset object to check.
        """
        raise NotImplementedError('dataset_exists is not implemented.')

    def table_exists(self,
                     table  # type: TableReference, Table
                     ):
        # type: (...) -> bool
        """Checks if a table exists.

        Args:
            table: The TableReference or Table for the table to check whether it exists.
        """
        raise NotImplementedError('table_exists is not implemented.')

    def delete_dataset(self,
                       dataset  # type: Dataset, DatasetReference
                       ):
        # type: (...) -> None
        """Deletes a dataset.

        Args:
            dataset: The Dataset or DatasetReference to delete.
        """
        raise NotImplementedError('delete_dataset is not implemented.')

    def delete_table(self,
                     table  # type: Table, TableReference
                     ):
        # type: (...) -> None
        """Deletes a table.

        Args:
            table: The Table or TableReference to delete.
        """
        raise NotImplementedError('delete_table is not implemented.')

    def create_dataset(self,
                       dataset  # type: DatasetReference, Dataset
                       ):
        # type: (...) -> None
        """
        Creates a dataset.

        Args:
            dataset: The Dataset object to create.
        """
        raise NotImplementedError('create_dataset is not implemented.')

    def create_table(self,
                     table  # type: Table, TableReference
                     ):
        # type: (Table) -> None
        """
        Creates a table.

        Args:
            table: The Table or TableReference object to create. Note that if you pass a
                TableReference the table will be created with no schema.
        """
        raise NotImplementedError('create_table is not implemented.')

    def fetch_data_from_table(self,
                              table  # type: Table, TableReference
                              ):
        # type: (...) -> List[Tuple[Any]]
        """
        Fetches data from the given table.

        Args:
            table: The Table or TableReference object representing the table from which
                to fetch data.

        Returns:
            List of tuples, where each tuple is a row of the table.
        """
        raise NotImplementedError('fetch_data_from_table is not implemented.')

    @staticmethod
    def list_schema_differences(schema_a, schema_b):
        # type: (List[SchemaField], List[SchemaField]) -> List[str]
        """
        Compares two schemas and raises RuntimeErrors if there are any differences.

        Args:
            schema_a: A list of SchemaFields representing one table schema.
            schema_b: A list of SchemaFields representing another table schema.

        Returns:
            Empty list if there were no schema mismatches; otherwise a list of schema mismatches.
        """
        diff_list = []
        if len(schema_a) != len(schema_b):
            diff_list.append('Schema {} is not the same length as schema {}.'
                             .format(str(schema_a), str(schema_b)))

        for i in range(len(schema_a)):
            if schema_a[0] != schema_b[0]:
                diff_list.append('Column index {} is named {} in the first schema, but {} in the'
                                 'second schema.'.format(i, schema_a[0], schema_b[0]))
            a_datatype = schema_a[1]
            b_datatype = schema_b[1]
            if a_datatype != b_datatype:
                for dt_class in DATATYPE_CLASSES:
                    if a_datatype in dt_class and b_datatype not in dt_class:
                        diff_list.append("For column {}, {} is not the same type as {}."
                                         .format(schema_a[0], a_datatype, b_datatype))
        return diff_list

    def _raise_if_tables_exist(self, tables, dataset_id=None):
        """
        Raises a RuntimeError if any table in the list tables appears in the relevant dataset.

        Args:
            tables: A list of table IDs
            dataset: The dataset to check to see if the tables exist in. If not set, it will check
                self.default_dataset_id.

        Raises:
            RuntimeError if any table in tables appears in the dataset.
        """
        intersect = (set(tables)
                     .intersection(self.tables(dataset_id or self.default_dataset_id)))
        if len(intersect):
            raise RuntimeError('The tables {} that you requested to create already exist in '
                               'the dataset {}.',
                               ','.join(intersect), dataset_id or self.default_dataset_id)


def is_job_done(job,  # type: google.cloud.bigquery.job.QueryJob
                query="",  # type: Optional[str]
                max_wait_secs=None  # type: int
                ):
    # type: (...) -> bool
    """Returns True only if the query job passed in is finished.

    Args:
        job: A gcloud.QueryJob.
        query: Optionally, the query this job is executing.
        max_wait_secs: The number of seconds to wait for the job to finish before raising an error.
            If None, uses the default timeout.
    Returns:
        True if the job is in state DONE, False otherwise.
    Raises:
        RuntimeError: If the job finished and returned an error result.
        google.api_core.exceptions.RetryError: If the job did not finish in the designated timeout.
    """
    retry = DEFAULT_RETRY_FOR_API_CALLS
    if max_wait_secs:
        retry = retry.with_deadline(max_wait_secs)
    if job.done(retry=retry):
        if query:
            validate_query_job(job, query)
        return True
    return False


def validate_query_job(query_job, query):
    # type: (google.cloud.bigquery.job.QueryJob, str) -> None
    """If the given query job has errors, raises a RuntimeError with the pretty-printed query and
    the errors.

    Args:
        query_job: A gcloud.QueryJob.
        query: The query this job is executing.
    Raises:
        RuntimeError: If the job finished and returned an error result.
    """
    if query_job.done(retry=DEFAULT_RETRY_FOR_API_CALLS) and query_job.error_result:
        if query_job.error_result['reason'] in VALIDATION_ERROR_REASONS:  # validation error
            msg = str(query_job.errors)
            # This craziness puts line numbers next to the SQL.
            lines = query.split('\n')
            longest = max(len(l) for l in lines)
            # Print out a 'ruler' above and below the SQL so we can judge columns.
            ruler = ' ' * 4 + '|'  # Left pad for the line numbers (4 digits plus ':')
            for _ in range(longest // 10):
                ruler += ' ' * 4 + '.' + ' ' * 4 + '|'
            header = '-----Offending Sql Follows-----'
            padding = ' ' * ((longest - len(header)) // 2)
            msg += '\n\n{}{}\n\n{}\n{}\n{}'.format(padding, header, ruler, '\n'.join(
                '{:4}:{}'.format(n + 1, line) for n, line in enumerate(lines)), ruler)
            raise RuntimeError(msg)
        else:
            logging.warning('validate_query_job caught a non-validation error: {}'.format(
                str(query_job.errors)))
