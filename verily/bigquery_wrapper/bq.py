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
"""

# Workaround for https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2366
from __future__ import absolute_import

import cStringIO
import csv
import json
import logging
import os
from collections import OrderedDict

from typing import List  # noqa: F401

from google.api_core import retry
from google.cloud import bigquery, storage
from google.cloud.bigquery.dataset import Dataset, DatasetReference
from google.cloud.bigquery.job import ExtractJobConfig, LoadJobConfig, QueryJobConfig
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table, TableReference
from google.cloud.exceptions import (BadGateway, InternalServerError, NotFound, ServiceUnavailable,
                                     TooManyRequests)
from verily.bigquery_wrapper.bq_base import MAX_TABLES, BigqueryBaseClient, BQ_PATH_DELIMITER

# This is the default timeout for any BigQuery operations executed in this file, if no timeout is
# specified in the constructor.
DEFAULT_TIMEOUT_SEC = 600

# Bigquery has a limit of max 10000 rows to insert per request
MAX_ROWS_TO_INSERT = 10000


class Client(BigqueryBaseClient):
    """Stores credentials and pointers to a BigQuery project.

    Args:
      project_id: The id of the project to associate with the client.
      default_dataset: Optional. The default dataset to use for operations if none is specified.
      maximum_billing_tier: Optional. The maximum billing tier to use for operations.
      max_wait_secs: Optional. The amount of time to keep retrying operations, or to wait on an
          operation to finish. If not set, will default to DEFAULT_TIMEOUT_SEC
    """

    def __init__(self, project_id, default_dataset=None, maximum_billing_tier=None,
                 max_wait_secs=DEFAULT_TIMEOUT_SEC):
        self.gclient = bigquery.Client(project=project_id)
        self.max_wait_secs = max_wait_secs
        self.default_retry = retry.Retry(
            predicate=retry.if_exception_type(
                (InternalServerError, TooManyRequests, ServiceUnavailable, BadGateway)),
            deadline=max_wait_secs)
        super(Client, self).__init__(project_id, default_dataset, maximum_billing_tier)

    def get_delimiter(self):
        """ Returns the delimiter used to separate project, dataset, and table in a table path. """
        return BQ_PATH_DELIMITER

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
        config = QueryJobConfig()
        if self.maximum_billing_tier:
            config.maximum_billing_tier = self.maximum_billing_tier

        config.use_legacy_sql = use_legacy_sql

        query_job = self.gclient.query(query, job_config=config, retry=self.default_retry)

        rows = query_job.result(retry=self.default_retry,
                                timeout=max_wait_secs or self.max_wait_secs)
        return [x.values() for x in list(rows)]

    def get_table_reference_from_path(self, table_path):
        # type: (str) -> TableReference
        """
        Returns a TableReference for a given path to a BigQuery table.

        Args:
            table_path: A BigQuery table path in the form project.dataset.table

        Returns:
            A TableReference for the table specified by the path
        """
        project, dataset, table = self.parse_table_path(table_path)
        dataset_ref = DatasetReference(project, dataset)
        return TableReference(dataset_ref, table)

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
          expected_schema: The expected schema of the resulting table; unused in this implementation
        """

        if write_disposition not in ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']:
            raise ValueError('write_disposition must be one of WRITE_TRUNCATE, '
                             'WRITE_APPEND, or WRITE_EMPTY')

        config = QueryJobConfig()
        if self.maximum_billing_tier:
            config.maximum_billing_tier = self.maximum_billing_tier
        config.use_legacy_sql = use_legacy_sql
        config.write_disposition = write_disposition
        config.allow_large_results = True

        config.destination = self.get_table_reference_from_path(table_path)

        query_job = self.gclient.query(query, job_config=config, retry=self.default_retry)

        return query_job.result(timeout=max_wait_secs or self.max_wait_secs)

    def create_tables_from_dict(self,
                                table_names_to_schemas,  # type: Dict[str, List[SchemaField]]
                                dataset_id=None,  # type: Optional[str]
                                replace_existing_tables=False,  # type: Optional[bool]
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
        dataset_id = dataset_id or self.default_dataset_id
        dataset_ref = DatasetReference(self.project_id, dataset_id)

        # If the flag isn't set to replace existing tables, raise an error if any tables we're
        # trying to create already exist.
        if not replace_existing_tables:
            self._raise_if_tables_exist(table_names_to_schemas.keys(), dataset_id)

        for name, schema in table_names_to_schemas.iteritems():
            table_ref = TableReference(dataset_ref, name)
            # Use the Table object so it retains its schema.
            table = bigquery.Table(table_ref, schema=schema)

            if self.table_exists(table) and replace_existing_tables:
                self.delete_table(table)
            self.create_table(table)

    def create_dataset_by_name(self, name, expiration_hours=None):
        # type: (str, Optional[float]) -> None
        """Create a new dataset within the current project.

        Args:
          name: The name of the new dataset.
          expiration_hours: The default expiration time for tables within the dataset.
        """
        if name not in self.get_datasets():
            # Initialize the Dataset instead of passing a reference so we can set expiration hours.
            dataset = Dataset(DatasetReference(self.project_id, str(name)))
            if expiration_hours:
                dataset.default_table_expiration_ms = expiration_hours * (60 * 60 * 1000)
            self.create_dataset(dataset)
        else:
            logging.warning('Dataset {} already exists.'.format(name))

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

        dataset_ref = DatasetReference(self.project_id, str(name))

        tables_in_dataset = self.gclient.list_tables(dataset_ref, retry=self.default_retry)
        if delete_all_tables:
            for table_list_item in tables_in_dataset:
                self.delete_table(table_list_item.reference)
        elif tables_in_dataset.num_items > 0:
            raise RuntimeError("Dataset {} still contains {} tables so you can't delete it."
                               .format(name, str(tables_in_dataset)))

        self.delete_dataset(dataset_ref)

    def delete_table_by_name(self, table_path):
        # type: (str) -> None
        """Delete a table.

        Args:
          table_path: A string of the form '<dataset id>.<table name>' or
              '<project id>.<dataset_id>.<table_name>'
        """

        self.delete_table(self.get_table_reference_from_path(table_path))

    def tables(self, dataset_id):
        # type: (str) -> List[str]
        """Returns a list of table names in a given dataset.

        Args:
          dataset_id: The name of the dataset to query.

        Returns:
          A list of table names (strings).
        """
        dataset_ref = DatasetReference(self.project_id, dataset_id)

        tables = self.gclient.list_tables(dataset_ref, retry=self.default_retry)
        return [t.table_id for t in tables]

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

        dataset_ref = DatasetReference(project_id if project_id else self.project_id, dataset_id)
        table = self.gclient.get_table(TableReference(dataset_ref, table_name),
                                       retry=self.default_retry)

        return table.schema

    def get_datasets(self):
        # type: (None) -> List[str]
        """Returns a list of dataset ids in the default project.

        Returns:
            A list of dataset ids/names (strings).
        """
        return [x.dataset_id for x in self.gclient.list_datasets(max_results=MAX_TABLES,
                                                                 retry=self.default_retry)]

    @staticmethod
    def _convert_row_tuples_to_dicts(data, schema):
        # type: (Tuple[str], List[SchemaField]) -> List[Dict[str, Any]]
        """
        Converts data (in combination with the passed-in schema) into JSON for inserting.

        Args:
            data: A list of tuples, where each tuple represents a row of data. Each tuple should be
                the same length as 'schema', and should list its columns in the same order 'schema'
                does.
            schema: A list of SchemaFields representing the schema of the rows passed in as data.

        Returns:
            A list of dictionaries, where each dictionary is a representation of a table row.

        Raises:
            RuntimeError if the length of any row is different than the length of the schema
        """
        dict_rows = []
        for row in data:
            if len(row) != len(schema):
                raise RuntimeError('The row {} has the wrong number of values for the schema {}.'
                                   .format(str(row), str(schema)))

            row_dict = {schema[i].name: val for i, val in enumerate(row)}
            dict_rows.append(row_dict)

        return dict_rows

    def _stream_chunks_of_rows(self, table, data, table_schema):
        # type: (Table, List[Tuple[Any]], List[SchemaField]) -> None
        """
        Does a streaming insert of data into the table (given the table_schema). Note that streaming
        inserts are immediately available for querying, but not for exporting or copying.
        https://cloud.google.com/bigquery/streaming-data-into-bigquery

        This function chunks the data into portions below the import rate limit and inserts them
        chunk by chunk.

        Args:
            table: Name of the table to insert into
            data: List of tuples, where each tuple represents a row of data to insert
            table_schema: List of SchemaFields representing the schema of the table

        Raises:
            RuntimeError upon any insertion errors
        """
        error_list = []
        start = 0
        num_rows = len(data)

        # BigQuery has a limit of max 10k to insert per request
        while start < num_rows:
            end = start + MAX_ROWS_TO_INSERT
            # Convert the rows into dictionaries so we can insert with schema checking.
            error_list.extend(self.gclient
                              .insert_rows(table,
                                           self._convert_row_tuples_to_dicts(data[start:end],
                                                                             table_schema),
                                           retry=self.default_retry))
            start = end

        if error_list:
            raise RuntimeError('Could not insert rows. Reported errors:\n' +
                               self._make_errors_readable(error_list))

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
        # Use the Table object so we can pass through the schema.
        table = Table(self.get_table_reference_from_path(table_path), schema)
        if self.table_exists(table):
            if replace_existing_table:
                self.delete_table(table)
            else:
                raise RuntimeError('The table {} already exists.'.format(table_path))
        self.create_table(table)

        if data:
            if make_immediately_available:
                output = cStringIO.StringIO()

                csv_out = csv.writer(output)
                for row in data:
                    csv_out.writerow(row)

                job_config = LoadJobConfig()
                job_config.source_format = 'text/csv'
                # By default this does six retries. It does not accept any other timeout or
                # retry parameters.
                job = self.gclient.load_table_from_file(output, table.reference,
                                                        job_config=job_config,
                                                        rewind=True)
                job.result()

                output.close()
            else:
                self._stream_chunks_of_rows(table, data, schema)

    def append_rows(self, table_path, data, schema=None):
        # type: (str, List[Tuple[Any]], Optional[List[SchemaField]]) -> None
        """Appends the rows contained in data to the table at table_path using streaming inserts.
        Note that streaming inserts are immediately available for querying, but not for exporting or
        copying. https://cloud.google.com/bigquery/streaming-data-into-bigquery

        Args:
          table_path: A string of the form '<dataset id>.<table name>'.
          data: A list of rows, each of which is a list of values.
          schema: Optionally, a list of pairs (<column name>, <value type>) to describe the
              table's expected schema. If this is present, it will check the table's schema against
              the provided schema.

        Raises:
            RuntimeError: if the schema passed in as columns doesn't match the schema of the
                already-created table represented by table_path, or if the table doesn't exist,
                or if there are errors inserting the rows.
        """

        table = self.gclient.get_table(self.get_table_reference_from_path(table_path),
                                       retry=self.default_retry)

        if not self.table_exists(table):
            raise RuntimeError("The table " + table_path + " doesn't exist.")

        if schema is not None:
            schema_diffs = BigqueryBaseClient.list_schema_differences(table.schema, schema)
            if schema_diffs:
                raise RuntimeError("The incoming schema doesn't match "
                                   "the existing table's schema: {}"
                                   .format('\n'.join(schema_diffs)))

        self._stream_chunks_of_rows(table, data, table.schema)

    @staticmethod
    def _make_errors_readable(error_list):
        # type: (List[Dict[str, str]]) -> str
        """
        The returned value from table.insert_data is a list of dictionaries, where
        the dictionary values contain a list of dictionaries with the actual errors so it's
        complicated to get results in a readable format. This functions parses that output into a
        nice string.
        """
        row_errors = []
        if error_list:
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
        return '\n'.join(row_errors)

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
            destination_table_name: The name of the table to copy to.
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

        destination_dataset = destination_dataset or self.default_dataset_id
        destination_project = destination_project or self.project_id

        dataset_ref = DatasetReference(destination_project, destination_dataset)

        if not self.dataset_exists(dataset_ref):
            raise RuntimeError('The dataset {} does not exist in project {}.'
                               .format(destination_dataset, destination_project))

        dest_table_ref = TableReference(dataset_ref, destination_table_name)

        if self.table_exists(dest_table_ref):
            if replace_existing_table:
                self.delete_table(dest_table_ref)
            else:
                raise RuntimeError('The table {} already exists in dataset {}.'
                                   .format(destination_table_name, destination_dataset))

        dest_table_path = self.path(destination_table_name,
                                    destination_dataset,
                                    destination_project)

        self.create_table_from_query('SELECT * FROM `{}`'.format(source_table_path),
                                     dest_table_path)

    def export_table_to_bucket(self,
                               table_path,  # type: str
                               bucket_name,  # type: str
                               dir_in_bucket='',  # type: Optional[str]
                               output_format='csv',  # type: Optional[str]]
                               compression=False,  # type: Optional[bool]
                               output_ext='',  # type: Optional[str]
                               max_wait_secs=None,  # type: Optional[int]
                               support_multifile_export=True # type: bool
                               ):
        # type: (...) -> None
        """
        Export a BigQuery table to a file (or a set of files)
        in the given bucket. The output files will be in a directory with the same name
        as the table, within the bucket_name and dir_in_bucket provided.

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
        bq_output_format = self._convert_to_bq_format(output_format)

        if compression and output_format == 'avro':
            raise ValueError('{} cannot be combined with GZIP compression'.format(output_format))

        src_table_ref = self.get_table_reference_from_path(table_path)

        # Generate the destination of the table content.
        output_filename = src_table_ref.table_id
        if support_multifile_export:
            # End in a * so that multiple shards can be written out if needed.
            output_filename += '*'
        if output_ext:
            output_filename += '_' + output_ext
        output_filename += '.' + output_format
        if compression:
            output_filename += '.gz'
        path = os.path.join(dir_in_bucket, output_filename)

        destination = 'gs://{}/{}'.format(bucket_name, path.lstrip().lstrip('/'))

        config = ExtractJobConfig()
        config.destination_format = bq_output_format
        config.compression = 'GZIP' if compression else 'NONE'

        extract_job = self.gclient.extract_table(src_table_ref, destination, job_config=config,
                                                 retry=self.default_retry)

        # Wait for completion
        extract_job.result(timeout=max_wait_secs or self.max_wait_secs)

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

    @staticmethod
    def _convert_to_bq_format(format):
        """
        Converts one of the internally expressed formats (csv, avro, or json) into the
        corresponding BigQuery constant. See
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.sourceFormat
        """
        # A mapping table from supported formats to bigquery required formats.
        bigquery_required_formats = {'csv': 'CSV', 'json': 'NEWLINE_DELIMITED_JSON', 'avro': 'AVRO'}
        if format not in bigquery_required_formats:
            raise ValueError('Invalid input format: {}. Must be among {}'.format(
                format, bigquery_required_formats.keys()))
        return bigquery_required_formats[format]

    @staticmethod
    def _make_load_job_config(source_format,  # type: str
                              write_disposition,  # type: str
                              schema=None,  # type: Optional[List[SchemaField]]
                              skip_leading_row=False,  #type: bool
                              ):
        """
        Makes and returns a LoadJobConfig according to the passed-in parameters.
        Args:
            source_format: Should be a recognized BigQuery source format. See
                https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.sourceFormat
            write_disposition: Should be a recognized BigQuery write disposition. See
                https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.writeDisposition
            schema: A list of SchemaFields. If unset, BigQuery will try to infer a schema.
            skip_leading_row: If True, the first row of the file loaded in will be skipped.
        """
        job_config = LoadJobConfig()
        job_config.source_format = source_format
        job_config.write_disposition = write_disposition
        if schema:
            job_config.schema = schema
        else:
            job_config.autodetect = True
        if skip_leading_row:
            job_config.skip_leading_rows = 1
        return job_config

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
            table_path:  The path to the table that the data should be loaded in. If the table
                doesn't already exist, it will be created.
            source_uri: The URI for the file in the bucket to load.
            schema: The BigQuery schema for the data. If not provided, BigQuery will try to infer.
            input_format: The format of the input file. Can be 'csv', 'avro', or 'json'.
            write_disposition: The write disposition to use, see writeDisposition on this page:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
            skip_leading_row: If True, it will skip the first row of data in the
                file (presumably a header)
            max_wait_secs: The amount of time to wait for the table to import. This operation can
                be slower than other operations in this class. If unset, it will use the
                class default timeout.

        Raises:
            RuntimeError if there is a problem executing the load job.
        """
        input_format = self._convert_to_bq_format(input_format)

        table_ref = self.get_table_reference_from_path(table_path)
        job_config = self._make_load_job_config(input_format, write_disposition,
                                                schema, skip_leading_row)
        load_job = self.gclient.load_table_from_uri(source_uri, table_ref,
                                                    retry=self.default_retry,
                                                    job_config=job_config)
        load_job.result(max_wait_secs or self.max_wait_secs)

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
            table_path:  The path to the table that the data should be loaded in. If the table
                doesn't already exist, it will be created.
            opened_source_file: The source file, already opened in read-binary mode.
            schema: The BigQuery schema for the data. If unset, BigQuery will try to infer.
            input_format: The format of the input file. Can be 'csv', 'avro', or 'json'.
            write_disposition: The write disposition to use, see writeDisposition on this page:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
            skip_leading_row: If True, it will skip the first row of data in the
                file (presumably a header)
            max_wait_secs: The amount of time to wait for the table to import. This operation can
                be slower than other operations in this class. If unset, it will use the
                class default timeout.

        Raises:
            RuntimeError if there is a problem executing the load job.
        """
        input_format = self._convert_to_bq_format(input_format)

        # Load it into Bigquery
        table_ref = self.get_table_reference_from_path(table_path)
        job_config = self._make_load_job_config(input_format, write_disposition,
                                                schema, skip_leading_row)
        load_job = self.gclient.load_table_from_file(opened_source_file, table_ref,
                                                     job_config=job_config)
        load_job.result(max_wait_secs or self.max_wait_secs)

    def dataset_exists(self,
                       dataset  # type: Dataset, DatasetReference
                       ):
        # type: (...) -> bool
        """Checks if a dataset exists.

        Args:
            dataset: The BQ dataset object to check.
        """
        if isinstance(dataset, Dataset):
            dataset = dataset.reference

        try:
            self.gclient.get_dataset(dataset, retry=self.default_retry)
            return True
        except NotFound:
            return False

    def table_exists(self,
                     table  # type: TableReference, Table
                     ):
        # type: (...) -> bool
        """Checks if a table exists.

        Args:
            table: The TableReference or Table for the table to check whether it exists.
        """

        if isinstance(table, Table):
            table = table.reference

        try:
            self.gclient.get_table(table, retry=self.default_retry)
            return True
        except NotFound:
            return False

    def delete_dataset(self,
                       dataset  # type: Dataset, DatasetReference
                       ):
        # type: (...) -> None
        """Deletes a dataset.

        Args:
            dataset: The Dataset or DatasetReference to delete.
        """
        self.gclient.delete_dataset(dataset, retry=self.default_retry)

    def delete_table(self,
                     table  # type: Table, TableReference
                     ):
        # type: (...) -> None
        """Deletes a table.

        Args:
            table: The Table or TableReference to delete.
        """
        self.gclient.delete_table(table, retry=self.default_retry)

    def create_dataset(self,
                       dataset  # type: DatasetReference, Dataset
                       ):
        # type: (...) -> None
        """
        Creates a dataset.

        Args:
            dataset: The Dataset object to create.
        """
        if isinstance(dataset, DatasetReference):
            dataset = Dataset(dataset)

        self.gclient.create_dataset(dataset)

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
        if isinstance(table, TableReference):
            # Normally you'd pass in the schema here upon Table instantiation
            table = Table(table)

        self.gclient.create_table(table)

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
        return self.get_query_results('SELECT * FROM `{}`'.format(table.table_id))
