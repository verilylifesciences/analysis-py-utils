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
"""Mock library for interacting with BigQuery, backed by Spark SQL.

Sample usage:

    client = mock_bq.Client(project_id)
    result = client.Query(query)
"""

# Workaround for https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2366
from __future__ import absolute_import

import logging
import re

from google.cloud.bigquery.schema import SchemaField
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import ParseException
from typing import Any, List, Optional, Tuple  # noqa: F401

from verily.bigquery_wrapper.bq_base import BQ_PATH_DELIMITER, BigqueryBaseClient
from verily.bigquery_wrapper.mock_bq_base import MockBaseClient

# Map from BQ data type to Spark SQL data type.
BQ_TO_SPARK_SQL_DTYPE = {
    'STRING': StringType(),
    'INT64': IntegerType(),
    'INTEGER': IntegerType(),
    'FLOAT64': FloatType(),
    'FLOAT': FloatType(),
    'BOOLEAN': BooleanType(),
    'TIMESTAMP': TimestampType(),
    'DATETIME': TimestampType(),
    'DATE': DateType()
}

# Map from Spark SQL data type to BQ data type.
SPARK_SQL_TO_BQ_DTYPE = {
    StringType(): 'STRING',
    IntegerType(): 'INTEGER',
    FloatType(): 'FLOAT',
    BooleanType(): 'BOOLEAN',
    TimestampType(): 'TIMESTAMP',
    DateType(): 'DATE'
}


# Pylint thinks this is an abstract class because it throws NotImplementedErrors.
# The warning is disabled because in this case those errors indicate that those methods
# are just difficult or impossible to mock.
# pylint: disable=R0921
class Client(MockBaseClient):
    """Stores pointers to a mock BigQuery project, backed by an in-memory Spark SQL database.

    Spark SQL syntax is HiveQL (https://cwiki.apache.org/confluence/display/Hive/Home), which is
    fairly close to BigQuery Standard SQL.

    Args:
        project_id: The id of the project to associate with the client.
        default_dataset: The default dataset to use in function calls if none is explicitly
            provided.
        maximum_billing_tier: Unused in this implementation.
        print_before_and_after: If set to True, this will print out queries as passed in, then
            print out the query after it's been reformatted to match appropriate SQLite conventions.
            Helpful for debugging.
    """

    def __init__(self, project_id, default_dataset=None, maximum_billing_tier=None,
                 print_before_and_after=False):
        super(Client, self).__init__(project_id, default_dataset, maximum_billing_tier,
                                     print_before_and_after)
        self.spark = SparkSession.builder.getOrCreate()

    def get_delimiter(self):
        return BQ_PATH_DELIMITER

    @staticmethod
    def _format_path(table_path):
        """Returns the given table path enclosed by backticks.

        This is necessary for pyspark functions that take a table name, because table paths with
        periods or hyphens must be enclosed by backticks in these functions.
        """
        return '`{}`'.format(table_path)

    def _get_schema_of_result(self, query):
        """Returns the schema of the result of the given query."""
        return self.spark.sql(self._reformat_query(query)).schema

    def _get_column_names_in_result(self, query):
        """Returns a list of the column names of the rows the query will return. """
        return [f.name for f in self._get_schema_of_result(query)]

    @staticmethod
    def _schema_fields_to_spark_data_types(schema_fields):
        # type: (List[SchemaField]) -> StructType(StructField(str, DataType, bool))
        """Converts a list of bigquery SchemaField to a spark sql DataType struct."""
        return StructType([
            StructField(f.name, BQ_TO_SPARK_SQL_DTYPE[f.field_type], f.mode == 'NULLABLE') for f in
            schema_fields
        ])

    def _create_table(self, table_path, data=None, bq_schema=None, spark_schema=None):
        """Creates a table in Spark SQL. Replaces it if it already exists.

        Exactly one of bq_schema and spark_schema must be specified.

        Args:
            table_path: The path to the table to create.
            data: List of tuples to insert upon creation of the table.
            bq_schema: A list of SchemaFields describing the table.
            spark_schema: A StructType containing a list of StructField describing the table.

        Raises:
            RuntimeError: if bq_schema and spark_schema are both specified or neither are specified.
        """
        if bool(bq_schema) == bool(spark_schema):
            raise RuntimeError('Exactly one of bq_schema and spark_schema must be specified.')

        self._add_table(table_path)

        formatted_table_path = self._format_path(table_path)

        schema = spark_schema or self._schema_fields_to_spark_data_types(bq_schema)
        df = self.spark.createDataFrame(data or [], schema)
        df.createOrReplaceTempView(formatted_table_path)

    def _reformat_query(self, query):
        """Does a variety of transformations to reinterpret a BigQuery query into a Spark SQL query.

        These transformations are just the ones that have come up in testing so far and were
        easy to encode, so there's plenty of room for growth here. Anything very difficult to
        find and replace should probably just be tested in the normal BQ environment though.

        Args:
            query: The query to reformat

        Returns:
            The reformatted query.

        Raises:
            RuntimeError: If the transformed query wouldn't execute in Spark SQL. If this happens,
                the test author can either roll in whatever the missing transformation is into this
                function, or choose to test on real BigQuery.
        """
        if self.print_before_and_after:
            logging.info("ORIGINAL: " + query)

        query = self._remove_query_prefix(query)
        query = self._transform_farmfingerprint(query)
        query = self._transform_extract(query)
        query = self._transform_mod(query)
        query = self._transform_format(query)

        if self.print_before_and_after:
            logging.info("REFORMATTED: " + query)

        # If we don't end up with valid Spark SQL, throw the exception here and print the query.
        try:
            self.spark.sql(query)
        except ParseException as e:
            raise RuntimeError('We tried to reformat your query into Spark SQL, ' +
                               'but it still won\'t work. Check to make sure it was a valid ' +
                               'query to begin with, then consider adding the transformation ' +
                               'needed to make it work in the _reformat_query method.' +
                               '\nSpark SQL error: ' + str(e) + '\nThe result query: ' +
                               ' '.join(query.split()))
        return query

    @staticmethod
    def _transform_extract(query):
        """Transform EXTRACT(X FROM Y) to just X(Y)."""
        return re.sub(r'EXTRACT\((.+) FROM (.+)\)', r'\1(\2)', query)

    @staticmethod
    def _transform_mod(query):
        """Transform MOD(arg1,arg2) to PMOD(arg1,arg2)."""
        return re.sub('MOD\((.*),(.*)\)', r'PMOD(\1,\2)', query)

    def get_query_results(self, query, use_legacy_sql=False, max_wait_secs=None):
        # type: (str, Optional[bool], Optional[int]) -> List[Tuple[Any]]
        """Returns a list of rows, each of which is a tuple of values.

        Args:
            query: A string with a complete SQL query.
            max_results: Maximum number of results to return.
            use_legacy_sql: If set, will raise a RuntimeError.
            max_wait_secs: Unused in this implementation
        Returns:
            A list of tuples of values.
        Raises:
            RuntimeError if use_legacy_sql is true.
        """
        if use_legacy_sql:
            raise RuntimeError("Legacy SQL is disallowed for this mock.")

        return [tuple(row) for row in self.spark.sql(self._reformat_query(query)).collect()]

    def create_table_from_query(self,
                                query,  # type: str
                                table_path,  # type: str
                                write_disposition='WRITE_TRUNCATE',  # type: Optional[str]
                                use_legacy_sql=False,  # type: Optional[bool]
                                max_wait_secs=None,  # type: Optional[int]
                                expected_schema=None  # type: Optional[List[SchemaField]]
                                ):
        # type: (...) -> None
        """Creates a table in Spark SQL from a specified query.

        If the table already exists, replace it.

        Args:
            query: The query to run.
            table_path: The path to the table (in the client's project) to write
                the results to.
            write_disposition: Specifies behavior if table already exists. See options here:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs under
                configuration.query.writeDisposition
            use_legacy_sql: Should always be set to False; legacy SQL is disallowed in this mock.
            max_wait_secs: Unused in this implementation
            expected_schema: The expected schema of the resulting table (only required for mocking).
        Raises:
            RuntimeError if use_legacy_sql is true.
        """
        if use_legacy_sql:
            raise RuntimeError('Legacy SQL is disallowed for this mock.')

        if write_disposition not in ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']:
            raise ValueError('write_disposition must be one of WRITE_TRUNCATE, '
                             'WRITE_APPEND, or WRITE_EMPTY')

        query_results = self.get_query_results(query)

        # Create a schema based on the results returned. Even if the query has no results,
        # Spark SQL still knows what columns should be there.
        bq_schema, spark_schema = None, None
        if expected_schema is not None:
            column_names = self._get_column_names_in_result(query)
            if len(expected_schema) != len(column_names):
                raise RuntimeError("The schema doesn't have the same length as the result columns.")
            for i in range(len(expected_schema)):
                if expected_schema[i].name != column_names[i]:
                    raise RuntimeError("The schema has a mismatching column name.")
            # No schema type check because types in Spark SQL do not match up 1:1 with types in
            # Bigquery.
            bq_schema = expected_schema
        else:
            spark_schema = self._get_schema_of_result(query)

        table_exists = self.parse_table_path(table_path)[2] in self.tables(self.default_dataset_id)

        if table_exists and write_disposition != 'WRITE_TRUNCATE':
            if write_disposition == 'WRITE_EMPTY':
                raise RuntimeError(
                    'Table {} already exists and write_disposition is WRITE_EMPTY.'.format(
                        table_path))
            elif write_disposition == 'WRITE_APPEND':
                self.append_rows(table_path, query_results)
        else:
            self._create_table(table_path, data=query_results, bq_schema=bq_schema,
                               spark_schema=spark_schema)

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
        # If the flag isn't set to replace existing tables, raise an error if any tables we're
        # trying to create already exist.
        if not replace_existing_tables:
            self._raise_if_tables_exist(table_names_to_schemas.keys())

        for table_name, schema in table_names_to_schemas.iteritems():
            table_path = self.path(table_name, dataset_id=dataset_id)

            if (table_name in self.tables(dataset_id or self.default_dataset_id)
                    and replace_existing_tables):
                self.delete_table_by_name(self.path(table_name))
            self._create_table(table_path, bq_schema=schema)

    def delete_table_by_name(self, table_path):
        """Delete a table within the current project.

        Args:
            table_path: A string of the form '<dataset id>.<table name>' or
                '<project id>.<dataset_id>.<table_name>'
        """
        self.remove_table(table_path)
        formatted_table_path = self._format_path(table_path)
        self.spark.catalog.dropTempView(formatted_table_path)

    def get_schema(self, dataset_id, table_name, project_id=None):
        # type: (str, str, Optional[str]) -> List[SchemaField]
        """Returns the schema of a table. Note that due to the imperfect mapping
        of Spark SQL types to BQ types, these schemas won't be perfect. Anything relying heavily
        on correct schemas should use the real BigQuery client.

        Args:
            dataset_id: The dataset to query.
            table_name: The name of the table.
            project_id: The project ID of the table.
        Returns:
            A list of SchemaFields representing the schema.
        """
        table_path = self.path(table_name, dataset_id=dataset_id)
        formatted_table_path = self._format_path(table_path)
        spark_schema = self.spark.table(formatted_table_path).schema
        return [SchemaField(f.name, SPARK_SQL_TO_BQ_DTYPE[f.dataType]) for f in spark_schema]

    def populate_table(self, table_path, schema, data=None, make_immediately_available=False,
                       replace_existing_table=False):
        # type: (str, List[SchemaField], Optional[List[Any]], Optional[bool], Optional[bool]) -> None
        """Creates a table and populates it with a list of rows.

        If the table is already created, it will raise a RuntimeError, unless replace_existing_table
        is True.

        Args:
            table_path: A string of the form '<dataset id>.<table name>'
                or '<project id>.<dataset id>.<table name>'.
            schema: A list of SchemaFields to represent the table's schema.
            data: A list of rows, each of which corresponds to a row to insert into the table.
                String representations of date and numeric types are allowed.
            make_immediately_available: Unused in this implementation.
            replace_existing_table: If set to True, the table at table_path will be deleted and
                recreated if it's already present.

        Raises:
            RuntimeError if the table at table_path is already there and replace_existing_table
                is False
        """
        _, dataset, table_name = self.parse_table_path(table_path)
        formatted_table_path = self._format_path(table_path)

        if table_name in self.tables(dataset):
            if replace_existing_table:
                self.delete_table_by_name(table_path)
            else:
                raise RuntimeError('The table {} already exists.'.format(table_path))

        # Convert the data to the right types. Write it to a df with all types as string, then cast
        # each column to the appropriate type.
        string_schema = StructType([
            StructField(f.name, StringType(), f.mode == 'NULLABLE') for f in schema
        ])
        untyped_df = self.spark.createDataFrame(data, schema=string_schema)
        spark_schema = self._schema_fields_to_spark_data_types(schema)
        casted_df = untyped_df.select(
            [untyped_df[f.name].cast(f.dataType) for f in spark_schema])

        casted_df.createOrReplaceTempView(formatted_table_path)
        self._add_table(table_path)

    def append_rows(self, table_path, data, schema=None):
        # type: (str, List[Tuple[Any]], Optional[List[SchemaField]]) -> None
        """Appends the rows contained in data to the table at table_path. This function assumes
        the table itself is already created.

        Args:
            table_path: A string of the form '<dataset id>.<table name>'.
            data: A list of rows, each of which is a list of values.
            schema: List of SchemaField for validation that the rows match the schema.

        Raises:
            RuntimeError: if the schema passed in as columns doesn't match the schema of the
                already-created table represented by table_path
        """
        table_project, dataset_id, table_name = self.parse_table_path(table_path)
        formatted_table_path = self._format_path(table_path)

        if table_name not in self.tables(dataset_id):
            raise RuntimeError("The table " + table_name + " doesn't exist.")
        if schema is not None:
            current_schema = self.get_schema(dataset_id, table_name, project_id=table_project)
            schema_diffs = BigqueryBaseClient.list_schema_differences(current_schema, schema)
            if schema_diffs:
                raise RuntimeError("The incoming schema doesn't match "
                                   "the existing table's schema: {}"
                                   .format('\n'.join(schema_diffs)))

        df = self.spark.table(formatted_table_path)
        df = df.union(self.spark.createDataFrame(data))
        df.createOrReplaceTempView(formatted_table_path)

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
        destination_project = destination_project or self.project_id
        destination_dataset = destination_dataset or self.default_dataset_id

        if destination_project not in self.project_map.keys():
            raise RuntimeError('Project {} does not exist.'.format(destination_project))

        if destination_dataset not in self.project_map[destination_project]:
            raise RuntimeError('Dataset {} does not exist in project {}.'
                               .format(destination_dataset, destination_project))

        if destination_table_name in self.table_map[destination_dataset]:
            if replace_existing_table:
                self.delete_table_by_name(self.path(destination_table_name,
                                                    destination_dataset,
                                                    destination_project))
            else:
                raise RuntimeError('The table {} already exists in dataset {}.'
                                   .format(destination_table_name, destination_dataset))

        self.create_table_from_query('SELECT * FROM `{}`'.format(
            source_table_path),
            self.path(destination_table_name, destination_dataset, destination_project))
