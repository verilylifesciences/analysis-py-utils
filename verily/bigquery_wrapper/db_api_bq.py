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
"""Library for interacting with databases via Python DB API.

Sample usage:

    connection = ...  # Create connection to your DB of choice.
    client = db_api_bq.Client(connection)
    result = client.Query(query)
"""

# Workaround for https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2366
from __future__ import absolute_import

import six

from verily.bigquery_wrapper.bq_base import (BQ_PATH_DELIMITER,
                                             BigqueryBaseClient)


# Pylint thinks this is an abstract class because it throws NotImplementedErrors.
# The warning is disabled because in this case those errors indicate that those methods
# are just difficult or impossible to mock.
# pylint: disable=R0921
class Client(BigqueryBaseClient):
    """A client exposing a BigQuery-like API, backed by another database instead.

    Args:
        connection: A DB API Connection object.
        project_id: The id of the project to associate with the client.
        default_dataset: The default dataset to use in function calls if none is explicitly
            provided.
        maximum_billing_tier: Unused in this implementation.
    """

    def __init__(self, connection,
                 project_id, default_dataset=None, maximum_billing_tier=None):
        super(Client, self).__init__(project_id, default_dataset, maximum_billing_tier)

        self.conn = connection
        self.cursor = self.conn.cursor()

    def _convert_to_other_db_path(self, table_path):
        """
        Converts a table path from being delimited with BQ_PATH_DELIMITER to other db's delimiter.
        """
        table_project, dataset_id, table_name = self.parse_table_path(table_path,
                                                                      delimiter=BQ_PATH_DELIMITER)
        return self.path(table_name, dataset_id, table_project)

    def _bq_type_to_db_type(self, typename):
        """Maps BQ types to other DB's types."""
        return typename

    def _db_type_to_bq_type(self, typename, sample=None):
        """Maps other DB's types to BQ types."""
        return typename

    def _get_column_names_in_result(self, query):
        """ Returns a list of the column names of the rows the query will return. """
        self.cursor.execute(self._reformat_query(query))
        return [x[0] for x in self.cursor.description]

    def _create_table(self, other_db_path, schema_fields=None, schema_string=None):
        """Creates a table.

        Adds its path to the appropriate mappings.
        The DB API requires a schema string for table creation. If one isn't passed in, this
        function will use a dummy schema to create the table.

        Args:
            other_db_path: The table path as expected by the other database (rather than BigQuery),
                since this is a private function.
            schema_fields: A list of SchemaFields descrbing the table. If both schema_fields
                and schema_string are present, schema_fields will take precedence.
            schema_string: A string describing the schema.
        Raises:
            RuntimeError: if it looks like the project or dataset haven't been created yet.
        """
        if schema_fields is not None:
            for field in schema_fields:
                if field.mode == 'REPEATED':
                    raise RuntimeError(
                        'Repeated fields are only supported by BigQuery; please use BQ instead')
            schema_list = [
                x.name + ' ' + self._bq_type_to_db_type(x.field_type) for x in schema_fields]
            create_query = 'CREATE TABLE ' + other_db_path + '(' + ','.join(schema_list) + ')'
        elif schema_string is not None:
            create_query = 'CREATE TABLE ' + other_db_path + ' ' + schema_string
        else:
            create_query = 'CREATE TABLE ' + other_db_path + '(dummy_field)'

        self.cursor.execute(create_query)
        self.conn.commit()

    def _insert_list_into_table(self, other_db_path, data_list):
        """Inserts a list of rows into a table.

        Args:
            other_db_path: The table path as expected by the other database (rather than BigQuery),
                since this is a private function.
            query_results: A list of tuples (or lists) representing the rows of data to be inserted.
        """
        # If there's no data to insert, just return.
        if len(data_list) == 0:
            return

        # Only insert rows with non-zero length.
        for row in [x for x in self._escape_row(data_list) if len(x)]:
            values_string = '(' + ','.join(row) + ')'
            query = 'INSERT INTO {} VALUES {}'.format(other_db_path, values_string)

            try:
                self.cursor.execute(query)
            except Exception as e:
                raise RuntimeError(e, ' '.join(query.split()))

    def _escape_row(self, data_list):
        """Escapes data items for use in a query.

        In order to use objects in a query, they all need to be converted to strings.  Some types
        require special additional handling:
        - anything that's string-like needs to be enclosed in single quotes
        - Nones are converted to NULLs
        - single quotes inside the field itself are converted to double quotes.

        Subclasses may override this as necessary to match the syntax of the SQL dialect in
        question.

        Args:
           data_list: A list of lists of arbitrary Python objects

        Returns:
           A list of lists of the same shape containing strings suitable for inclusion in a query.
        """
        new_data_list = []
        for row in data_list:
            row_list = []
            for col in row:
                if col is None:
                    col = 'NULL'
                elif type(col) not in [int, float]:
                    col = '\'' + str(col).replace('\'', '"') + '\''
                row_list.append(str(col))
            new_data_list.append(row_list)
        return new_data_list

    def _reformat_query(self, query):
        """Transforms a BigQuery query into the language of another db.

        Subclasses should override this function as appropriate to implement the translation,
        e.g. by changing the names of functions, types, etc.

        Args:
            query: The query to reformat

        Returns:
            The transformed query.
        """
        return query

    def get_query_results(self, query, use_legacy_sql=False, max_wait_secs=None):
        # type: (str, Optional[Bool], Optional[int]) -> List[Tuple[Any]]
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
            raise NotImplementedError("Legacy SQL is not supported.")

        rows = []
        for row in self.cursor.execute(self._reformat_query(query)):
            rows.append(tuple(self._reformat_results(row)))
        return rows

    def _reformat_results(self, result_row):
        """Applies any needed reformatting to results returned from the database.

        Subclasses should override as needed to present expected behavior.

        Args:
            result_row: A single row of results as returned from the other database.
        Returns:
            The reformatted row.
        """
        return result_row

    def create_table_from_query(self,
                                query,  # type: str
                                table_path,  # type: str
                                write_disposition='WRITE_EMPTY',  # type: Optional[str]
                                use_legacy_sql=False,  # type: Optional[bool]
                                max_wait_secs=None,  # type: Optional[int]
                                expected_schema=None  # type: Optional[List[SchemaField]]
                                ):
        # type: (...) -> None
        """Creates a table in the database from a specified query.

        The database requires a schema to create a table. We can't derive the schema from
        returned query results, so we make something up with the right number of columns
        if we get any results at all. If there are no query results we just add a single column.

        For use cases where the column names or correct schemas are important, use
        CreateTablesFromDict instead.

        Args:
            query: The query to run.
            table_path: The path to the table (in the client's project) to write
                the results to.
            write_disposition: Unused in this implementation
            use_legacy_sql: Should always be set to False; legacy SQL is not supported.
            max_wait_sec: Unused in this implementation
            expected_schema: The expected schema of the resulting table (only required for mocking).

        Raises:
            RuntimeError if use_legacy_sql is true.
        """

        other_db_path = self._convert_to_other_db_path(table_path)

        if use_legacy_sql:
            raise NotImplementedError("Legacy SQL is not supported.")

        if write_disposition not in ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']:
            raise ValueError('write_disposition must be one of WRITE_TRUNCATE, '
                             'WRITE_APPEND, or WRITE_EMPTY')

        query_results = self.get_query_results(query)

        # Create a schema based on the results returned. Even if the query has no results,
        # the database still knows what columns should be there.
        schema_string = None
        column_names = self._get_column_names_in_result(query)
        if expected_schema is not None:
            if len(expected_schema) != len(column_names):
                raise RuntimeError("The schema doesn't have the same length as the result columns.")
            for i in range(len(expected_schema)):
                if expected_schema[i].name != column_names[i]:
                    raise RuntimeError("The schema has a mismatching column name.")
            # No schema type check because types in other databases may be different and more
            # ambiguous than types in Bigquery.
        else:
            schema_string = '(' + ','.join(column_names) + ')'

        table_exists = self.parse_table_path(other_db_path)[2] in self.tables(
            self.default_dataset_id)
        if table_exists:
            if write_disposition == 'WRITE_EMPTY':
                raise RuntimeError(
                    'Table {} already exists and write_disposition is WRITE_EMPTY.'.format(
                        other_db_path))
            elif write_disposition == 'WRITE_TRUNCATE':
                self.delete_table_by_name(table_path)
                self._create_table(other_db_path, schema_fields=expected_schema,
                                   schema_string=schema_string)
        else:
            self._create_table(other_db_path, schema_fields=expected_schema,
                               schema_string=schema_string)

        if query_results:
            results = [list(itm) for itm in query_results]
            if expected_schema is not None:
                internal_rep = self._cast_to_correct_internal_representation(
                    results, expected_schema)
                self._insert_list_into_table(other_db_path, internal_rep)
            else:
                self._insert_list_into_table(other_db_path, results)

    def _cast_to_correct_internal_representation(self, query_results, expected_schema):
        """Turns types from what is received from the other db into the correct internal type.

        e.g. another database might represent booleans as integers, so we might
        want to turn 0 to False and 1 to True.

        Args:
            query_results: List of list of results
            expected_schema: The schema, the length should match the length of each row
        """
        return query_results

    def delete_table_by_name(self, table_path):
        """Delete a table within the current project.

        Args:
            table_path: A string of the form '<dataset id>.<table name>' or
                '<project id>.<dataset_id>.<table_name>'
        """
        other_db_path = self._convert_to_other_db_path(table_path)
        self.cursor.execute('DROP TABLE ' + other_db_path)
        self.conn.commit()

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
        _, dataset, table_name = self.parse_table_path(table_path,
                                                       delimiter=BQ_PATH_DELIMITER)

        tables_in_dataset = self.tables(dataset)

        schema_field_list = [x.name + ' ' + self._bq_type_to_db_type(x.field_type)
                             for x in schema]
        schema = '(' + ', '.join(schema_field_list) + ')'

        if table_name in tables_in_dataset:
            if replace_existing_table:
                self.delete_table_by_name(table_path)
            else:
                raise RuntimeError('The table {} already exists.'.format(table_path))

        table_path = self._convert_to_other_db_path(table_path)

        self._create_table(table_path, schema_string=schema)
        self._insert_list_into_table(table_path, data)

    def append_rows(self, table_path, data, schema=None):
        # type: (str, List[Tuple[Any]], Optional[List[SchemaField]]) -> None
        """Appends the rows contained in data to the table at table_path. This function assumes
        the table itself is already created.

        Args:
            table_path: A string of the form '<dataset id>.<table name>'.
            columns: A list of pairs (<column name>, <value type>).
            data: A list of rows, each of which is a list of values.

        Raises:
            RuntimeError: if the schema passed in as columns doesn't match the schema of the
                already-created table represented by table_path
        """
        table_project, dataset_id, table_name = self.parse_table_path(table_path,
                                                                      delimiter=BQ_PATH_DELIMITER)
        table_path = self._convert_to_other_db_path(table_path)

        if table_name not in self.tables(dataset_id):
            raise RuntimeError("The table " + table_name + " doesn't exist.")
        if schema is not None:
            current_schema = self.get_schema(dataset_id, table_name, project_id=table_project)
            schema_diffs = BigqueryBaseClient.list_schema_differences(current_schema, schema)
            if schema_diffs:
                raise RuntimeError("The incoming schema doesn't match "
                                   "the existing table's schema: {}"
                                   .format('\n'.join(schema_diffs)))
        self._insert_list_into_table(table_path, data)

    def dataset_exists(self,
                       dataset  # type: Dataset, DatasetReference
                       ):
        # type: (...) -> bool
        """Checks if a dataset exists.

        Args:
            dataset: The BQ dataset object to check.
        """
        raise NotImplementedError('dataset_exists is not implemented in db_api_bq since'
                                  'db_api_bq does not use BigQuery objects.')

    def table_exists(self,
                     table  # type: TableReference, Table
                     ):
        # type: (...) -> bool
        """Checks if a table exists.

        Args:
            table: The TableReference or Table for the table to check whether it exists.
        """
        raise NotImplementedError('table_exists is not implemented in db_api_bq since'
                                  'db_api_bq does not use BigQuery objects.')

    def delete_dataset(self, dataset):
        # type: (...) -> None
        """Deletes a dataset.

        Args:
            dataset: The Dataset or DatasetReference to delete.
        """
        raise NotImplementedError('delete_dataset is not implemented in db_api_bq since'
                                  'db_api_bq does not use BigQuery objects.')

    def delete_table(self,
                     table  # type: Table, TableReference
                     ):
        # type: (...) -> None
        """Deletes a table.

        Args:
            table: The Table or TableReference to delete.
        """
        raise NotImplementedError('delete_table is not implemented in db_api_bq since'
                                  'db_api_bq does not use BigQuery objects.')

    def create_dataset(self,
                       dataset  # type: DatasetReference, Dataset
                       ):
        # type: (...) -> None
        """
        Creates a dataset.

        Args:
            dataset: The Dataset object to create.
        """
        raise NotImplementedError('create_dataset is not implemented in db_api_bq since'
                                  'db_api_bq does not use BigQuery objects.')

    def create_table(self, table):
        # type: (Table) -> None
        """
        Creates a table.

        Args:
            table: The Table or TableReference object to create. Note that if you pass a
                TableReference the table will be created with no schema.
        """
        raise NotImplementedError('create_table is not implemented in db_api_bq since'
                                  'db_api_bq does not use BigQuery objects.')

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
        raise NotImplementedError('fetch_data_from_table is not implemented in db_api_bq since'
                                  'db_api_bq does not use BigQuery objects.')
