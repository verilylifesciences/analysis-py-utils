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
"""Mock library for interacting with BigQuery, backed by SQLite.

Sample usage:

    client = mock_bq.Client(project_id)
    result = client.Query(query)
"""
import datetime
import logging
import re

from pysqlite2 import dbapi2 as sqlite3
from google.cloud.bigquery.schema import SchemaField

from verily.bigquery_wrapper.bq_base import TABLE_PATH_DELIMITER, BigqueryBaseClient

# SQLite only uses . to separate database and table, so we need a
# different delimiter. It's pretty strict about what special characters it
# will accept as a part of a table name so I'm using Z since it's an uncommon
# character. The change should be invisible to users except in debugging test
# cases and tables with the delimiters replaced are never backparsed into Bigquery
# tables so there's no danger in using a table path with a Z in it.
REPLACEMENT_DELIMITER = 'Z'

TRUE_STR = 'True'
FALSE_STR = 'False'


# Pylint thinks this is an abstract class because it throws NotImplementedErrors.
# The warning is disabled because in this case those errors indicate that those methods
# are just difficult or impossible to mock.
# pylint: disable=R0921
class Client(BigqueryBaseClient):
    """Stores pointers to a mock BigQuery project, backed by an in-memory
    sqlite database instead.

    Args:
      project_id: The id of the project to associate with the client.
    """

    def __init__(self, project_id, default_dataset=None, maximum_billing_tier=None):
        # SQLite does not allow dashes in table paths.
        formatted_project_id = project_id.replace('-', '_')
        super(Client, self).__init__(formatted_project_id, default_dataset, maximum_billing_tier)

        # Connect to an in-memory database path.
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()

        # We use dictionaries to simulate the project/dataset/table structure present in BQ.
        # Mostly these will serve as checks in tests to make sure that a project and dataset
        # are created before a table is inserted into them.
        self.project_map = {}
        self.project_map[self.project_id] = []

        self.table_map = {}
        self.table_map[default_dataset] = []

    @staticmethod
    def _bq_type_to_sqlite_type(typename):
        """ Maps BQ types to SQLite types. One limitation of this class is that there's not a 1:1
        mapping, but this will cover most use cases."""
        if typename in ['FLOAT', 'FLOAT64']:
            return 'REAL'
        elif typename in ['INTEGER', 'INT64']:
            return 'INTEGER'

        # Strings and timestamps will be treated as text.
        return 'TEXT'

    @staticmethod
    def _sqlite_type_to_bq_type(typename, sample=None):
        """ Maps SQLite types to BQ types. One limitation of this class is that there's not a 1:1
        mapping, but this will cover most use cases. Passing in a sample gives it more information
        to make a better guess. """
        if typename == 'REAL':
            return 'FLOAT'
        elif typename == 'TEXT':
            # TEXT could be used to represent anything besides float or integer.
            if not sample:
                return 'STRING'
            if sample.lower() == 'true' or sample.lower() == 'false':
                return 'BOOLEAN'
            try:
                datetime.datetime.strptime(sample, '%Y-%m-%d %H:%M:%S UTC')
                return 'TIMESTAMP'
            except ValueError:
                pass
            return 'STRING'
        # Likely the only case here that will happen in practice is for INTEGER to be returned.
        return typename

    def _get_column_names_in_result(self, query):
        """ Returns a list of the column names of the rows the query will return. """
        self.cursor.execute(self._reformat_query(query))
        return [x[0] for x in self.cursor.description]

    def _create_table(self, table_path, schema_fields=None, schema_string=None):
        """ Creates a table in SQLite. Adds its path to the appropriate mappings.
        SQLite3 requires a schema string for table creation. If one isn't passed in, this
        function will use a dummy schema to create the table.

        Args:
            schema_fields: A list of SchemaFields descrbing the table. If both schema_fields
                and schema_string are present, schema_fields will take precedence.
            schema_string: A SQLite3 string describing the schema.
        Raises:
            RuntimeError: if it looks like the project or dataset haven't been created yet.
        """
        # Get the components of the table path and then parse them back into a
        # SQLite3 friendly path.
        project, dataset, table_name = self.parse_table_path(table_path)
        standardized_path = self.path(table_name, dataset, project, delimiter=REPLACEMENT_DELIMITER)

        # Make sure the project and dataset has already been explicitly created before
        # trying to make the table.
        try:
            self.table_map[dataset].append(table_name)
        except KeyError:
            raise RuntimeError(project + " not in known datasets." +
                               "Are you using the right dataset?")
        try:
            self.project_map[project].append(dataset)
        except KeyError:
            raise RuntimeError(project + " not in known projects." +
                               "Are you using the right project?")

        if schema_fields is not None:
            for field in schema_fields:
                if field.mode == 'REPEATED':
                    raise RuntimeError('Mock BigQuery does not support repeated fields. Please use '
                                       'real BigQuery.')
            schema_list = [
                x.name + ' ' + self._bq_type_to_sqlite_type(x.field_type) for x in schema_fields
                ]
            create_query = 'CREATE TABLE ' + standardized_path + '(' + ','.join(schema_list) + ')'
        elif schema_string is not None:
            create_query = 'CREATE TABLE ' + standardized_path + ' ' + schema_string
        else:
            create_query = 'CREATE TABLE ' + standardized_path + '(dummy_field)'

        self.cursor.execute(create_query)
        self.conn.commit()

    def _insert_list_into_table(self, table_path, data_list):
        """ Inserts a list of rows into a SQLite3 table.

        Args:
            table_path: The path to the table to insert the data.
            query_results: A list of tuples (or lists) representing the rows of data to be inserted.
        """
        # If there's no data to insert, just return.
        if len(data_list) == 0:
            return

        # Get the components of the table path and then parse them back into a
        # SQLIte3 friendly path.
        project, dataset, table_name = self.parse_table_path(table_path)
        standardized_path = self.path(table_name, dataset, project, delimiter=REPLACEMENT_DELIMITER)

        # Only insert rows with non-zero length.
        for row in [x for x in self._escape_row(data_list) if len(x)]:
            values_string = '(' + ','.join(row) + ')'
            query = 'INSERT INTO {} VALUES {}'.format(standardized_path, values_string)

            try:
                self.cursor.execute(query)
            except Exception as e:
                raise RuntimeError(e, ' '.join(query.split()))

    @staticmethod
    def _escape_row(data_list):
        """ For returned results in the query, anything that's string-like needs to be enclosed
        in single quotes before it makes it into the insert query, and everything needs to be
        a string so the list join will work right. Nones are converted to NULLs.
        """
        new_data_list = []
        for row in data_list:
            row_list = []
            for col in row:
                if col is None:
                    col = 'NULL'
                elif type(col) not in [int, float]:
                    col = '\'' + str(col) + '\''
                row_list.append(str(col))
            new_data_list.append(row_list)
        return new_data_list

    def _reformat_query(self, query, print_before_and_after=True):
        """Does a variety of transformations to reinterpret a BigQuery into a SQLite executable
        query.

        These transformations are just the ones that have come up in testing so far and were
        easy to encode, so there's plenty of room for growth here. Anything very difficult to
        find and replace should probably just be tested in the normal BQ environment though.

        So far this class:
        - replaces BQ formatted table paths with SQLite friendly ones
        - replaces the EXTRACT(YEAR operator
        - ensures that decimal division always happens
        - replaces the CONCAT operator with SQLite3's || operator for concatenation
        - replaces FORMAT with the SQLite3 printf equivalent

        Args:
            query: The query to reformat
            print_before_and_after: A boolean to indicate whether to print the query before and
                after transformation. This is mostly for test debugging.
        Raises:
            RuntimeError: If the transformed query wouldn't execute in SQLite. If this happens, the
                test author can either roll in whatever the missing transformation is into this
                function, or choose to test on real BigQuery.
        """
        if print_before_and_after:
            logging.info("ORIGINAL: " + query)

        # Remove backticks.
        query = query.replace('`', '')

        # For all known tables, replace the BigQuery formatted path with the SQLite3 formatted path
        for project, dataset_list in self.project_map.iteritems():
            for dataset in dataset_list:
                for table in self.table_map[dataset]:
                    qualified_table = self.path(
                            table, dataset, project,
                            delimiter=TABLE_PATH_DELIMITER, replace_dashes=False)
                    sanitized_table = self.path(
                            table, dataset, project, delimiter=REPLACEMENT_DELIMITER)
                    query = query.replace(qualified_table, sanitized_table)

        query = self._remove_query_prefix(query)
        query = self._transform_farmfingerprint(query)
        query = self._transform_extract_year(query)
        query = self._transform_extract_month(query)
        query = self._transform_concat(query)
        query = self._transform_division(query)
        query = self._transform_format(query)
        query = self._transform_substr(query)
        query = self._transform_mod(query)

        if print_before_and_after:
            logging.info("REFORMATTED: " + query)

        # If we don't end up with valid sqlite3, throw the exception here and print the query.
        try:
            self.conn.execute(query)
        except Exception as e:
            raise RuntimeError('We tried to reformat your query into SQLite, ' +
                               'but it still won\'t work. Check to make sure it was a valid ' +
                               'query to begin with, then consider adding the transformation ' +
                               'needed to make it work in the _reformat_query method.' +
                               '\nSQLite error: ' + str(e) + '\nThe result query: ' +
                               ' '.join(query.split()))
        return query

    @staticmethod
    def _remove_query_prefix(query):
        """Remove the BigQuery SQL variant prefix.
        https://cloud.google.com/bigquery/docs/reference/standard-sql/enabling-standard-sql#sql-prefix
        """
        query = query.replace('#standardSQL', '')
        query = query.replace('#legacySQL', '')
        return query

    @staticmethod
    def _transform_extract_year(query):
        """Transform EXTRACT(YEAR to a substring operator to get the year
        from a YYYY-MM-DD timestamp or date.
        """
        extract_regex = re.compile(r'EXTRACT\(YEAR FROM (?P<colname>.+?)\)')
        match = re.search(extract_regex, query)
        while match:
            repl_string = 'CAST(substr(' + match.group('colname') + ', 0, 5) as INTEGER)'
            query = query[:match.start()] + repl_string + query[match.end():]
            match = re.search(extract_regex, query)
        return query

    @staticmethod
    def _transform_extract_month(query):
        """Transform EXTRACT(MONTH to a substring operator to get the month
        from a YYYY-MM-DD timestamp or date.
        """
        extract_regex = re.compile(r'EXTRACT\(MONTH FROM (?P<colname>.+?)\)')
        match = re.search(extract_regex, query)
        while match:
            repl_string = 'CAST(substr(' + match.group('colname') + ', 6, 8) as INTEGER)'
            query = query[:match.start()] + repl_string + query[match.end():]
            match = re.search(extract_regex, query)
        return query

    @staticmethod
    def _transform_concat(query):
        """Transform a CONCAT operator into a chain of strings separated by ||."""
        concat_regex = re.compile(r'CONCAT\((?P<concat_list>.*)\)')
        match = re.search(concat_regex, query)
        while match:
            items = match.group('concat_list').split(',')
            repl_string = ' || '.join(items)
            query = query[:match.start()] + repl_string + query[match.end():]
            match = re.search(concat_regex, query)
        return query

    @staticmethod
    def _transform_division(query):
        """Typecast the numerator of every division operation to force decimal division"""
        div_regex = re.compile(r'(\((?P<num>\w+?)\s*\/\s*(?P<den>\w+?)\))')
        for match in re.findall(div_regex, query):
            repl_string = '(CAST(' + match[1] + ' AS FLOAT) /' + match[2] + ')'
            query = query.replace(match[0], repl_string)
        return query

    @staticmethod
    def _transform_format(query):
        """Replace the BQ function FORMAT with the equivalent SQLite function printf"""
        return query.replace('FORMAT(', 'printf(')

    @staticmethod
    def _transform_substr(query):
        """In the variant of sqlite we use, SUBSTR(string,0,...) yields one too few characters.
        This is a workaround for that."""
        concat_regex = re.compile(r'SUBSTR\((?P<string>.*),(?P<start>.*),(?P<stop>.*)\)')
        for match in re.findall(concat_regex, query):
            string_to_match = match[0]
            start = match[1]
            stop = match[2]
            if start == '0':
                repl_string = 'SUBSTR(' + string_to_match + ',1,' + str(stop) + ')'
                query = query.replace('SUBSTR(' + string_to_match + ',' + start + ',' + stop + ')',
                                      repl_string)
        return query

    @staticmethod
    def _transform_farmfingerprint(query):
        """For testing, we don't need to subsample, so we replace the fingerprint with 0"""
        # Note: This regex also includes concat and cast because without it, it includes too many
        # ')' see PM-??? for details.
        extract_regex = re.compile(
                r'FARM_FINGERPRINT\(CONCAT\(CAST\((?P<arg1>.*).*CAST\((?P<arg2>.*)\)\)\)')
        match = re.search(extract_regex, query)
        while match:
            repl_string = '0'
            query = query[:match.start()] + repl_string + query[match.end():]
            match = re.search(extract_regex, query)
        return query

    @staticmethod
    def _transform_mod(query):
        """Transform MOD(arg1,arg2) to arg1 % arg2."""
        extract_regex = re.compile(
                r'MOD\((?P<arg1>.*),(?P<arg2>.*)\)')
        match = re.search(extract_regex, query)
        while match:
            repl_string = match.group('arg1') + ' % ' + match.group('arg2')
            query = query[:match.start()] + repl_string + query[match.end():]
            match = re.search(extract_regex, query)
        return query

    def get_query_results(self, query, max_results=None, use_legacy_sql=False):
        """Returns a list of rows, each of which is a list of values.

        Args:
          query: A string with a complete SQL query.
          max_results: Maximum number of results to return.
          use_legacy_sql: Unused but left here for uniformity with bq.py. Should never
              be true.
        Returns:
          A list of tuples, with each tuple representing a result row.
        Raises:
          RuntimeError if use_legacy_sql is true.
        """

        if use_legacy_sql:
            raise RuntimeError("Legacy SQL is disallowed for this mock.")

        rows = []
        for row in self.cursor.execute(self._reformat_query(query)):
            rows.append(tuple(Client._reformat_results(row)))

        if max_results:
            return rows[:max_results]
        return rows

    @staticmethod
    def _reformat_results(result_row):
        reformatted_row = []
        for item in result_row:
            if item == 'None':
                reformatted_row.append(None)
            else:
                reformatted_row.append(item)
        return reformatted_row

    def create_table_from_query(
            self,
            query,
            table_path,
            write_disposition='WRITE_EMPTY',  # pylint: disable=unused-argument
            use_legacy_sql=False,
            max_wait_sec=60,
            expected_schema=None):  # pylint: disable=unused-argument
        """Creates a table in SQLite3 from a specified query.
        SQLite3 requires a schema to create a table. We can't derive the schema from
        returned query results, so we make something up with the right number of columns
        if we get any results at all. If there are no query results we just add a single column.
        If a test heavily relies on column names or correct schemas, it should use
        CreateTablesFromDict instead.

        Args:
          query: The query to run.
          table_path: The path to the table (in the client's project) to write
              the results to.
          write_disposition: Unused but left here for uniformity with bq.py
          use_legacy_sql: Unused but left here for uniformity with bq.py. Should always be false.
          max_wait_sec: Unused but left here for uniformity with bq.py
          expected_schema: The expected schema of the resulting table (only required for mocking).
        Raises:
          RuntimeError if use_legacy_sql is true.
        """

        if use_legacy_sql:
            raise RuntimeError("Legacy SQL is disallowed for this mock.")

        query_results = self.get_query_results(query)

        # Create a schema based on the results returned. Even if the query has no results,
        # SQLite still knows what columns should be there.
        column_names = self._get_column_names_in_result(query)
        if expected_schema is not None:
            if len(expected_schema) != len(column_names):
                raise RuntimeError("The schema doesn't have the same length as the result columns.")
            for i in range(len(expected_schema)):
                if expected_schema[i].name != column_names[i]:
                    raise RuntimeError("The schema has a mismatching column name.")
            # No schema type check because types in SQLite are much different and more ambiguous
            # than types in Bigquery.
            self._create_table(table_path, schema_fields=expected_schema)
        else:
            schema_string = '(' + ','.join(column_names) + ')'
            self._create_table(table_path, schema_string=schema_string)

        if query_results:
            results = [list(itm) for itm in query_results]
            if expected_schema is not None:
                internal_rep = self._cast_to_correct_internal_representation(
                    results, expected_schema)
                self._insert_list_into_table(table_path, internal_rep)
            else:
                self._insert_list_into_table(table_path, results)

    @staticmethod
    def _cast_to_correct_internal_representation(query_results, expected_schema):
        """Turns types into correct internal representation, i.e., 0 -> FALSE for booleans.

        Args:
          query_results: List of list of results
          expected_schema: The schema, the length should match the length of each row
        """
        for row in query_results:
            assert len(row) == len(expected_schema)
            for i in range(len(row)):
                s = expected_schema[i]
                q = row[i]
                if s.field_type in ('BOOLEAN', 'BOOL'):
                    # Representing in all caps to match the existing boolean representation.
                    if q == 0:
                        row[i] = FALSE_STR
                    elif q == 1:
                        row[i] = TRUE_STR
                    else:
                        raise RuntimeError('Boolean typed value was not 0 or 1: {}'.format(q))

        return query_results

    def create_tables_from_dict(self, table_names_to_schemas, dataset_id=None,
                                replace_existing_tables=True):
        """Creates a set of tables from a dictionary of table names to their schemas.

        Args:
          table_names_to_schemas: A dictionary of:
            key: The table name.
            value: A list of SchemaField objects.
          dataset_id: Has no effect.
          replace_existing_tables: Has no effect.
        """
        for table_path, schema in table_names_to_schemas.iteritems():
            self._create_table(table_path, schema_fields=schema)

    def create_dataset_by_name(self, name, expiration_hours=None):
        """Create a new dataset within the current project.

        Args:
          name: The name of the new dataset.
          expiration_hours: Unused but kept for compatibility with bq.
        """
        self.project_map[self.project_id].append(name)
        self.table_map[name] = []

    def delete_dataset_by_name(self, name, delete_all_tables=False):
        """Delete a dataset within the current project.

        Args:
          name: The name of the dataset to delete.
          delete_all_tables: In real BigQuery, a dataset can't be deleted until it's empty
              of tables. For this mock class it's probably not as important since we're
              not directly tracking tables as part of datasets, but there's no good reason
              I can think of that you'd want this to be False in this case since this is
              solely for testing. Still, we provide this functionality to be in parallel
              with the real BigQuery client.

        Raises:
          RuntimeError if there are tables in the dataset you try to delete
        """
        if delete_all_tables:
            for table_path in list(self.tables(name)):
                self.delete_table_by_name(table_path)

        if len(self.tables(name)) > 0:
            raise RuntimeError('The dataset ' + name
                               + ' still contains tables: ' + str(self.tables(name)))

        del self.table_map[name]
        self.project_map[self.project_id].remove(name)

    def delete_table_by_name(self, table_path):
        """Delete a table within the current project.

        Args:
          table_path: A string of the form '<dataset id>.<table name>'.
        """
        project, dataset, table_name = self.parse_table_path(table_path)
        standardized_path = self.path(table_name, dataset, project, REPLACEMENT_DELIMITER)
        self.cursor.execute('DROP TABLE ' + standardized_path)
        self.conn.commit()

        self.table_map[dataset].remove(table_name)

    def tables(self, dataset_id):
        """Returns a list of table names in a given dataset.

        Args:
          dataset_id: The dataset to query.

        Returns:
          A list of table names (strings).
        """
        return self.table_map[dataset_id]

    def get_schema(self, dataset_id, table_name, project_id=None):
        """Returns the schema of a table. Note that due to the imperfect mapping
        of SQLiteTypes to BQ types, these schemas won't be perfect. Anything relying heavily
        on correct schemas should use the real Bigquery client.

        Args:
          dataset_id: The dataset to query.
          table_name: The name of the table.
          project_id: The project ID of the table.
        Returns:
          A list of SchemaFields representing the schema.
        """
        # schema rows are in the format (order, name, type, ...)
        standardized_path = self.path(table_name, dataset_id, project_id, REPLACEMENT_DELIMITER)
        # 'pragma' is SQLite's equivalent to DESCRIBE TABLE
        pragma_query = 'pragma table_info(\'' + standardized_path + '\')'
        single_row_query = 'SELECT * FROM ' + standardized_path + ' LIMIT 1'

        single_row = self.conn.execute(single_row_query).fetchall()
        schema = self.conn.execute(pragma_query).fetchall()

        returned_schema = []
        for i in range(len(schema)):
            row_name = schema[i][1]
            if len(single_row) > 0:
                row_type = self._sqlite_type_to_bq_type(schema[i][2], sample=single_row[0][i])
            else:
                row_type = self._sqlite_type_to_bq_type(schema[i][2])
            # Repeated fields are not supported in mock BigQuery so we always set the mode
            # to nullable.
            returned_schema.append(SchemaField(row_name, row_type, mode='NULLABLE'))
        return returned_schema

    def get_datasets(self):
        """Returns a list of dataset ids in the current project.

        Returns:
          A list of dataset ids/names (strings).
        """
        return self.project_map[self.project_id].keys()

    def populate_table(self, table_path, schema, data=[], max_wait_sec=60, max_tries=1):
        """Create a table and populate it with a list of rows. This mock
        retains the functionality of the original bq client and deletes
        and recreates the table if it was already present.

        Args:
            table_path: A string of the form '<dataset id>.<table name>'.
            schema: A list of SchemaFields representing the schema.
            data: A list of rows, each of which is a list of values.
            max_wait_sec: Has no effect.
            max_tries: Has no effect.
        """
        _, dataset, table_name = self.parse_table_path(table_path, TABLE_PATH_DELIMITER)
        tables_in_dataset = self.table_map[dataset]

        schema_field_list = [x.name + ' ' + self._bq_type_to_sqlite_type(x.field_type)
                             for x in schema]
        schema = '(' + ', '.join(schema_field_list) + ')'

        if table_name in tables_in_dataset:
            self.delete_table_by_name(table_path)
        self._create_table(table_path, schema_string=schema)
        self._insert_list_into_table(table_path, data)

    def append_rows(self, table_path, data, columns=None):
        """Appends the rows contained in data to the table at table_path. This function assumes
        the table itself is already created.

        Args:
          table_path: A string of the form '<dataset id>.<table name>'.
          columns: A list of pairs (<column name>, <value type>).
          data: A list of rows, each of which is a list of values.
          max_wait_sec: The longest we should wait to insert the rows

        Raises:
            RuntimeError: if the schema passed in as columns doesn't match the schema of the
                already-created table represented by table_path
        """
        table_project, dataset_id, table_name = self.parse_table_path(table_path)
        if table_name not in self.tables(dataset_id):
            raise RuntimeError("The table " + table_name + " doesn't exist.")
        if (columns is not None
                and self.get_schema(dataset_id, table_name, table_project) != columns):
            raise RuntimeError("The incoming schema doesn't match the existing table's schema.")
        self._insert_list_into_table(table_path, data)

    def dataset_exists(self, dataset):
        # type: (dataset) -> bool
        """Checks if a dataset exists.

        Since mock_bq does not use real bq objects, this method should remain unimplemented.

        Args:
            dataset: The BQ dataset object to check.
        """
        raise NotImplementedError('dataset_exists is not implemented in mock_bq.')

    def table_exists(self, table):
        # type: (table) -> bool
        """Checks if a table exists.

        Since mock_bq does not use real bq objects, this method should remain unimplemented.

        Args:
            table: The BQ table object to check.
        """
        raise NotImplementedError('table_exists is not implemented in mock_bq.')

    def delete_dataset(self, dataset):
        # type: (dataset) -> None
        """Deletes a dataset.

        Since mock_bq does not use real bq objects, this method should remain unimplemented.

        Args:
            dataset: The BQ dataset object to delete.
        """
        raise NotImplementedError('delete_dataset is not implemented in mock_bq.')

    def delete_table(self, table):
        # type: (table) -> None
        """Deletes a table.

        Since mock_bq does not use real bq objects, this method should remain unimplemented.

        Args:
            table: The BQ table object to delete.
        """
        raise NotImplementedError('delete_table is not implemented in mock_bq.')

    def create_dataset(self, dataset):
        # type: (dataset) -> None
        """Creates a dataset.

        Since mock_bq does not use real bq objects, this method should remain unimplemented.

        Args:
            dataset: The BQ dataset object to create.
        """
        raise NotImplementedError('create_dataset is not implemented in mock_bq.')

    def create_table(self, table):
        # type: (table) -> None
        """Creates a table.

        Since mock_bq does not use real bq objects, this method should remain unimplemented.

        Args:
            table: The BQ table object to representing the table to create.
        """
        raise NotImplementedError('create_table is not implemented in mock_bq.')

    def reload_dataset(self, dataset):
        # type: (dataset) -> None
        """Reloads a dataset.

        Since mock_bq does not use real bq objects, this method should remain unimplemented.

        Args:
            dataset: The BQ dataset object to reload.
        """
        raise NotImplementedError('reload_dataset is not implemented in mock_bq.')

    def reload_table(self, table):
        # type: (table) -> None
        """Reloads a table.

        Since mock_bq does not use real bq objects, this method should remain unimplemented.

        Args:
            table: The BQ table object to reload.
        """
        raise NotImplementedError('reload_table is not implemented in mock_bq.')

    def fetch_data_from_table(self, table):
        # type: (table) -> List[tuple]
        """Fetches data from the given table.

        Since mock_bq does not use real bq objects, this method should remain unimplemented.

        Args:
            table: The BQ table object representing the table from which to fetch data.
        Returns:
            List of tuples, where each tuple is a row of the table.
        """
        raise NotImplementedError('fetch_data_from_table is not implemented in mock_bq.')
