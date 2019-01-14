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
"""BQTestCase class, a subclass of unittest.TestCase for the BigQuery library.

This class creates a temporary dataset before running the tests and tears it
down at the end. Subclasses should override create_mock_tables() to create
temporary tables by calling bq.PopulateTable().
"""

# Workaround for https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2366
from __future__ import absolute_import

import datetime
import json
import logging
import os
import random
import sys
import unittest
import uuid

import numpy as np
import pandas as pd

from google.cloud.bigquery.schema import SchemaField
from typing import Optional

from verily.bigquery_wrapper import bq as real_bq
try:
    from verily.bigquery_wrapper import mock_bq
except ImportError as e:
    print("***NOTE: Only BigQuery tests will work in this environment:\n\t" + str(e))

from verily.bigquery_wrapper.pandas_utils import safe_read_csv

# We do our best to clean up all the test datasets, but even if something goes wrong
# we set testing tables to delete themselves after an hour. This won't eliminate the
# datasets themselves but at least it cleans up the tables.
EXPIRATION_HOURS = 1

NP_TYPE_LOOKUP = {'STRING': str, 'INT64': np.int64, 'INTEGER': np.int64, 'DATE': str,
                  'TIMESTAMP': str, 'FLOAT64': np.float64, 'FLOAT': np.float64, 'BOOL': bool,
                  'BOOLEAN': bool}

SELECT_ALL_FORMAT = 'SELECT * FROM `{}`'


class BQTestCase(unittest.TestCase):
    """Provides a client and a temporary dataset for testing SQL queries.
    By default, it will use an acutal BigQuery client. If you pass in
    use_mocks=True to the setup method, then it will use a mock environment backed by an in-memory
    database.
    """

    # If the FORCE_USE_REAL_BQ_FOR_TESTS environment variable is set to anything but empty then it
    # will force all testing BigQuery clients to be real BigQuery, overriding any that are set
    # to use mock BigQuery.
    FORCE_USE_REAL_BQ = os.environ.get('FORCE_USE_REAL_BQ_FOR_TESTS')

    TEST_PROJECT = os.getenv('TEST_PROJECT')
    tables_created_in_constructor = []

    @classmethod
    def setUpClass(cls, use_mocks=False):
        """Sets up BQ client and creates dataset and tables for testing.

        Args:
            use_mocks: Whether to use a mock implementation of the client.
        """
        force_use_real_bq = bool(cls.FORCE_USE_REAL_BQ)
        if force_use_real_bq:
            log = logging.getLogger("BQTestCase")
            log.warning('Forcing test to run in real BigQuery.')

        cls.use_mocks = False if force_use_real_bq else use_mocks
        cls.default_test_dataset_id = (datetime.datetime.utcnow().strftime("test_%Y_%m_%d_%H_%M_") +
                                       str(random.SystemRandom().randint(1000, 9999)))
        if cls.use_mocks:
            cls.TEST_PROJECT = 'mock_bq_project'
            cls.client = mock_bq.Client(cls.TEST_PROJECT, cls.default_test_dataset_id)
        else:
            if cls.TEST_PROJECT is None:
              raise ValueError("Environment variable 'TEST_PROJECT' is not set. "
                               "Set its value to be the project id in which you "
                               "wish to run test queries.")
            cls.client = real_bq.Client(cls.TEST_PROJECT, cls.default_test_dataset_id)
        # Make the tables in the test datasets expire after an hour.
        cls.client.create_dataset_by_name(cls.default_test_dataset_id,
                                          expiration_hours=EXPIRATION_HOURS)
        cls.create_mock_tables()

        # Get the tables present in the class before anything else runs.
        cls.tables_created_in_constructor.extend(cls.client.tables(cls.default_test_dataset_id))

    def tearDown(self):
        # After each test, delete the tables created apart from class setup.
        current_tables = self.client.tables(self.default_test_dataset_id)
        for table in current_tables:
            if table not in self.tables_created_in_constructor:
                self.client.delete_table_by_name(table)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.client.delete_dataset_by_name(cls.default_test_dataset_id, delete_all_tables=True)
        # TODO(Issue 6): This exception should probably be handled differently but I'm not sure how.
        except Exception as ex:
            log = logging.getLogger("BQTestCase")
            log.warning("Problem deleting dataset: " + str(ex) + ";"
                        "Dataset contains tables: " + str(cls.client.tables(
                    cls.default_test_dataset_id)))

    @classmethod
    def create_mock_tables(cls):
        """Subclasses should override this to create up mock tables."""

        raise NotImplementedError("BQTestCase is an abstract class.")

    @staticmethod
    def make_n_digit_random_number(n):
        if not isinstance(n, int) or n < 1 or n > 36:
            raise ValueError('n must be an integer between 1 and 36.')
        return str(uuid.uuid4().int)[:n]

    @staticmethod
    def create_synthetic_table_query(cols, rows):
        """Creates a query that returns results that look like a table.

        This can be used to mock out a subquery.  It can also be used in place of a
        temporary table, it is generally faster to test.

        Args:
          cols: An array of the column names for this synthetic table.
          rows: A two dimensional array of cells for this synthetic table.

        Returns:
          A query of the form "SELECT 1 as ColA UNION ALL SELECT 2 as ColA".
        """

        selects = ('SELECT ' + ','.join(('{} AS {}'.format(repr(cell), col)
                                         for cell, col in zip(row, cols))) for row in rows)
        return '(' + '\nUNION ALL\n'.join(selects) + ')'

    @classmethod
    def populate_sparse_table(cls,
                              table_name,    # type: str
                              table_schema,  # type: List[google.cloud.bigquery.schema.SchemaField]
                              col_subset,    # type: List[str]
                              row_data,      # type: List[List[Any]]
                              constants_dict=None  # type: Dict[str, Any]
                              ):
        # type: (...) -> None
        """
        Populate a subset of columns in a table.

        Args:
            table_name: name of the table
            table_schema: the full table schema, as a list of
                google.cloud.bigquery.schema.SchemaFields
            col_subset: list of column names to be populated
            row_data: list of lists, where each sublist represents a row, where each datum belong in
                the column at the same index in col_subset
            constants_dict: dict from column names to values. For any columns listed here, all rows
                will have the same given value.
        """
        # If no dict given for constants_dict, make it empty. It is not safe to give an empty dict
        # as a default argument because it is mutable and changes are preserved across method runs.
        if constants_dict is None:
            constants_dict = {}

        all_col_names = [schema_field.name for schema_field in table_schema]
        # Validate that col_subset and constants_dict column names are consistent with table_schema
        for col_name in col_subset:
            if col_name not in all_col_names:
                raise ValueError('Col name %s not found in schema' % col_name)
        for col_name in constants_dict:
            if col_name not in all_col_names:
                raise ValueError('Col name %s not found in schema' % col_name)

        # Map a mapping from table column names to indices of those columns in col_subset.
        col_map = {}
        for table_col in all_col_names:
            if table_col in col_subset:
                col_map[table_col] = col_subset.index(table_col)
        data = []
        for input_row in row_data:
            output_row = []
            for table_col in all_col_names:
                if table_col in constants_dict:
                    output_row.append(constants_dict[table_col])
                elif table_col in col_map:
                    output_row.append(input_row[col_map[table_col]])
                else:
                    output_row.append(None)
            assert len(all_col_names) == len(output_row), \
                'Output row should have an item for each table column'
            data.append(output_row)
        assert len(data) == len(row_data), \
            'Generated data should have as many rows as input'
        table_path = cls.client.path(table_name)
        cls.client.populate_table(table_path, table_schema, data, make_immediately_available=True)

    def _comment_helper(self, comments_list, idx):
        """Return the comment from the element in the list or '' if comment_list is empty"""
        if not comments_list:
            return ''
        return comments_list[idx]

    def expect_table_contains(self, table_path, expected):
        """
        Checks whether the table at table_path contains exactly the rows in expected.

        Args:
            table_path: The table path to check
            expected: The rows expected to be in table_path
        """
        query = SELECT_ALL_FORMAT.format(table_path)
        self.expect_query_result(query, expected, enforce_ordering=False)

    def expect_query_result(self, query, expected, enforce_ordering=True, comments=None):
        """Compiles the query, runs it and checks the results.

        Args:
          query: The query to execute.
          expected: A two dimensional array of query results to compare against.
          enforce_ordering: If True, the rows must appear in the same order in the query results and
              in the expected results. If False, then it will just check for count equality and then
              for set equality between the two results.
          comments: list of comments, one per expected row.
        """
        if comments:
            self.assertEquals(len(expected), len(comments),
                              'Bad test arguments {} rows of expected and {} comments.\n\n'.format(
                                      len(expected), len(comments)))

        results = self.client.get_query_results(query)

        if self.use_mocks:
            expected = self._typecast_mock_compatible(expected)
            results = self._typecast_mock_compatible(results)

        self.assertEqual(
            len(results),
            len(expected), 'Expected {} rows of data, found {}.\n\n'
            'Expected:\n{}\n\nFound:\n{}'.format(
                len(expected), len(results), _data_to_string(expected), _data_to_string(results)))

        if not enforce_ordering:
            self.assertSetEqual(set(results), set(expected))
            return

        for i in range(len(results)):
            self.assertEqual(
                len(results[i]),
                len(expected[i]), 'Expected {} values in row {}, found {}.\n\n'
                'Expected:\n{}\n\nFound:\n{}\nComment:\n{}'.format(
                    len(expected[i]), i,
                    len(results[i]), _data_to_string(expected), _data_to_string(results),
                    self._comment_helper(comments, i)))
            for j in range(len(results[i])):
                if isinstance(results[i][j], float):
                    self.assertAlmostEqual(results[i][j], expected[i][j], 3,
                                           'Expected {} in row {}, column {}, but found {}.\n\n'
                                           'Expected:\n{}\n\nFound:\n{}\nComment:\n{}'.format(
                                               expected[i][j], i, j, results[i][j],
                                               _data_to_string(expected), _data_to_string(results),
                                           self._comment_helper(comments, i)))
                else:
                    self.assertEqual(results[i][j], expected[i][j],
                                     'Expected {} in row {}, column {}, but found {}.\n\n'
                                     'Expected:\n{}\n\nFound:\n{}\nComment:\n{}'.format(
                                         expected[i][j], i, j, results[i][j],
                                         _data_to_string(expected), _data_to_string(results),
                                     self._comment_helper(comments, i)))

    @staticmethod
    def _typecast_mock_compatible(results):
        """
        The types in SQLite do not align with BigQuery types, so this function aligns them by
        narrowing down the possible types to booleans, floats, ints, and strings.
        Args:
            results: A list representing results, where each item in the list is another list
                representing a results row.
        Returns:
            An equivalent of the list with all values as booleans, ints, floats, and strings.
        """
        typecast_result = []
        for row in results:
            typecast_row = []
            for col in row:
                # Try for boolean typecasting.
                if isinstance(col, bool):
                    cast = col
                elif str(col).lower() == 'true':
                    cast = True
                elif str(col).lower() == 'false':
                    cast = False
                # Check for numerical types.
                elif isinstance(col, int) or isinstance(col, float):
                    cast = col
                # Anything else becomes a string.
                else:
                    cast = str(col)
                typecast_row.append(cast)
            typecast_result.append(tuple(typecast_row))
        return typecast_result

    @staticmethod
    def _load_csv_with_schema(schema_file, data_file, parse_dates=False):
        # type: (str, str, bool) -> List[Any]
        """
        This loads data_file using the types from schema_file and returns a list of lists
        Args:
            schema_file: Json file with the 'name' and 'type' of the columns
            data_file: csv file with the data
            parse_dates: if the date types should be converted to datetime.date

        Returns:
            A list of lists representing the data.

        """

        # Create the conversion factor table
        with open(schema_file) as f:
            schema_json = json.load(f)
        dtype_map = {}
        date_col_indicies = [] if parse_dates else None
        for idx, d in enumerate(schema_json):
            dtype_map[d['name']] = NP_TYPE_LOOKUP[d['type']]
            if parse_dates and d['type'] == 'DATE':
                date_col_indicies.append(idx)

        def parser(x): return datetime.datetime.strptime(x, "%Y-%m-%d").date()

        df = safe_read_csv(data_file, dtype_map)

        df = df.where(pd.notnull(df), None)
        values = df.values.tolist()
        if parse_dates:
            for row in values:
                for idx in date_col_indicies:
                    row[idx] = parser(row[idx])

        return values

    @classmethod
    def create_test_table(cls, table_name, schema_file, data_file=None, table_postfix=''):
        # type: (str, str, Optional[str], str) -> ()
        """
        This method creates a table to be used in testing

        Args:
            table_name: A string with the name of the table
            schema_file: A string with the name of the schema file from which to build the test
                table
            data_file: A string describing the file with the data to use in testing. If None,
                create an empty table.
            table_postfix: A string to be added to the end of the table_name.
        """
        data = cls._load_csv_with_schema(schema_file, data_file) if data_file else []
        table_path = cls.client.path(table_name + table_postfix)
        with open(schema_file) as f:
            schema_json = json.load(f)
        schema_list = [SchemaField(row['name'], row['type']) for row in schema_json]
        cls.client.populate_table(table_path, schema_list, data, make_immediately_available=True)

    @staticmethod
    def _get_fields_from_schema(schema_file):
        # type: (str) -> str
        """This method takes a schema and returns a comma string of all column names

        Args:
            schema_file: A string with the name of the schema file

        Returns:
            Returns a string with all column names joined together
        """
        with open(schema_file) as f:
            schema_json = json.load(f)
        names = [row['name'] for row in schema_json]
        return ','.join(names)


def _data_to_string(data):
    return '\n'.join([str(list(row)) for row in data])


def main():
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger("BQTestCase").setLevel(logging.WARNING)
    unittest.main()