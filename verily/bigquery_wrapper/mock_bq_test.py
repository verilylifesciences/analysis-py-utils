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
"""Unit tests for the bq library."""

# Workaround for https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2366
from __future__ import absolute_import

from ddt import ddt
from google.cloud.bigquery.schema import SchemaField

from verily.bigquery_wrapper import bq_test_case, bq_shared_tests
# We use the standard BQ_PATH_DELIMITER throughout the test cases because all the functions in
# mock BQ should take in real BQ paths and handle them correctly.
from verily.bigquery_wrapper.bq_base import BQ_PATH_DELIMITER


@ddt
class MockBQTest(bq_shared_tests.BQSharedTests):
    @classmethod
    def create_mock_tables(cls):
        # type: () -> None
        """Create mock tables"""

        super(MockBQTest, cls).create_mock_tables()

        cls.dates_table_name = cls.client.path('dates', delimiter=BQ_PATH_DELIMITER)
        cls.client.populate_table(
                cls.dates_table_name,
                [SchemaField('foo', 'DATETIME'),
                 SchemaField('bar', 'INTEGER'),
                 SchemaField('baz', 'INTEGER')],
                [['1987-05-13 00:00:00', 2, 3], ['1950-01-01 00:00:00', 5, 6]], )

        cls.str_table_name = cls.client.path('strings', delimiter=BQ_PATH_DELIMITER)
        cls.client.populate_table(
                cls.str_table_name,
                [SchemaField('char1', 'STRING')],
                [['123'], ['456']], )

    @classmethod
    def setUpClass(cls):
        # type: () -> None
        """Set up class"""
        super(MockBQTest, cls).setUpClass(use_mocks=True)

    @classmethod
    def tearDownClass(cls):
        # type: () -> None
        """Tear down class"""
        super(MockBQTest, cls).tearDownClass()

    def test_query_needs_legacy_sql_prefix_removed(self):
        # type: () -> None
        self.expect_query_result('#legacySQL\nSELECT baz FROM `{}`'.format(self.src_table_name),
                                 [(3,), (6,)],
                                 enforce_ordering=False)

    def test_query_needs_standard_sql_prefix_removed(self):
        # type: () -> None
        self.expect_query_result('#standardSQL\nSELECT baz FROM `{}`' .format(self.src_table_name),
                                 [(3,), (6,)],
                                 enforce_ordering=False)

    def test_query_needs_division_fixed(self):
        # type: () -> None
        self.expect_query_result('SELECT (foo / bar) , baz FROM `{}`' .format(self.src_table_name),
                                 [(0.5, 3), (0.8, 6)],
                                 enforce_ordering=False)

    def test_query_needs_concat_fixed(self):
        # type: () -> None
        self.expect_query_result('SELECT CONCAT(foo || bar) , baz FROM `{}`'
                                 .format(self.src_table_name),
                                 [('12', 3), ('45', 6)],
                                 enforce_ordering=False)

    def test_query_needs_format_fixed(self):
        # type: () -> None
        # Some environments have an older version of sqlite3 installed which is fine
        # for all tests except this one.
        import sqlite3
        if sqlite3.sqlite_version != '3.8.2':
            self.expect_query_result('SELECT FORMAT(\'%d and %d\', foo, bar) , baz '
                                                   + 'FROM `{}`'.format(self.src_table_name),
                                     [('1 and 2', 3), ('4 and 5', 6)],
                                     enforce_ordering=False)

    def test_query_needs_extract_year_fixed(self):
        # type: () -> None
        self.expect_query_result('SELECT EXTRACT(YEAR FROM foo) FROM `{}`'
                                 .format(self.dates_table_name),
                                 [(1987,), (1950,)],
                                 enforce_ordering=False)

    def test_query_needs_extract_month_fixed(self):
        # type: () -> None
        self.expect_query_result('SELECT EXTRACT(MONTH FROM foo) FROM `{}`'
                                 .format(self.dates_table_name),
                                 [(5,), (1,)],
                                 enforce_ordering=False)

def test_query_needs_substr_fixed(self):
    # type: () -> None
    self.expect_query_result('SELECT SUBSTR(char1,0,2) FROM `{}`'.format(self.str_table_name),
                             [('12',), ('45',)],
                             enforce_ordering=False)

def test_query_needs_farm_fingerprint_fixed(self):
    # type: () -> None
    self.expect_query_result('SELECT FARM_FINGERPRINT(CONCAT(CAST(1),CAST(2))) FROM `{}`'
                             .format(self.src_table_name),
                             [('0',),])

def test_query_needs_farm_fingerprint_fixed_complex(self):
    # type: () -> None
    self.expect_query_result('SELECT FARM_FINGERPRINT(CONCAT(CAST(1 AS STRING),"/",'
                             'CAST(2 AS STRING))) FROM `{}`'
                             .format(self.src_table_name),
                             [('0',),])

def test_query_needs_mod_fixed(self):
    # type: () -> None
    self.expect_query_result('SELECT MOD(foo,4) FROM `{}`'.format(self.src_table_name),
                             [(0,), (1,)],
                             enforce_ordering=False)

def test_query_needs_wont_execute_in_sqlite_raises(self):
    # type: () -> None
    with self.assertRaises(RuntimeError):
        self.client.get_query_results('bad query')


def test_query_legacysql_raises(self):
    # type: () -> None
    with self.assertRaises(RuntimeError):
        self.client.get_query_results(
                'SELECT * FROM `{}`'.format(self.src_table_name), use_legacy_sql=True)


if __name__ == '__main__':
    bq_test_case.main()
