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

from ddt import data, ddt, unpack
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

from verily.bigquery_wrapper import bq_test_case, mock_bq

LONG_TABLE_LENGTH = 200000

FOO_BAR_BAZ_INTEGERS_SCHEMA = [SchemaField('foo', 'INTEGER'),
                               SchemaField('bar', 'INTEGER'),
                               SchemaField('baz', 'INTEGER')]

@ddt
class BQTest(bq_test_case.BQTestCase):
    @classmethod
    def create_mock_tables(cls):
        # type: () -> None
        """Create mock tables"""
        cls.src_table_name = cls.client.path('tmp', delimiter=mock_bq.REPLACEMENT_DELIMITER)
        cls.client.populate_table(
            cls.src_table_name,
                FOO_BAR_BAZ_INTEGERS_SCHEMA,
            [[1, 2, 3], [4, 5, 6]], )

        cls.dates_table_name = cls.client.path('dates', delimiter=mock_bq.REPLACEMENT_DELIMITER)
        cls.client.populate_table(
            cls.dates_table_name,
            [SchemaField('foo', 'DATETIME'),
             SchemaField('bar', 'INTEGER'),
             SchemaField('baz', 'INTEGER')],
            [['1987-05-13 00:00:00', 2, 3], ['1950-01-01 00:00:00', 5, 6]], )

        cls.long_table_name = cls.client.path('long_table', delimiter=mock_bq.REPLACEMENT_DELIMITER)
        cls.client.populate_table(cls.long_table_name, [
            SchemaField('foo', 'INTEGER'),
        ], [[1]] * LONG_TABLE_LENGTH)

        cls.str_table_name = cls.client.path('strings', delimiter=mock_bq.REPLACEMENT_DELIMITER)
        cls.client.populate_table(
            cls.str_table_name,
            [SchemaField('char1', 'STRING')],
            [['123'], ['456']], )

    @classmethod
    def setUpClass(cls):
        # type: () -> None
        """Set up class"""
        super(BQTest, cls).setUpClass(use_mocks=True)

    @classmethod
    def tearDownClass(cls):
        # type: () -> None
        """Tear down class"""
        super(BQTest, cls).tearDownClass()

    def test_load_data(self):
        # type: () -> None
        """Test bq.Client.get_query_results"""
        result = self.client.get_query_results('SELECT * FROM `' + self.src_table_name + '`')
        self.assertTrue((result == [(1, 2, 3), (4, 5, 6)]) or (result == [(4, 5, 6), (1, 2, 3)]))

    # The numbers here match the test numbers for the actual BQ client, so this test just ensures
    # that the max_results parameter works the same in the mock environment as in the real one.
    @data(
        (500, 500, 'max_results < 10k'),
        (100000, 100000, 'max_results == 10k'),
        (100001, 100001, 'max_results > 10k'),
        (LONG_TABLE_LENGTH * 2, LONG_TABLE_LENGTH, 'max_results > total_rows'),
        (None, LONG_TABLE_LENGTH, 'Load all rows'), )
    @unpack
    def test_load_large_data(self, max_results, expected_length, test_description):
        # type: (int, int, str) -> None
        """Test using bq.Client.get_query_results to load very large data
        Args:
            max_results: Maximum number of results to return
            expected_length: Expected length of results to return
        """
        result = self.client.get_query_results(
            'SELECT * FROM `' + self.long_table_name + '`', max_results=max_results)

        self.assertEqual(
            len(result), expected_length, test_description + '; expected: ' + str(expected_length) +
            ' actual: ' + str(len(result)))

    def test_create_table_from_query(self):
        # type: () -> None
        dest_table = self.client.path('tmp2', delimiter=mock_bq.REPLACEMENT_DELIMITER)
        self.client.create_table_from_query('SELECT * FROM `'
                                            + self.src_table_name + '`', dest_table)
        result = self.client.get_query_results('SELECT * FROM `' + dest_table + '`')
        self.assertSetEqual(set(result), set([(1, 2, 3), (4, 5, 6)]))
        self.client.delete_table(dest_table)

    def test_query_needs_legacy_sql_prefix_removed(self):
        # type: () -> None
        result = self.client.get_query_results('#legacySQL\nSELECT baz FROM `'
                                               + self.src_table_name
                                               + '`')
        self.assertSetEqual(set(result), set([(3,), (6,)]))

    def test_query_needs_standard_sql_prefix_removed(self):
        # type: () -> None
        result = self.client.get_query_results('#standardSQL\nSELECT baz FROM `'
                                               + self.src_table_name
                                               + '`')
        self.assertSetEqual(set(result), set([(3,), (6,)]))

    def test_query_needs_division_fixed(self):
        # type: () -> None
        result = self.client.get_query_results('SELECT (foo / bar) , baz FROM `'
                                               + self.src_table_name
                                               + '`')
        self.assertSetEqual(set(result), set([(0.5, 3), (0.8, 6)]))

    def test_query_needs_concat_fixed(self):
        # type: () -> None
        result = self.client.get_query_results('SELECT CONCAT(foo || bar) , baz FROM `' +
                                               self.src_table_name + '`')
        self.assertSetEqual(set(result), set([('12', 3), ('45', 6)]))

    def test_query_needs_format_fixed(self):
        # type: () -> None
        result = self.client.get_query_results('SELECT FORMAT(\'%d and %d\', foo, bar) , baz '
                                               + 'FROM `' + self.src_table_name + '`')
        self.assertSetEqual(set(result), set([('1 and 2', 3), ('4 and 5', 6)]))

    def test_query_needs_extract_year_fixed(self):
        # type: () -> None
        result = self.client.get_query_results('SELECT EXTRACT(YEAR FROM foo) FROM `' +
                                               self.dates_table_name + '`')
        self.assertSetEqual(set(result), set([(1987, ), (1950, )]))

    def test_query_needs_extract_month_fixed(self):
        # type: () -> None
        result = self.client.get_query_results('SELECT EXTRACT(MONTH FROM foo) FROM `' +
                                               self.dates_table_name + '`')
        self.assertSetEqual(set(result), set([(5, ), (1, )]))

    def test_query_needs_substr_fixed(self):
        # type: () -> None
        result = self.client.get_query_results(
                'SELECT SUBSTR(char1,0,2) FROM `' + self.str_table_name + '`')
        self.assertSetEqual(set(result), set([(u'12', ), (u'45', )]))

    def test_query_needs_farm_fingerprint_fixed(self):
        # type: () -> None
        result = self.client.get_query_results(
                'SELECT FARM_FINGERPRINT(CONCAT(CAST(1),CAST(2))) FROM `' +
                                               self.src_table_name + '`')
        self.assertSetEqual(set(result), set([(0, )]))

    def test_query_needs_farm_fingerprint_fixed_complex(self):
        # type: () -> None
        result = self.client.get_query_results(
                'SELECT FARM_FINGERPRINT(CONCAT(CAST(1 AS STRING),"/",CAST(2 AS STRING))) FROM `' +
                                               self.src_table_name + '`')
        self.assertSetEqual(set(result), set([(0, )]))

    def test_query_needs_mod_fixed(self):
        # type: () -> None
        result = self.client.get_query_results(
                'SELECT MOD(foo,4) FROM `' + self.src_table_name + '`')
        self.assertSetEqual(set(result), set([(0, ), (1, )]))

    def test_query_needs_wont_execute_in_sqlite_raises(self):
        # type: () -> None
        with self.assertRaises(RuntimeError):
            self.client.get_query_results('bad query')

    def test_query_legacysql_raises(self):
        # type: () -> None
        with self.assertRaises(RuntimeError):
            self.client.get_query_results(
                'SELECT * FROM `' + self.src_table_name + '`', use_legacy_sql=True)

    def test_create_tables_from_dict(self):
        # type: () -> None
        self.client.create_tables_from_dict({
            'empty_1': [
                bigquery.SchemaField('col1', 'INTEGER'),
                bigquery.SchemaField('col2', 'STRING'),
            ],
            'empty_2': [
                bigquery.SchemaField('col1', 'FLOAT'),
                bigquery.SchemaField('col2', 'INTEGER'),
            ]
        })
        self.assertEqual([('col1', 'INTEGER', 'NULLABLE'), ('col2', 'STRING', 'NULLABLE')],
                         [(x.name, x.field_type, x.mode)
                          for x in self.client.get_schema(self.dataset_name, 'empty_1')])
        self.assertEqual([('col1', 'FLOAT', 'NULLABLE'), ('col2', 'INTEGER', 'NULLABLE')],
                         [(x.name, x.field_type, x.mode)
                          for x in self.client.get_schema(self.dataset_name, 'empty_2')])

    def test_populate_table(self):
        # type: () -> None
        dest_table = self.client.path('pop_table', delimiter=mock_bq.REPLACEMENT_DELIMITER)
        self.client.populate_table(dest_table, [SchemaField('col1', 'INTEGER'),
                                                SchemaField('col2', 'STRING')],
                                   [(1, 'a'), ('2', 'c')])
        result = self.client.get_query_results('SELECT * FROM `' + dest_table + '`')
        self.assertSetEqual(set(result), set([(1, 'a'), (2, 'c')]))

    def test_populate_table_with_nulls(self):
        # type: () -> None
        dest_table = self.client.path('pop_table', delimiter=mock_bq.REPLACEMENT_DELIMITER)
        self.client.populate_table(dest_table, [SchemaField('col1', 'INTEGER'),
                                                SchemaField('col2', 'STRING')],
                                   [(1, None), (2, 'c')])
        result = self.client.get_query_results(
            'SELECT * FROM `' + dest_table + '` WHERE col2 IS NULL')
        self.assertSetEqual(set(result), set([(1, None)]))

    def test_populate_table_with_64_types(self):
        # type: () -> None
        dest_table = self.client.path('pop_table', delimiter=mock_bq.REPLACEMENT_DELIMITER)
        self.client.populate_table(dest_table, [SchemaField('col1', 'INT64'),
                                                SchemaField('col2', 'FLOAT64')],
                                   [(1, 2.5), (20, 6.5)])
        result = self.client.get_query_results('SELECT * FROM `' + dest_table + '`')
        self.assertSetEqual(set(result), set([(1, 2.5), (20, 6.5)]))

    def test_add_rows(self):
        table_name = self.src_table_name + '_for_append'
        self.client.populate_table(table_name,
                                   FOO_BAR_BAZ_INTEGERS_SCHEMA,
                                   [[1, 2, 3], [4, 5, 6]])

        self.client.append_rows(table_name, [[7, 8, 9]])

        self.assertEqual([(1,2,3), (4,5,6), (7,8,9)].sort(),
                         self.client.get_query_results("SELECT * FROM " + table_name).sort())

    def test_add_rows_bad_schema_raises(self):
        table_name = self.src_table_name + '_for_append'
        self.client.populate_table(table_name,
                                   FOO_BAR_BAZ_INTEGERS_SCHEMA,
                                   [[1, 2, 3], [4, 5, 6]])

        with self.assertRaises(RuntimeError):
            self.client.append_rows(table_name,
                                    [[7, 8, 9]],
                                    [SchemaField('foo', 'INTEGER'),
                                     SchemaField('bar', 'INTEGER')])

if __name__ == '__main__':
    bq_test_case.main()
