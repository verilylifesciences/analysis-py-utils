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

import random

from ddt import data, ddt, unpack
from google.cloud import bigquery, storage
from google.cloud.bigquery.schema import SchemaField

from verily.bigquery_wrapper import bq, bq_test_case

LONG_TABLE_LENGTH = 200000


@ddt
class BQTest(bq_test_case.BQTestCase):
    @classmethod
    def create_mock_tables(cls):
        # type: () -> None
        """Create mock tables"""
        cls.src_table_name = cls.table_path('tmp')
        cls.client.populate_table(cls.src_table_name, [SchemaField('foo', 'INTEGER'),
                                                       SchemaField('bar', 'INTEGER'),
                                                       SchemaField('baz', 'INTEGER')],
                                  [[1, 2, 3], [4, 5, 6]])

        cls.long_table_name = cls.table_path('long_table')
        cls.client.populate_table(cls.long_table_name, [
            SchemaField('foo', 'INTEGER'),
        ], [[1]] * LONG_TABLE_LENGTH)

    @classmethod
    def create_temp_bucket(cls):
        # type: () -> None
        """Create temporary bucket"""
        cls.temp_bucket_name = str(random.randint(1000000, 9999999))
        cls.bucket = storage.Client(cls.TEST_PROJECT).bucket(cls.temp_bucket_name)
        if not cls.bucket.exists():
            cls.bucket.create()

    @classmethod
    def setUpClass(cls):
        # type: () -> None
        """Set up class"""
        # Because we're testing the actual Bigquery functionality we don't want the mocks.
        super(BQTest, cls).setUpClass(use_mocks=False)
        cls.create_temp_bucket()

    @classmethod
    def tearDownClass(cls):
        # type: () -> None
        """Tear down class"""
        cls.bucket.delete()
        super(BQTest, cls).tearDownClass()

    def tearDown(self):
        # type: () -> None
        """Clean bucket after each test"""
        for blob in self.bucket.list_blobs():
            blob.delete()

    def test_get_bq_table_without_dataset(self):
        # type: () -> None
        """Test whether an error is raised in _get_bq_table if neither dataset_id or default_dataset
        is specified"""
        client_without_dataset = bq.Client(self.TEST_PROJECT)
        with self.assertRaises(ValueError):
            client_without_dataset._get_bq_table(self.src_table_name, None)

    def test_get_bq_table(self):
        # type: () -> None
        """Test _get_bq_table"""
        project_id, dataset_id, table_name = self.client.parse_table_path(self.src_table_name)
        expected_table_id = '{}:{}.{}'.format(project_id, dataset_id, table_name)

        src_table = self.client._get_bq_table(table_name, dataset_id, project_id)
        src_table.reload()

        self.assertEqual(src_table.table_id, expected_table_id)

    def test_load_data(self):
        # type: () -> None
        """Test bq.Client.get_query_results"""
        result = self.client.get_query_results('SELECT * FROM `' + self.src_table_name + '`')
        self.assertTrue((result == [(1, 2, 3), (4, 5, 6)]) or (result == [(4, 5, 6), (1, 2, 3)]))

    @data((500, 500, 'max_results < 10k'), (100000, 100000, 'max_results == 10k'),
          (100001, 100001, 'max_results > 10k'), (LONG_TABLE_LENGTH * 2, LONG_TABLE_LENGTH,
                                                  'max_results > total_rows'), (
                                                      None, LONG_TABLE_LENGTH, 'Load all rows'))
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

        self.assertEqual(len(result), expected_length, test_description)

    @data(('invalid format', True, 'Invalid output_format'), ('avro', True, 'GZIP Avro format'))
    @unpack
    def test_invalid_args_export_table(self, out_fmt, compression, test_description):
        # type: (str, bool, str) -> None
        """Test whether error is raised for invalid arguments in ExportTableToBucket
        Args:
            out_fmt: Output format. Must be one of {'csv', 'json', 'avro'}
            compression: Whether to compress file using GZIP. Cannot be applied to avro
            test_description: A description of the test
        """

        with self.assertRaises(ValueError):
            self.client.export_table_to_bucket(self.src_table_name, self.temp_bucket_name,
                                               'dummy_file', out_fmt, compression)

    # TODO: Add test to export tables from a project different from self.client.project_id
    @data(('csv', True, '', '', 'tmp.csv.gz', 'csv w/ gzip'),
          ('json', True, 'test', '', 'test/tmp.json.gz', 'json w/ gzip'),
          ('avro', False, '/test', '', 'test/tmp.avro', 'Avro w/o gzip'),
          ('csv', True, '', 'ext', 'tmp_ext.csv.gz', 'csv w/ gzip & ext'))
    @unpack
    def test_export_table(self,
                          out_fmt, # type: str
                          compression, # type: bool
                          dir_in_bucket, # type: str
                          output_ext, # type: str
                          expected_output_path, # type: str
                          test_description # type: str
    ):
        # type: (...) -> None
        """Test ExportTableToBucket
        Args:
            out_fmt: Output format. Must be one of {'csv', 'json', 'avro'}
            compression: Whether to compress file using GZIP. Cannot be applied to avro
            dir_in_bucket: The directory in the bucket to store the output files
            output_ext: Extension of the output file name
            expected_output_path: Expected output path
            test_description: A description of the test
        """

        self.client.export_table_to_bucket(self.src_table_name, self.temp_bucket_name,
                                           dir_in_bucket, out_fmt, compression, output_ext)

        self.assertTrue(
            isinstance(self.bucket.get_blob(expected_output_path), storage.Blob), test_description)

    # TODO: Add test to export schemas from a project different from self.client.project_id
    @data(('', '', 'tmp-schema.json', 'Export schema to root'),
          ('test', '', 'test/tmp-schema.json', 'Export schema to /test'),
          ('', 'ext', 'tmp_ext-schema.json', 'Export schema to root with extension'))
    @unpack
    def test_export_schema(self, dir_in_bucket, output_ext, expected_schema_path, test_description):
        # type: (str, str, str, str) -> None
        """Test ExportSchemaToBucket
         Args:
            dir_in_bucket: The directory in the bucket to store the output files
            output_ext: Extension of the output file name
            expected_output_path: Expected output path
            test_description: A description of the test
        """

        self.client.export_schema_to_bucket(self.src_table_name,
                                            self.temp_bucket_name, dir_in_bucket, output_ext)

        self.assertTrue(
            isinstance(self.bucket.get_blob(expected_schema_path), storage.Blob), test_description)

    def test_create_table_from_query(self):
        # type: () -> None
        """Test bq.Client.CreateTablesFromQuery"""
        dest_table = self.table_path('tmp2')
        self.client.create_table_from_query('SELECT * FROM `' + self.src_table_name
                                            + '`', dest_table)
        result = self.client.get_query_results('SELECT * FROM `' + dest_table + '`')
        self.assertTrue((result == [(1, 2, 3), (4, 5, 6)]) or (result == [(4, 5, 6), (1, 2, 3)]))
        self.client.delete_table(dest_table)

    def test_delete_dataset_with_tables_raises(self):
        # type: () -> None
        """Test that deleting a dataset with existing tables will raise an exception."""
        dest_table = self.table_path('tmp2')
        self.client.create_table_from_query('SELECT * FROM `'
                                            + self.src_table_name + '`', dest_table)

        with self.assertRaises(Exception):
            self.client.delete_dataset(self.dataset_name)

    def test_force_delete_dataset_with_tables(self):
        # type: () -> None
        """Test that we can use DeleteDataset to delete all the tables and the dataset. """
        temp_dataset_name = self.dataset_name + 'dataset_with_tables'
        self.client.create_dataset(temp_dataset_name)
        dest_table = self.table_path('to_be_deleted', dataset_name=temp_dataset_name)
        self.client.create_table_from_query('SELECT * FROM `'
                                            + self.src_table_name + '`', dest_table)

        self.client.delete_dataset(temp_dataset_name, True)
        self.assertTrue(temp_dataset_name not in self.client.get_datasets())

    def test_path(self):
        # type: () -> None
        """Test bq.Client.Parsetable_path"""
        table_name = 'my_table'
        table_path = '{}.{}.{}'.format(self.TEST_PROJECT, self.dataset_name, table_name)
        mock_path = '{}.{}.{}'.format('my_project', 'my_dataset', table_name)
        self.assertEqual(table_path, self.client.path('my_table'))
        self.assertEqual(table_path, self.client.path(self.dataset_name + '.' + table_name))
        self.assertEqual(table_path, self.client.path(table_path))
        self.assertEqual(mock_path, self.client.path(table_name, dataset_id='my_dataset',
                                                     project_id='my_project'))

        self.assertEqual((self.TEST_PROJECT, self.dataset_name, table_name),
                         self.client.parse_table_path(table_path))
        self.assertEqual((self.TEST_PROJECT, self.dataset_name, table_name),
                         self.client.parse_table_path(self.dataset_name + '.' + table_name))
        self.assertEqual((self.TEST_PROJECT, self.dataset_name, table_name),
                         self.client.parse_table_path(table_name))

    def test_create_tables_from_dict(self):
        # type: () -> None
        """Test bq.Client.get_schema"""
        self.client.create_tables_from_dict({
            'empty_1':
            [bigquery.SchemaField('col1', 'INTEGER', mode='REPEATED'),
             bigquery.SchemaField('col2', 'STRING')],
            'empty_2':
            [bigquery.SchemaField('col1', 'FLOAT'), bigquery.SchemaField('col2', 'INTEGER')]
        })
        self.assertEqual([('col1', 'INTEGER', 'REPEATED'), ('col2', 'STRING', 'NULLABLE')],
                         [(x.name, x.field_type, x.mode)
                          for x in self.client.get_schema(self.dataset_name, 'empty_1')])
        self.assertEqual([('col1', 'FLOAT', 'NULLABLE'), ('col2', 'INTEGER', 'NULLABLE')],
                         [(x.name, x.field_type, x.mode)
                          for x in self.client.get_schema(self.dataset_name, 'empty_2')])

    def test_add_rows_repeated(self):
        table_name = self.src_table_name + '_for_append_repeated'
        self.client.populate_table(table_name,
                                  [SchemaField('foo', 'INTEGER'),
                                   SchemaField('bar', 'INTEGER', mode='REPEATED')],
                                  [[1, [2, 3]], [4, [5, 6]]])

        self.client.append_rows(table_name, [[7, [8, 9]]])

        self.assertEqual([(1, [2, 3]), (4, [5, 6]), (7, [8, 9])].sort(),
                         self.client.get_query_results("SELECT * FROM " + table_name).sort())

    def test_add_rows(self):
        table_name = self.src_table_name + '_for_append'
        self.client.populate_table(table_name,
                                  [SchemaField('foo', 'INTEGER'),
                                   SchemaField('bar', 'INTEGER'),
                                   SchemaField('baz', 'INTEGER')],
                                  [[1, 2, 3], [4, 5, 6]])

        self.client.append_rows(table_name, [[7, 8, 9]])

        self.assertEqual([(1,2,3), (4,5,6), (7,8,9)].sort(),
                         self.client.get_query_results("SELECT * FROM " + table_name).sort())

    def test_add_rows_bad_schema_raises(self):
        table_name = self.src_table_name + '_for_append_bad_schema'
        self.client.populate_table(table_name,
                                  [SchemaField('foo', 'INTEGER'),
                                   SchemaField('bar', 'INTEGER'),
                                   SchemaField('baz', 'INTEGER')],
                                  [[1, 2, 3], [4, 5, 6]])

        with self.assertRaises(RuntimeError):
            self.client.append_rows(table_name,
                                    [[7, 8, 9]],
                                    [SchemaField('foo', 'INTEGER'), SchemaField('bar', 'INTEGER')])

if __name__ == '__main__':
    bq_test_case.main()
