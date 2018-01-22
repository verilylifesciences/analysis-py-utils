from __future__ import absolute_import

import random
import time

from ddt import data, ddt, unpack
from google.cloud.bigquery.schema import SchemaField

from verily.bigquery_wrapper import bq_test_case
# We use the standard BQ_PATH_DELIMITER throughout the test cases because all the functions in
# mock BQ should take in real BQ paths and handle them correctly.
from verily.bigquery_wrapper.bq_base import BQ_PATH_DELIMITER

LONG_TABLE_LENGTH = 200000

FOO_BAR_BAZ_INTEGERS_SCHEMA = [SchemaField('foo', 'INTEGER'),
                               SchemaField('bar', 'INTEGER'),
                               SchemaField('baz', 'INTEGER')]

SELECT_ALL_FORMAT = 'SELECT * FROM `{}`'


@ddt
class BQSharedTests(bq_test_case.BQTestCase):
    @classmethod
    def setUpClass(cls, use_mocks=False):
        # type: () -> None
        """Set up class"""
        super(BQSharedTests, cls).setUpClass(use_mocks=use_mocks)

    @classmethod
    def create_mock_tables(cls):
        # type: () -> None
        """Create mock tables"""
        cls.src_table_name = cls.client.path('tmp', delimiter=BQ_PATH_DELIMITER)
        cls.client.populate_table(
                cls.src_table_name,
                FOO_BAR_BAZ_INTEGERS_SCHEMA,
                [[1, 2, 3], [4, 5, 6]], )

        cls.long_table_name = cls.client.path('long_table', delimiter=BQ_PATH_DELIMITER)
        cls.client.populate_table(cls.long_table_name, [
            SchemaField('foo', 'INTEGER'),
            ], [[1]] * LONG_TABLE_LENGTH)

    def test_load_data(self):
        # type: () -> None
        """Test bq.Client.get_query_results"""
        result = self.client.get_query_results(SELECT_ALL_FORMAT.format(self.src_table_name))
        self.assertSetEqual(set(result), set([(1, 2, 3), (4, 5, 6)]))

    @data((LONG_TABLE_LENGTH, 'Load all rows'), )
    @unpack
    def test_load_large_data(self, expected_length, test_description):
        # type: (int, str) -> None
        """Test using bq.Client.get_query_results to load very large data
        Args:
            expected_length: Expected length of results to return
        """
        result = self.client.get_query_results(SELECT_ALL_FORMAT.format(self.long_table_name))

        self.assertEqual(
                len(result), expected_length,
                test_description + '; expected: ' + str(expected_length) +
                ' actual: ' + str(len(result)))

    def test_create_table_from_query(self):
        # type: () -> None
        dest_table = self.client.path('tmp2', delimiter=BQ_PATH_DELIMITER)
        self.client.create_table_from_query(SELECT_ALL_FORMAT.format(self.src_table_name),
                                            dest_table)
        result = self.client.get_query_results(SELECT_ALL_FORMAT.format(dest_table))
        self.assertSetEqual(set(result), set([(1, 2, 3), (4, 5, 6)]))
        self.client.delete_table_by_name(dest_table)

    def test_create_tables_from_dict(self):
        # type: () -> None
        self.client.create_tables_from_dict({
            'empty_1': [
                SchemaField('col1', 'INTEGER'),
                SchemaField('col2', 'STRING'),
                ],
            'empty_2': [
                SchemaField('col1', 'FLOAT'),
                SchemaField('col2', 'INTEGER'),
                ]
            })
        self.assertEqual([('col1', 'INTEGER', 'NULLABLE'), ('col2', 'STRING', 'NULLABLE')],
                         [(x.name, x.field_type, x.mode)
                          for x in self.client.get_schema(self.default_test_dataset_id, 'empty_1')])
        self.assertEqual([('col1', 'FLOAT', 'NULLABLE'), ('col2', 'INTEGER', 'NULLABLE')],
                         [(x.name, x.field_type, x.mode)
                          for x in self.client.get_schema(self.default_test_dataset_id, 'empty_2')])

    def test_create_tables_from_dict_overwrite(self):
        # type: () -> None
        # Create the dataset once.
        self.client.create_tables_from_dict({
            'empty_1':
                [SchemaField('col1', 'INTEGER'),
                 SchemaField('col2', 'STRING')],
            'empty_2':
                [SchemaField('col1', 'FLOAT'), SchemaField('col2', 'INTEGER')]
            },
                replace_existing_tables=True)

        # Create it again with a different schema. Make sure the changes take since it should have
        # recreated the dataset.
        self.client.create_tables_from_dict({
            'empty_1':
                [SchemaField('col1_test1', 'INTEGER'),
                 SchemaField('col2_test2', 'STRING')],
            'empty_2':
                [SchemaField('col1_test1', 'FLOAT'),
                 SchemaField('col2_test2', 'INTEGER')]
            },
                replace_existing_tables=True)
        self.assertEqual([('col1_test1', 'INTEGER', 'NULLABLE'),
                          ('col2_test2', 'STRING', 'NULLABLE')],
                         [(x.name, x.field_type, x.mode)
                          for x in self.client.get_schema(self.default_test_dataset_id, 'empty_1')])
        self.assertEqual([('col1_test1', 'FLOAT', 'NULLABLE'),
                          ('col2_test2', 'INTEGER', 'NULLABLE')],
                         [(x.name, x.field_type, x.mode)
                          for x in self.client.get_schema(self.default_test_dataset_id, 'empty_2')])

        # Try to create one of the tables again; it should raise a RuntimeError.
        with self.assertRaises(RuntimeError):
            self.client.create_tables_from_dict({
                'empty_1':
                    [SchemaField('col1', 'INTEGER'),
                     SchemaField('col2', 'STRING')],
                },
                    replace_existing_tables=False)

        # Try to create a table not in the dataset. It should work fine.
        self.client.create_tables_from_dict({
            'empty_3':
                [SchemaField('col1', 'INTEGER'),
                 SchemaField('col2', 'STRING')],
            },
                replace_existing_tables=False)
        self.assertEqual([('col1', 'INTEGER', 'NULLABLE'),
                          ('col2', 'STRING', 'NULLABLE')],
                         [(x.name, x.field_type, x.mode)
                          for x in self.client.get_schema(self.default_test_dataset_id, 'empty_3')])

    @data((True,), (False,))
    @unpack
    def test_populate_both_insert_methods(self, make_immediately_available):
        table_name = self.src_table_name + '_for_append'
        self.client.populate_table(table_name,
                                   [SchemaField('foo', 'INTEGER'),
                                    SchemaField('bar', 'INTEGER'),
                                    SchemaField('baz', 'INTEGER')],
                                   [[1, 2, 3], [4, 5, 6]],
                                   make_immediately_available=make_immediately_available,
                                   replace_existing_table=True)

        # Only assert against the results if we've set make_immediately_available (because the
        # results might not be immediately available). At least this runs through the code to give
        # a sanity check for any runtime errors.
        if make_immediately_available:
            self.assertSetEqual(set([(1, 2, 3), (4, 5, 6)]),
                                set(self.client.get_query_results(SELECT_ALL_FORMAT
                                                                  .format(table_name))))

    def test_populate_table_with_nulls(self):
        # type: () -> None
        dest_table = self.client.path('pop_table_nulls', delimiter=BQ_PATH_DELIMITER)
        self.client.populate_table(dest_table, [SchemaField('col1', 'INTEGER'),
                                                SchemaField('col2', 'STRING')],
                                   [(1, None), (2, 'c')],
                                   replace_existing_table=True,
                                   make_immediately_available=True)
        result = self.client.get_query_results(
                'SELECT * FROM `{}` WHERE col2 IS NULL'.format(dest_table))

        self.assertSetEqual(set(result), set([(1, None)]))

    def test_populate_table_with_64_types(self):
        # type: () -> None
        dest_table = self.client.path('pop_table_64types', delimiter=BQ_PATH_DELIMITER)
        self.client.populate_table(dest_table,
                                   [SchemaField('col1', 'INT64'),
                                    SchemaField('col2', 'FLOAT64')],
                                   [(1, 2.5), (20, 6.5)],
                                   make_immediately_available=True)
        result = self.client.get_query_results(SELECT_ALL_FORMAT.format(dest_table))
        self.assertSetEqual(set(result), set([(1, 2.5), (20, 6.5)]))

    def test_append_rows(self):
        table_name = self.src_table_name + '_for_append'
        table_path = self.client.path(table_name, delimiter=BQ_PATH_DELIMITER)

        self.client.populate_table(table_name,
                                   FOO_BAR_BAZ_INTEGERS_SCHEMA,
                                   [[1, 2, 3], [4, 5, 6]],
                                   make_immediately_available=True)

        self.client.append_rows(table_path, [[7, 8, 9]])

        # TODO(Issue 9): Implement a make_immediately_available flag for append_rows so that we
        # don't have to do this in tests.
        # There is a "few seconds" (according to the documentation) delay between when the streaming
        # insert completes and when it is available for querying.
        time.sleep(10)

        self.assertSetEqual(set([(1, 2, 3), (4, 5, 6), (7, 8, 9)]),
                            set(self.client.get_query_results(SELECT_ALL_FORMAT
                                                              .format(table_path))))

    def test_append_rows_bad_schema_raises(self):
        table_name = self.src_table_name + '_for_append'
        self.client.populate_table(table_name,
                                   FOO_BAR_BAZ_INTEGERS_SCHEMA,
                                   [[1, 2, 3], [4, 5, 6]],
                                   replace_existing_table=True)

        with self.assertRaises(RuntimeError):
            self.client.append_rows(table_name,
                                    [[7, 8, 9]],
                                    [SchemaField('foo', 'INTEGER'),
                                     SchemaField('bar', 'INTEGER')])

    def test_copy_table(self):
        source_table_path = self.src_table_name
        dest_table_path = self.client.path('copied_table', self.default_test_dataset_id,
                                           self.client.project_id,
                                           delimiter=BQ_PATH_DELIMITER)

        self.client.copy_table(source_table_path,
                               'copied_table',
                               destination_dataset=self.default_test_dataset_id,
                               destination_project=self.client.project_id,
                               replace_existing_table=True)

        original = self.client.get_query_results(SELECT_ALL_FORMAT.format(source_table_path))
        copied = self.client.get_query_results(SELECT_ALL_FORMAT.format(dest_table_path))

        self.assertSetEqual(set(original), set(copied))

    def test_copy_table_table_exists_raises(self):
        source_table_path = self.src_table_name
        source_project, source_dataset, source_table = self.client.parse_table_path(
                source_table_path, delimiter=BQ_PATH_DELIMITER)

        with self.assertRaises(RuntimeError):
            self.client.copy_table(source_table_path,
                                   source_table,
                                   destination_project=source_project,
                                   destination_dataset=source_dataset)

    def test_copy_table_project_does_not_exist_raises(self):
        with self.assertRaises(RuntimeError):
            self.client.copy_table(self.src_table_name,
                                   'copy',
                                   destination_project='doesnotexist',
                                   replace_existing_table=True)

    def test_copy_table_dataset_does_not_exist_raises(self):
        with self.assertRaises(RuntimeError):
            self.client.copy_table(self.src_table_name,
                                   'copy',
                                   destination_dataset='doesnotexist',
                                   replace_existing_table=True)

    def test_path(self):
        # type: () -> None
        """Test bq.Client.parse_table_path"""
        delim = self.client.get_delimiter()

        table_name = 'my_table'
        table_path = ('{}.{}.{}'.format(self.TEST_PROJECT, self.default_test_dataset_id, table_name)
                      .replace('.', delim))
        mock_path = ('{}.{}.{}'.format('my_project', 'my_dataset', table_name)
                     .replace('.', delim))

        self.assertEqual(table_path, self.client.path('my_table', delimiter=delim))
        self.assertEqual(table_path,
                         self.client.path(self.default_test_dataset_id + delim + table_name,
                                          delimiter=delim))
        self.assertEqual(table_path, self.client.path(table_path, delimiter=delim))
        self.assertEqual(mock_path, self.client.path(table_name, dataset_id='my_dataset',
                                                     project_id='my_project',
                                                     delimiter=delim))

        self.assertEqual((self.TEST_PROJECT, self.default_test_dataset_id, table_name),
                         self.client.parse_table_path(table_path, delimiter=delim))
        self.assertEqual((self.TEST_PROJECT, self.default_test_dataset_id, table_name),
                         self.client.parse_table_path(
                             self.default_test_dataset_id + delim + table_name,
                             delimiter=delim))
        self.assertEqual((self.TEST_PROJECT, self.default_test_dataset_id, table_name),
                         self.client.parse_table_path(table_name,
                                                      delimiter=delim))

    def test_delete_dataset_with_tables_raises(self):
        # type: () -> None
        """Test that deleting a dataset with existing tables will raise an exception."""
        dest_table = self.client.path('tmp2', delimiter=BQ_PATH_DELIMITER)
        self.client.create_table_from_query(SELECT_ALL_FORMAT.format(self.src_table_name),
                                            dest_table)

        with self.assertRaises(Exception):
            self.client.delete_dataset_by_name(self.default_test_dataset_id)

    def test_force_delete_dataset_with_tables(self):
        # type: () -> None
        """Test that we can use DeleteDataset to delete all the tables and the dataset. """
        temp_default_test_dataset_id = self.default_test_dataset_id + 'dataset_with_tables'
        self.client.create_dataset_by_name(temp_default_test_dataset_id)
        dest_table = self.client.path('to_be_deleted', delimiter=BQ_PATH_DELIMITER)
        self.client.create_table_from_query(SELECT_ALL_FORMAT.format(self.src_table_name),
                                            dest_table)

        self.client.delete_dataset_by_name(temp_default_test_dataset_id, True)
        self.assertTrue(temp_default_test_dataset_id not in self.client.get_datasets())
