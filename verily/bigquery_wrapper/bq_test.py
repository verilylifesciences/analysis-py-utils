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

import random

from ddt import data, ddt, unpack
from google.cloud import storage
from google.cloud.bigquery.schema import SchemaField

from verily.bigquery_wrapper import bq_shared_tests, bq_test_case


@ddt
class BQTest(bq_shared_tests.BQSharedTests):
    @classmethod
    def create_mock_tables(cls):
        # type: () -> None
        """Create mock tables"""
        super(BQTest, cls).create_mock_tables()

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

    def test_add_rows_repeated(self):
        table_name = self.src_table_name + '_for_append_repeated'
        self.client.populate_table(table_name,
                                   [SchemaField('foo', 'INTEGER'),
                                    SchemaField('bar', 'INTEGER', mode='REPEATED')],
                                   [[1, [2, 3]], [4, [5, 6]]])

        self.client.append_rows(table_name, [[7, [8, 9]]])

        self.assertEqual([(1, [2, 3]), (4, [5, 6]), (7, [8, 9])].sort(),
                         self.client.get_query_results('SELECT * FROM `{}`'.format(table_name))
                         .sort())

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

    # TODO (Issue 8): Add test to export tables from a project different from self.client.project_id
    @data(('csv', True, '', '', 'tmp.csv.gz', 'csv w/ gzip'),
          ('json', True, 'test', '', 'test/tmp.json.gz', 'json w/ gzip'),
          ('avro', False, '/test', '', 'test/tmp.avro', 'Avro w/o gzip'),
          ('csv', True, '', 'ext', 'tmp_ext.csv.gz', 'csv w/ gzip & ext'))
    @unpack
    def test_export_table(self,
                          out_fmt,  # type: str
                          compression,  # type: bool
                          dir_in_bucket,  # type: str
                          output_ext,  # type: str
                          expected_output_path,  # type: str
                          test_description  # type: str
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
                isinstance(self.bucket.get_blob(expected_output_path), storage.Blob),
                test_description)

    # TODO(Issue 7): Add test to export schemas from a project different from self.client.project_id
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
                isinstance(self.bucket.get_blob(expected_schema_path), storage.Blob),
                test_description)


if __name__ == '__main__':
    bq_test_case.main()
