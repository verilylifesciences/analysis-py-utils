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
"""Unit tests for the bq library."""

# Workaround for https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2366
from __future__ import absolute_import

import csv
import random
import uuid
from datetime import datetime
import six
from six.moves import cStringIO
import google
from ddt import data, ddt, unpack
from google.api_core import exceptions, retry
from google.cloud import storage
from google.cloud.bigquery import ExtractJob
from google.cloud.bigquery.job import QueryJobConfig
from google.cloud.bigquery.schema import SchemaField
from mock import MagicMock, PropertyMock, patch

from verily.bigquery_wrapper import bq, bq_shared_tests, bq_test_case
from verily.bigquery_wrapper.bq_base import (DEFAULT_RETRY_FOR_API_CALLS, is_job_done,
                                             validate_query_job)

# Arguments to pass to retry-related tests
EXCEPTION_RETRY_TEST_ARGS = (
    dict(
        exc=exceptions.BadRequest('Extra comma before FROM clause.'),
        should_retry=False
    ),
    dict(
        exc=exceptions.BadRequest(
            'The job encountered an internal error during execution. (Transient error.)'),
        should_retry=True
    ),
    dict(
        exc=exceptions.InternalServerError('Transient error.'),
        should_retry=True
    ),
    dict(
        exc=exceptions.TooManyRequests('Transient error.'),
        should_retry=True
    ),
    dict(
        exc=exceptions.BadGateway('Transient error.'),
        should_retry=True
    ),
)


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
        # Because we're testing the actual Bigquery functionality we don't want the mocks.
        super(BQTest, cls).setUpClass(use_mocks=False)
        cls.create_temp_bucket()

    @classmethod
    def tearDownClass(cls):
        # type: () -> None
        cls.bucket.delete()
        super(BQTest, cls).tearDownClass()

    def setUp(self):
        # type: () -> None
        self.test_id = str(uuid.uuid4().hex)

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
                                   [[1, [2, 3]], [4, [5, 6]]], make_immediately_available=False)

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
    @data(('csv', True, '', '', None, 'tmp-000000000000.csv.gz', True, 'csv w/ gzip'),
          ('json', True, 'test', '', None, 'test/tmp-000000000000.json.gz', True, 'json w/ gzip'),
          ('avro', False, '/test', '', None, 'test/tmp.avro', False, 'Avro w/o gzip'),
          ('csv', True, '', 'ext', None, 'tmp_ext.csv.gz', False, 'csv w/ gzip & ext'),
          ('csv', True, '', '', 'overwritten_name', 'overwritten_name.csv.gz', False, 'overwriting filename'))  # noqa
    @unpack
    def test_export_table(self,
                          out_fmt,  # type: str
                          compression,  # type: bool
                          dir_in_bucket,  # type: str
                          output_ext,  # type: str
                          explicit_filename,  # type: str
                          expected_output_path,  # type: str
                          support_multifile_export,  # type: bool
                          test_description  # type: str
                          ):
        # type: (...) -> None
        """Test ExportTableToBucket
        Args:
            out_fmt: Output format. Must be one of {'csv', 'json', 'avro'}
            compression: Whether to compress file using GZIP. Cannot be applied to avro
            dir_in_bucket: The directory in the bucket to store the output files
            output_ext: Extension of the output file name
            explicit_filename: Explicitly specified filename.
            expected_output_path: Expected output path
            test_description: A description of the test
        """

        fnames = self.client.export_table_to_bucket(
            self.src_table_name, self.temp_bucket_name, dir_in_bucket, out_fmt, compression,
            output_ext, support_multifile_export=support_multifile_export,
            explicit_filename=explicit_filename)

        # Test that the output file name is returned (everything after the last "/" of the path).
        self.assertEqual(fnames, [expected_output_path.rsplit('/', 1)[-1]])

        # Test that the object is in the bucket.
        self.assertTrue(
                isinstance(self.bucket.get_blob(expected_output_path), storage.Blob),
                           test_description +
                           ': File {} is not in {}'.format(expected_output_path,
                                                           str([x for x in self.bucket.list_blobs()])))

    def test_export_table_multifile(self):
        """Test correct filenames returned for export with multiple files created."""
        with patch.object(ExtractJob, 'destination_uri_file_counts',
                          new_callable=PropertyMock) as file_counts_mock:
            file_counts_mock.return_value = [2]
            fnames = self.client.export_table_to_bucket(
                self.src_table_name, self.temp_bucket_name, support_multifile_export=True)
            self.assertSetEqual(set(fnames), {'tmp-000000000000.csv', 'tmp-000000000001.csv'})

    @data(('csv', True, '', '', 'tmp-000000000000.csv.gz', 'csv w/ gzip', True),
          ('json', True, 'test', '', 'test/tmp-000000000000.json.gz', 'json w/ gzip', False),
          ('avro', False, 'test', '', 'test/tmp-000000000000.avro', 'Avro w/o gzip', False),
          ('csv', True, '', 'ext', 'tmp_ext-000000000000.csv.gz', 'csv w/ gzip & ext', True))
    @unpack
    def test_import_table_from_bucket(self,
                                      input_fmt,  # type: str
                                      compression,  # type: bool
                                      dir_in_bucket,  # type: str
                                      output_ext,  # type: str
                                      input_path,  # type: str
                                      test_description,  # type: str
                                      skip_leading_row,  # type: bool
                                    ):
        # type: (...) -> None
        """Test ImportTableFromBucket
        Note this test is dependent on export_table_to_bucket working correctly.

        Args:
            input_fmt: Input format. Must be one of {'csv', 'json', 'avro'}
            compression: Whether to compress file using GZIP. Cannot be applied to avro
            dir_in_bucket: The directory in the bucket to store the output files
            output_ext: Extension of the output file name
            expected_output_path: Expected output path
            test_description: A description of the test
            skip_leading_row: Whether to skip the leading row in the data file
        """
        self.client.export_table_to_bucket(self.src_table_name, self.temp_bucket_name,
                                           dir_in_bucket, input_fmt, compression, output_ext)

        input_path = 'gs://{}/{}'.format(self.temp_bucket_name, input_path)

        dest_path = self.client.path(self.test_id)

        self.client.import_table_from_bucket(dest_path,
                                             input_path,
                                             input_format=input_fmt,
                                             schema=bq_shared_tests.FOO_BAR_BAZ_INTEGERS_SCHEMA,
                                             skip_leading_row=skip_leading_row)

        results = self.client.get_query_results('SELECT * FROM `{}`'.format(dest_path))
        six.assertCountEqual(self, [(1, 2, 3), (4, 5, 6)], results)

    def test_import_table_from_file(self):
        data = [(8, 9, 10), (11, 12, 13)]
        output = cStringIO()

        csv_out = csv.writer(output)
        for row in data:
            csv_out.writerow(row)
        output.seek(0)

        dest_path = self.client.path(self.test_id)

        self.client.import_table_from_file(dest_path,
                                           output,
                                           input_format='csv',
                                           schema=bq_shared_tests.FOO_BAR_BAZ_INTEGERS_SCHEMA)

        results = self.client.get_query_results('SELECT * FROM `{}`'.format(dest_path))
        six.assertCountEqual(self, data, results)

    # TODO(Issue 7): Add test to export schemas from a project different from self.client.project_id
    @data(('', '', None, 'tmp-schema.json', 'Export schema to root'),
          ('test', '', None, 'test/tmp-schema.json', 'Export schema to /test'),
          ('', 'ext', None, 'tmp_ext-schema.json', 'Export schema to root with extension'),
          ('', '', 'overwritten_name', 'overwritten_name-schema.json', 'overwrite filename'))
    @unpack
    def test_export_schema(self,
                           dir_in_bucket,  # type: str
                           output_ext,  # type: str
                           explicit_filename,  # type: str
                           expected_schema_path,  # type: str
                           test_description  # type:str
                           ):
        # type: (str, str, str, str) -> None
        """Test ExportSchemaToBucket
         Args:
            dir_in_bucket: The directory in the bucket to store the output files
            output_ext: Extension of the output file name
            explicit_filename: Explicitly specified filename.
            expected_output_path: Expected output path
            test_description: A description of the test
        """

        fname = self.client.export_schema_to_bucket(self.src_table_name, self.temp_bucket_name,
                                                    dir_in_bucket, output_ext,
                                                    explicit_filename=explicit_filename)

        # Test that the output file name is returned (everything after the last "/" of the path).
        self.assertEqual(fname, expected_schema_path.rsplit('/', 1)[-1])

        # Test that the object is in the bucket.
        self.assertTrue(
                isinstance(self.bucket.get_blob(expected_schema_path), storage.Blob),
                test_description)

    def test_invalid_query_prints_query(self):
        """Test get_query_results prints the query if given an invalid query"""
        query = 'this is an invalid query!'
        with self.assertRaises(RuntimeError) as e:
            self.client.get_query_results(query)
        self.assertTrue(query in str(e.exception))

    def _get_mock_api_request_side_effect(self, exceptions_to_raise):
        """Get the side effect function for mock API requests to the BQ backend.

        In the tests, we mock out google.cloud._http.JSONConnection.api_request to raise exceptions
        as specified so that we are able to test the retry behavior of our code.

        Args:
            exceptions_to_raise: The list of exceptions to raise. Mock BQ backend API requests' side
                effect function iterates over the list. When the side effect function is called, it
                checks the next element in the list. If it's None or past the end of the
                exception sequence, no error is raised and return a normal api_request. If it's an
                exception, raise the exception.

        Returns:
            The side effect function for mock API requests to the BQ backend
        """
        # Make a copy of the api_request function that is not mocked out.
        copy_of_api_request = google.cloud._http.JSONConnection.api_request
        exception_iter = iter(exceptions_to_raise)
        def api_request_side_effect(*args, **kwargs):
            try:
                exception = next(exception_iter)
                if exception is not None:
                    raise exception
            except StopIteration:
                pass

            return copy_of_api_request(self.client.gclient._connection, *args, **kwargs)

        return api_request_side_effect

    @data(*EXCEPTION_RETRY_TEST_ARGS)
    @unpack
    def test_get_query_results_retries(self, exc, should_retry):
        # type: (Exception, bool) -> None
        """Tests that get_query_results retries on transient unstructured errors.

        Mocks out the api_request function, which actually makes the backend call.

        Args:
            exc: The exception to raise on the first two calls to api_request. If get_query_results
                retries the api call on both exceptions, the real api_request function will be
                called all following times.
            should_retry: Whether get_query_results is expected to retry on the given exception.
        """
        exceptions_to_raise = [exc, exc]
        side_effect_func = self._get_mock_api_request_side_effect(exceptions_to_raise)

        with patch('google.cloud._http.JSONConnection.api_request') as mock_api_request:
            mock_api_request.side_effect = side_effect_func

            if should_retry:
                self.assertEqual(self.client.get_query_results('SELECT 5'), [(5,)])
            else:
                with self.assertRaises(type(exc)):
                    self.client.get_query_results('SELECT 5')

    @patch('google.cloud._http.JSONConnection.api_request')
    def test_get_query_results_raises_error_if_deadline_exceeded(self, mock_api_request):
        # type: (MagicMock) -> None
        """Tests that get_query_results raises a RetryError if the deadline is exceeded.

        Args:
            mock_api_request: The MagicMock object for the api_request method.
        """
        # Raise a transient error on every API request so that the job doesn't finish.
        mock_api_request.side_effect = exceptions.InternalServerError('Transient error.')

        client_in_a_hurry = bq.Client(self.TEST_PROJECT, max_wait_secs=1)
        before_get_query_results = datetime.now()
        with self.assertRaises(exceptions.RetryError):
            client_in_a_hurry.get_query_results('SELECT 5')

        # Test that the process timed out before the default timeout. (It should time out after 1
        # second, but this leaves buffer for BQ being slow.)
        # Use the underlying Retry library's default timeout, because it's shorter than our
        # DEFAULT_TIMEOUT_SEC, and we want to fail if our timeout didn't get passed in somehow.
        diff = datetime.now() - before_get_query_results
        self.assertTrue(diff.seconds < retry._DEFAULT_DEADLINE)

    def _test_bq_api_call_retries(self, method_to_test, exc, should_retry):
        """A helper function to test retries when calling backend API.

        Args:
            method_to_test: The method to test. It is a method called after an AsyncJob is created
                and takes an AsyncJob and a string of query as arguments.
            exc: The exception to raise.
            should_retry: Whether bq.Client should catch the exception and do a retry.
        """
        exceptions_to_raise = [
            None,  # No error to raise when called in self.client.query
            exc   # Method validate_query_job calls QueryJob.done, which raises an error.
                  # If exc is transient, trigger a retry; otherwise exit.
        ]
        side_effect_func = self._get_mock_api_request_side_effect(exceptions_to_raise)

        with patch('google.cloud._http.JSONConnection.api_request') as mock_api_request:
            mock_api_request.side_effect = side_effect_func

            config = QueryJobConfig()
            config.allow_large_results = True

            config.destination = self.client.get_table_reference_from_path(
                self.client.path('{}_test_{}'.format(method_to_test.__name__, self.test_id)))

            query_to_run = 'SELECT 5'
            query_job = self.client.run_async_query(query_to_run, job_config=config)

            if should_retry:
                method_to_test(query_job, query_to_run)
            else:
                with self.assertRaises(type(exc)):
                    method_to_test(query_job, query_to_run)

    @data(*EXCEPTION_RETRY_TEST_ARGS)
    @unpack
    def test_is_job_done_retries(self, exc, should_retry):
        """Tests retries in is_job_done

        Args:
            exc: The exception to raise.
            should_retry: Whether bq.Client should catch the exception and do a retry.
        """
        self._test_bq_api_call_retries(is_job_done, exc, should_retry)

    @data(*EXCEPTION_RETRY_TEST_ARGS)
    @unpack
    def test_validate_query_job_retries(self, exc, should_retry):
        """Tests retries in validate_query_job

        Args:
            exc: The exception to raise.
            should_retry: Whether bq.Client should catch the exception and do a retry.
        """
        self._test_bq_api_call_retries(validate_query_job, exc, should_retry)

    @data(
        dict(
            max_wait_secs=12,
            expected_timeout=12
        ),
        dict(
            max_wait_secs=None,
            expected_timeout=DEFAULT_RETRY_FOR_API_CALLS._deadline
        )
    )
    @unpack
    def test_is_job_done_timeouts(self, max_wait_secs, expected_timeout):
        """Tests that timeouts are plumbed through to the done method in is_job_done."""
        mock_job = MagicMock()
        mock_job.done.return_value = False

        is_job_done(mock_job, max_wait_secs=max_wait_secs)

        done_call_args, done_call_kwargs = mock_job.done.call_args
        retry_object_used = done_call_kwargs['retry']
        self.assertEqual(retry_object_used._deadline, expected_timeout)

    @data(
        dict(
            table_content=[],
            expected=0
        ),
        dict(
            table_content=[[1, 2, 3], [4, 5, 6]],
            expected=2
        )
    )
    @unpack
    def test_get_table_num_rows(self, table_content, expected):
        # type: (List[List[int]], int) -> None
        """Tests get_table_num_rows

        Args:
            table_content: Content of the table to test. It is expected to have three integer
                columns, so that we can reuse bq_shared_tests.FOO_BAR_BAZ_INTEGERS_SCHEMA.
            expected: Expected number of rows in the test table
        """

        test_table_path = self.client.path('tmp_' + self.test_id)

        self.client.populate_table(
            test_table_path,
            bq_shared_tests.FOO_BAR_BAZ_INTEGERS_SCHEMA,
            table_content)

        self.assertEqual(self.client.get_table_num_rows(test_table_path), expected)

    @data(
        dict(
            table_content=[],
            expected=0
        ),
        dict(
            table_content=[[1, 2, 3], [4, 5, 6]],
            expected=48
        )
    )
    @unpack
    def test_get_table_num_bytes(self, table_content, expected):
        # type: (List[List[int]], int) -> None
        """Tests get_table_num_bytes

        Args:
            table_content: Content of the table to test. It is expected to have three integer
                columns, so that we can reuse bq_shared_tests.FOO_BAR_BAZ_INTEGERS_SCHEMA.
            expected: Expected number of bytes in the test table
        """

        test_table_path = self.client.path('tmp_' + self.test_id)

        self.client.populate_table(
            test_table_path,
            bq_shared_tests.FOO_BAR_BAZ_INTEGERS_SCHEMA,
            table_content)

        self.assertEqual(self.client.get_table_num_bytes(test_table_path), expected)

    @data((True,), (False,))
    @unpack
    def test_copy_dataset(self, create_destination_dataset):
        # type: (bool) -> None
        """Tests copy_dataset.

        This does not test the functionality of copying a dataset from one project to another, since
        we only have access to one project (self.TEST_PROJECT). It just tests that the dataset is
        copied to the desired destination in the same project.

        Args:
            create_destination_dataset: Whether to create the destination dataset (empty of tables)
                before calling copy_dataset. It should work either way.
        """
        source_dataset_name = 'source_dataset_{}'.format(self.test_id)
        destination_dataset_name = 'destination_dataset_{}'.format(self.test_id)
        self.client.create_dataset_by_name(source_dataset_name)
        self.addCleanup(lambda: self.client.delete_dataset_by_name(source_dataset_name,
                                                                   delete_all_tables=True))
        if create_destination_dataset:
            self.client.create_dataset_by_name(destination_dataset_name)
        self.addCleanup(lambda: self.client.delete_dataset_by_name(destination_dataset_name,
                                                                   delete_all_tables=True))

        table_names = ['table_1', 'table_2']
        for table_name in table_names:
            table_path = self.client.path(table_name, dataset_id=source_dataset_name)
            # Create an empty table.
            self.client.populate_table(
                table_path,
                bq_shared_tests.FOO_BAR_BAZ_INTEGERS_SCHEMA)

        bq.copy_dataset(self.TEST_PROJECT, source_dataset_name, self.TEST_PROJECT,
                     destination_dataset_name)

        self.assertEqual(self.client.tables(destination_dataset_name), table_names)

    def test_copy_dataset_fails_if_tables_in_destination_dataset(self):
        # type: (bool) -> None
        """Tests that copy_dataset fails if there are already tables in the destination dataset."""
        source_dataset_name = 'source_dataset_{}'.format(self.test_id)
        destination_dataset_name = 'destination_dataset_{}'.format(self.test_id)
        self.client.create_dataset_by_name(source_dataset_name)
        self.addCleanup(lambda: self.client.delete_dataset_by_name(source_dataset_name,
                                                                   delete_all_tables=True))
        self.client.create_dataset_by_name(destination_dataset_name)
        self.addCleanup(lambda: self.client.delete_dataset_by_name(destination_dataset_name,
                                                                   delete_all_tables=True))

        self.client.populate_table(
            self.client.path('table_to_copy', dataset_id=source_dataset_name),
            bq_shared_tests.FOO_BAR_BAZ_INTEGERS_SCHEMA)
        self.client.populate_table(
            self.client.path('existing_table', dataset_id=destination_dataset_name),
            bq_shared_tests.FOO_BAR_BAZ_INTEGERS_SCHEMA)

        with self.assertRaises(RuntimeError):
            bq.copy_dataset(self.TEST_PROJECT, source_dataset_name, self.TEST_PROJECT,
                         destination_dataset_name)

    # TODO(Issue 23): Fill out remaining tests for retry logic.


if __name__ == '__main__':
    bq_test_case.main()
