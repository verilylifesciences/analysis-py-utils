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
"""Base class for mocking interaction with BigQuery."""

# Workaround for https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2366
from __future__ import absolute_import

import re

from google.cloud.bigquery.schema import SchemaField
from verily.bigquery_wrapper.bq_base import (BQ_PATH_DELIMITER,
                                             BigqueryBaseClient)


# Pylint thinks this is an abstract class because it throws NotImplementedErrors.
# The warning is disabled because in this case those errors indicate that those methods
# are just difficult or impossible to mock.
# pylint: disable=R0921
class MockBaseClient(BigqueryBaseClient):
    """Base class for mock BQ clients.

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
        super(MockBaseClient, self).__init__(project_id, default_dataset, maximum_billing_tier)

        # We use dictionaries to simulate the project/dataset/table structure present in BQ.
        # Mostly these will serve as checks in tests to make sure that a project and dataset
        # are created before a table is inserted into them.
        self.project_map = dict()
        self.project_map[self.project_id] = []

        self.table_map = dict()
        self.table_map[default_dataset] = []

        self.print_before_and_after = print_before_and_after

    def _add_table(self, standardized_path):
        """Adds a table to the table map.

        Args:
            standardized_path: The table path with real delimiters already replaced with
                the mock delimiter (since this is a private function).
        Raises:
            RuntimeError: if it looks like the project or dataset haven't been created yet.
        """
        # Get the components of the table path.
        project, dataset, table_name = self.parse_table_path(standardized_path)

        # Make sure the project and dataset have already been explicitly created before trying to
        # add the table.
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

    @staticmethod
    def _remove_query_prefix(query):
        """Remove the BigQuery SQL variant prefix.
        https://cloud.google.com/bigquery/docs/reference/standard-sql/enabling-standard-sql#sql-prefix
        """
        query = query.replace('#standardSQL', '')
        query = query.replace('#legacySQL', '')
        return query

    @staticmethod
    def _transform_farmfingerprint(query):
        """For testing, we don't need to subsample, so we replace the fingerprint with 0"""
        # Note: This regex also includes concat and cast because without it, it includes too many
        # ')' see PM-??? for details.
        return re.sub(r'FARM_FINGERPRINT\(CONCAT\(CAST\((?P<arg1>.*).*CAST\((?P<arg2>.*)\)\)\)',
                      '0', query)  # noqa

    @staticmethod
    def _transform_format(query):
        """Replace the BQ function FORMAT with the equivalent SQLite function printf"""
        return query.replace('FORMAT(', 'printf(')

    def create_dataset_by_name(self, name, expiration_hours=None):
        # type: (str, Optional[float]) -> None
        """Create a new dataset within the current project.

        Args:
            name: The name of the new dataset.
            expiration_hours: Unused in this implementation.
        """
        self.project_map[self.project_id].append(name)
        self.table_map[name] = []

    def delete_dataset_by_name(self, name, delete_all_tables=False):
        # type: (str, Optional[bool]) -> None
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
            for table_name in list(self.tables(name)):
                self.delete_table_by_name(self.path(table_name, dataset_id=name,
                                                    delimiter=BQ_PATH_DELIMITER))

        if len(self.tables(name)) > 0:
            raise RuntimeError('The dataset {} still contains tables: {}'
                               .format(name, str(self.tables(name))))

        del self.table_map[name]
        self.project_map[self.project_id].remove(name)

    def remove_table(self, table_path):
        """Removes a table from the table map.

        Args:
            table_path: A string of the form '<dataset id>.<table name>' or
                '<project id>.<dataset_id>.<table_name>'
        """
        _, dataset, table = self.parse_table_path(table_path, delimiter=BQ_PATH_DELIMITER)
        self.table_map[dataset].remove(table)

    def tables(self, dataset_id):
        # type: (str) -> List[str]
        """Returns a list of table names in a given dataset.

        Args:
            dataset_id: The dataset to query.

        Returns:
            A list of table names (strings).
        """
        return self.table_map[dataset_id]

    def get_datasets(self, project_id=None):
        # type: (None) -> List[str]
        """Returns a list of dataset ids in the current project.

        Args:
            project_id: The project ID in which to get datasets. Defaults to the class's project_id.

        Returns:
          A list of project ids (strings).
        """
        return self.project_map[project_id or self.project_id]

    def get_projects(self):
        # type: (None) -> List[str]
        """Returns a list of projects.

        Returns:
          A list of dataset ids/names (strings).
        """
        return self.project_map.keys()

    def dataset_exists(self,
                       dataset  # type: Dataset, DatasetReference
                       ):
        # type: (...) -> bool
        """Checks if a dataset exists.

        Args:
            dataset: The BQ dataset object to check.
        """
        raise NotImplementedError('dataset_exists is not implemented in mock_bq since'
                                  'mock_bq does not use BigQuery objects.')

    def table_exists(self,
                     table  # type: TableReference, Table
                     ):
        # type: (...) -> bool
        """Checks if a table exists.

        Args:
            table: The TableReference or Table for the table to check whether it exists.
        """
        raise NotImplementedError('table_exists is not implemented in mock_bq since'
                                  'mock_bq does not use BigQuery objects.')

    def delete_dataset(self, dataset):
        # type: (...) -> None
        """Deletes a dataset.

        Args:
            dataset: The Dataset or DatasetReference to delete.
        """
        raise NotImplementedError('delete_dataset is not implemented in mock_bq since'
                                  'mock_bq does not use BigQuery objects.')

    def delete_table(self,
                     table  # type: Table, TableReference
                     ):
        # type: (...) -> None
        """Deletes a table.

        Args:
            table: The Table or TableReference to delete.
        """
        raise NotImplementedError('delete_table is not implemented in mock_bq since'
                                  'mock_bq does not use BigQuery objects.')

    def create_dataset(self,
                       dataset  # type: DatasetReference, Dataset
                       ):
        # type: (...) -> None
        """
        Creates a dataset.

        Args:
            dataset: The Dataset object to create.
        """
        raise NotImplementedError('create_dataset is not implemented in mock_bq since'
                                  'mock_bq does not use BigQuery objects.')

    def create_table(self, table):
        # type: (Table) -> None
        """
        Creates a table.

        Args:
            table: The Table or TableReference object to create. Note that if you pass a
                TableReference the table will be created with no schema.
        """
        raise NotImplementedError('create_table is not implemented in mock_bq since'
                                  'mock_bq does not use BigQuery objects.')

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
        raise NotImplementedError('fetch_data_from_table is not implemented in mock_bq since'
                                  'mock_bq does not use BigQuery objects.')
