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
"""Mock library for interacting with BigQuery, backed by SQLite.

Sample usage:

    client = mock_bq.Client(project_id)
    result = client.Query(query)
"""

# Workaround for https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2366
from __future__ import absolute_import

import datetime
import logging
import platform
import re
import six

from google.cloud.bigquery.schema import SchemaField
if platform.sys.version_info.major == 2:
    from pysqlite2 import dbapi2 as sqlite3
else:
    import sqlite3
from verily.bigquery_wrapper.bq_base import BQ_PATH_DELIMITER
from verily.bigquery_wrapper.db_api_bq import Client as DbApiClient

# SQLite only uses . to separate database and table, so we need a
# different delimiter. It's pretty strict about what special characters it
# will accept as a part of a table name so I'm using Z since it's an uncommon
# character. The change should be invisible to users except in debugging test
# cases and tables with the delimiters replaced are never backparsed into Bigquery
# tables so there's no danger in using a table path with a Z in it.
MOCK_DELIMITER = 'Z'

TRUE_STR = 'True'
FALSE_STR = 'False'


class Client(DbApiClient):
    """Stores pointers to a mock BigQuery project, backed by an in-memory
    sqlite database instead.

    Args:
        project_id: The id of the project to associate with the client.
        default_dataset: The default dataset to use in function calls if none is explicitly provided.
        maximum_billing_tier: Unused in this implementation.
        print_before_and_after: If set to True, this will print out queries as passed in, then
            print out the query after it's been reformatted to match appropriate SQLite conventions.
            Helpful for debugging.
    """

    def __init__(self, project_id, default_dataset=None, maximum_billing_tier=None,
                 print_before_and_after=False):
        # SQLite does not allow dashes in table paths.
        formatted_project_id = project_id.replace('-', '_')
        # Connect to an in-memory database path.
        super(Client, self).__init__(sqlite3.connect(':memory:'), formatted_project_id,
                                     default_dataset, maximum_billing_tier)

        # We use dictionaries to simulate the project/dataset/table structure present in BQ.
        # Mostly these will serve as checks in tests to make sure that a project and dataset
        # are created before a table is inserted into them.
        self.project_map = {}
        self.project_map[self.project_id] = []

        self.table_map = {}
        self.create_dataset_by_name(default_dataset)

        self.print_before_and_after = print_before_and_after

    def get_delimiter(self):
        return MOCK_DELIMITER

    def _bq_type_to_db_type(self, typename):
        """ Maps BQ types to SQLite types. One limitation of this class is that there's not a 1:1
        mapping, but this will cover most use cases."""
        if typename in ['FLOAT', 'FLOAT64']:
            return 'REAL'
        elif typename in ['INTEGER', 'INT64']:
            return 'INTEGER'

        # Strings and timestamps will be treated as text.
        return 'TEXT'

    def _db_type_to_bq_type(self, typename, sample=None):
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

    def _create_table(self, standardized_path, schema_fields=None, schema_string=None):
        """Creates a table.

        Adds its path to the appropriate mappings.
        The DB API requires a schema string for table creation. If one isn't passed in, this
        function will use a dummy schema to create the table.

        Args:
            standardized_path: The table path with real delimiters already replaced with
                the mock delimiter (since this is a private function).
            schema_fields: A list of SchemaFields descrbing the table. If both schema_fields
                and schema_string are present, schema_fields will take precedence.
            schema_string: A string describing the schema.
        Raises:
            RuntimeError: if it looks like the project or dataset haven't been created yet.
        """
        # Get the components of the table path and then parse them back into a
        # SQLite3 friendly path.
        project, dataset, table_name = self.parse_table_path(standardized_path)

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

        super(Client, self)._create_table(standardized_path, schema_fields, schema_string)

    def _reformat_query(self, query):
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

        Raises:
            RuntimeError: If the transformed query wouldn't execute in SQLite. If this happens, the
                test author can either roll in whatever the missing transformation is into this
                function, or choose to test on real BigQuery.
        """
        if self.print_before_and_after:
            logging.info("ORIGINAL: " + query)

        # Remove backticks.
        query = query.replace('`', '')

        # For all known tables, replace the BigQuery formatted path with the SQLite3 formatted path
        for project, dataset_list in six.iteritems(self.project_map):
            for dataset in dataset_list:
                for table in self.table_map[dataset]:
                    qualified_table = self.path(table, dataset, project,
                                                delimiter=BQ_PATH_DELIMITER, replace_dashes=False)
                    sanitized_table = self.path(table, dataset, project,
                                                delimiter=MOCK_DELIMITER)
                    query = query.replace(qualified_table, sanitized_table)

        query = self._remove_query_prefix(query)
        query = self._transform_booleans(query)
        query = self._transform_farmfingerprint(query)
        query = self._transform_extract_year(query)
        query = self._transform_extract_month(query)
        query = self._transform_extract_day_of_week(query)
        query = self._transform_concat(query)
        query = self._transform_division(query)
        query = self._transform_format(query)
        query = self._transform_substr(query)
        query = self._transform_mod(query)
        query = self._transform_if(query)

        if self.print_before_and_after:
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
    def _transform_booleans(query):
        """Surround booleans in quotation marks, since booleans get converted to TEXT in sqlite."""
        query = query.replace('TRUE', '"{}"'.format(TRUE_STR))
        return query.replace('FALSE', '"{}"'.format(FALSE_STR))

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
    def _transform_extract_day_of_week(query):
        """Transform EXTRACT(DAYOFWEEK to strftime function to get the day of the week
        from a date or timestamp.

        Note: SQLite's EXTRACT(DAYOFWEEK) function returns a range from 0-6 (0 = Sunday), whereas
        BQ returns a range from 1-7 (1 = Sunday), so we need to add 1 to the result.
        """
        extract_regex = re.compile(r'EXTRACT\(DAYOFWEEK FROM (?P<colname>.+?)\)')
        match = re.search(extract_regex, query)
        while match:
            repl_string = "CAST(strftime('%w', " + match.group('colname') + ") as INTEGER) + 1"
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
        extract_regex = re.compile(r'FARM_FINGERPRINT\(CONCAT\(CAST\((?P<arg1>.*).*CAST\((?P<arg2>.*)\)\)\)')  # noqa
        match = re.search(extract_regex, query)
        while match:
            repl_string = '0'
            query = query[:match.start()] + repl_string + query[match.end():]
            match = re.search(extract_regex, query)
        return query

    @staticmethod
    def _transform_mod(query):
        """Transform MOD(arg1,arg2) to arg1 % arg2."""
        extract_regex = re.compile(r'MOD\((?P<arg1>.*),(?P<arg2>.*)\)')
        match = re.search(extract_regex, query)
        while match:
            repl_string = match.group('arg1') + ' % ' + match.group('arg2')
            query = query[:match.start()] + repl_string + query[match.end():]
            match = re.search(extract_regex, query)
        return query

    @staticmethod
    def _transform_if(query):
        """Transform IF(COND,arg1,arg2) to CASE WHEN COND THEN arg1 ELSE arg2 END."""
        extract_regex = re.compile(r'IF\((?P<cond>.*),(?P<arg1>.*),(?P<arg2>.*)\)')
        match = re.search(extract_regex, query)
        while match:
            repl_string = 'CASE WHEN {cond} THEN {arg1} ELSE {arg2} END'.format(
                cond=match.group('cond'), arg1=match.group('arg1'), arg2=match.group('arg2'))
            query = query[:match.start()] + repl_string + query[match.end():]
            match = re.search(extract_regex, query)
        return query

    def _reformat_results(self, result_row):
        """See parent class for docstring."""
        reformatted_row = []
        for item in result_row:
            if item == 'None':
                reformatted_row.append(None)
            else:
                reformatted_row.append(item)
        return reformatted_row

    def _cast_to_correct_internal_representation(self, query_results, expected_schema):
        """Turns types into correct internal representation, e.g., 0 -> FALSE for booleans.

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

    def create_tables_from_dict(self,
                                table_names_to_schemas,  # type: Dict[str, List[SchemaField]]
                                dataset_id=None,  # type: Optional[str]
                                replace_existing_tables=False  # type: Optional[bool]
                                ):
        # type: (...) -> None
        """Creates a set of tables from a dictionary of table names to their schemas.

        Args:
            table_names_to_schemas: A dictionary of:
                key: The table name.
                value: A list of SchemaField objects.
            dataset_id: The dataset in which to create tables. If not specified, use default
                dataset.
            replace_existing_tables: If True, delete and re-create tables. Otherwise, checks to see
                if any of the requested tables exist. If they do, it will raise a RuntimeError.

        Raises:
            RuntimeError if replace_existing_tables is False and any of the tables requested for
                creation already exist
        """

        # If the flag isn't set to replace existing tables, raise an error if any tables we're
        # trying to create already exist.
        if not replace_existing_tables:
            self._raise_if_tables_exist(table_names_to_schemas.keys(), dataset_id)

        for table_name, schema in six.iteritems(table_names_to_schemas):
            table_path = self.path(table_name, dataset_id=dataset_id, project_id=self.project_id,
                                   delimiter=MOCK_DELIMITER)

            if (table_name in self.tables(dataset_id or self.default_dataset_id)
                    and replace_existing_tables):
                    self.delete_table_by_name(self.path(table_name, delimiter=BQ_PATH_DELIMITER))
            self._create_table(table_path, schema_fields=schema)

    def create_dataset_by_name(self, name, expiration_hours=None):
        # type: (str, Optional[float]) -> None
        """Create a new dataset within the current project.

        Args:
          name: The name of the new dataset.
          expiration_hours: Unused in this implementation.
        """
        if name in self.table_map:
            logging.warning('Dataset {} already exists.'.format(name))
            return
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

    def delete_table_by_name(self, table_path):
        """Delete a table within the current project.

        Args:
            table_path: A string of the form '<dataset id>.<table name>' or
                '<project id>.<dataset_id>.<table_name>'
        """
        super(Client, self).delete_table_by_name(table_path)
        _, dataset, table = self.parse_table_path(table_path, delimiter=BQ_PATH_DELIMITER)
        self.table_map[dataset].remove(table)

    def get_schema(self, dataset_id, table_name, project_id=None):
        # type: (str, str, Optional[str]) -> List[SchemaField]
        """Returns the schema of a table. Note that due to the imperfect mapping
        of SQLiteTypes to BQ types, these schemas won't be perfect. Anything relying heavily
        on correct schemas should use the real BigQuery client.

        Args:
            dataset_id: The dataset to query.
            table_name: The name of the table.
            project_id: The project ID of the table.
        Returns:
            A list of SchemaFields representing the schema.
        """
        # schema rows are in the format (order, name, type, ...)
        standardized_path = self.path(table_name, dataset_id, project_id,
                                      delimiter=MOCK_DELIMITER)
        # 'pragma' is SQLite's equivalent to DESCRIBE TABLE
        pragma_query = 'pragma table_info(\'' + standardized_path + '\')'
        single_row_query = 'SELECT * FROM ' + standardized_path + ' LIMIT 1'

        single_row = self.conn.execute(single_row_query).fetchall()
        schema = self.conn.execute(pragma_query).fetchall()

        returned_schema = []
        for i in range(len(schema)):
            row_name = schema[i][1]
            if len(single_row) > 0:
                row_type = self._db_type_to_bq_type(schema[i][2], sample=single_row[0][i])
            else:
                row_type = self._db_type_to_bq_type(schema[i][2])
            # Repeated fields are not supported in mock BigQuery so we always set the mode
            # to nullable.
            returned_schema.append(SchemaField(row_name, row_type, mode='NULLABLE'))
        return returned_schema

    def dataset_exists_with_name(self, dataset_name):
        # type: (str) -> bool
        """Determines whether a dataset exists with the given name.

        Args:
            dataset_name: The name of the dataset to check.

        Returns:
            True if the dataset exists in this client's project, False otherwise.
        """
        return dataset_name in self.table_map

    def table_exists_with_name(self, table_path):
        # type: (str) -> bool
        """Determines whether a table exists at the given table path.

        Args:
            table_path: The table path of the table to check. Uses the default dataset ID if a
                dataset is not specified as part of the table path.

        Returns:
            True if the table exists at the given path, False otherwise.
        """
        _, dataset_name, table_name = self.parse_table_path(table_path)
        return dataset_name in self.table_map and table_name in self.table_map[dataset_name]

    def tables(self, dataset_id):
        # type: (str) -> List[str]
        """Returns a list of table names in a given dataset.

        Args:
          dataset_id: The dataset to query.

        Returns:
          A list of table names (strings).
        """
        table_ids = []
        for table_path in self.table_map[dataset_id]:
            _, _, table_id = self.parse_table_path(table_path)
            table_ids.append(table_id)

        return table_ids

    def get_datasets(self):
        # type: (None) -> List[str]
        """Returns a list of dataset ids in the current project.

        Returns:
          A list of dataset ids/names (strings).
        """
        return self.project_map[self.project_id]

    def copy_table(self, source_table_path,  # type: str
                   destination_table_name,  # type: str
                   destination_dataset=None,  # type: Optional[str]
                   destination_project=None,  # type: Optional[str]
                   replace_existing_table=False  # type: bool
                   ):
        # type: (...) -> None
        """
        Copies the table at source_table_path to the location
        destination_project.destination_dataset.destination_table_name. If the destination project
        or dataset aren't set, the class default will be used.

        Args:
            source_table_path: The path of the table to copy.
            destination_table_name: The name of the table to copy to.
            destination_dataset: The name of the destination dataset. If unset, the client default
                dataset will be used.
            destination_project: The name of the destination project. If unset, the client default
                project will be used.
            replace_existing_table: If True, if the destination table already exists, it will delete
                it and copy the source table in its place.

        Raises:
            RuntimeError if the destination table already exists and replace_existing_table is False
            or the destination dataset does not exist
        """
        destination_project = destination_project or self.project_id
        destination_dataset = destination_dataset or self.default_dataset_id

        if destination_project not in self.project_map.keys():
            raise RuntimeError('Project {} does not exist.'.format(destination_project))

        if destination_dataset not in self.project_map[destination_project]:
            raise RuntimeError('Dataset {} does not exist in project {}.'
                               .format(destination_dataset, destination_project))

        if destination_table_name in self.table_map[destination_dataset]:
            if replace_existing_table:
                self.delete_table_by_name(self.path(destination_table_name,
                                                    destination_dataset,
                                                    destination_project,
                                                    delimiter=BQ_PATH_DELIMITER))
            else:
                raise RuntimeError('The table {} already exists in dataset {}.'
                                   .format(destination_table_name, destination_dataset))

        self.create_table_from_query('SELECT * FROM `{}`'.format(
                self._convert_to_other_db_path(source_table_path)),
                self.path(destination_table_name, destination_dataset, destination_project,
                          delimiter=BQ_PATH_DELIMITER))

