# Verily Python Analysis Utilities

## Setup

1. Ensure that the tests are run in an environment authenticated to send
requests to BigQuery, such as [Google Cloud
Shell](https://cloud.google.com/shell/docs/) or a local machine with
[gcloud](https://cloud.google.com/sdk/docs/) installed and `gcloud auth
application-default login` executed to configure the credential.

2. Install this Python package.
```bash
pip install git+https://github.com/verilylifesciences/analysis-py-utils.git
```

3. Set the environment variable for the Cloud Platform project in which the queries will run.
```bash
export TEST_PROJECT=YOUR-PROJECT-ID
```

## Using BQTestCase

BQTestCase provides an implementation of the Python
[unittest](https://docs.python.org/2/library/unittest.html) unit testing
framework for testing
[BigQuery](https://docs.python.org/2/library/unittest.html) queries. Tests that inherit from it will:

* Create the dataset and table for the input data for your test.
* Send your query to BigQuery for evaluation.
* Retrieve the query results for comparison to expected results.
* Clean up any datasets or tables created as a part of your test.

**experimental** It also provides a mock BigQuery framework for simple unit
tests that cuts running time from tens of seconds to less than a second.

Here is a simple example test using BQTestCase:

```python
import unittest
from google.cloud.bigquery.schema import SchemaField
from verily.bigquery_wrapper import bq_test_case

class SampleTest(bq_test_case.BQTestCase):

    # You always have to override this method in your tests that inherit from bq_test_case.
    # The constructor of BQTestCase calls this method and will raise a NotImplementedError
    # if it doesn't have a method body. You can create tables other places in your test, too,
    # but you probably want to create tables here that are across multiple tests.
    @classmethod
    def create_mock_tables(cls):
        # type: () -> None
        """Create mock tables"""
        cls.src_table_name = cls.client.path('tmp')
        cls.client.populate_table(
            cls.src_table_name,
            [SchemaField('foo', 'INTEGER'),
             SchemaField('bar', 'INTEGER'),
             SchemaField('baz', 'INTEGER')],
            [[1, 2, 3], [4, 5, 6]])

    @classmethod
    def setUpClass(cls):
        # type: () -> None
        """Set up class"""
        # By default, the BQTestCase will use BigQuery to back its tests. Setting
        # use_mocks to True will use mock_bq to back its tests. This makes the tests
        # run much faster than they would if they used actual BigQuery. Of course,
        # there are some limitations as to what the mock can handle.
        super(SampleTest, cls).setUpClass(use_mocks=False)

    # In your tests, use cls.client as your BigQuery client, and BQTestCase will
    # figure out the rest!
    def test_load_data(cls):
        # type: () -> None
        """Test bq.Client.get_query_results"""
        result = cls.client.get_query_results(
            'SELECT * FROM `' + cls.src_table_name + '`')
        cls.assertSetEqual(set(result), set([(1, 2, 3), (4, 5, 6)]))

if __name__ == "__main__":
  unittest.main()
```

### Testing templated queries in external files.

This framework can also be used to test queries in external files.

As a concrete example, suppose you have a query in file `data_counts.sql` that
uses [Jinja](http://jinja.pocoo.org/) for templating.

```sql
#standardSQL
  --
  -- Count the number of distinct foos and bars in the table.
  --
SELECT
  COUNT(DISTINCT foo) AS foo_cnt,
  COUNT(DISTINCT bar) AS bar_cnt
FROM
  `{{ DATA_TABLE }}`
```

To test it, you can create file `data_counts_test.py` in the same directory with
the following contents.

```python
import unittest
from google.cloud.bigquery.schema import SchemaField
from jinja2 import Template
from verily.bigquery_wrapper import bq_test_case


class QueryTest(bq_test_case.BQTestCase):
  @classmethod
  def setUpClass(cls):
    super(QueryTest, cls).setUpClass(use_mocks=False)

  @classmethod
  def create_mock_tables(cls):
    cls.src_table_name = cls.client.path("data")
    cls.client.populate_table(
        cls.src_table_name,
        [SchemaField("foo", "STRING"),
         SchemaField("bar", "INTEGER")],
        [
            ["foo1", 0],
            ["foo2", 0],
            ["foo3", 10],
            ["foo1", 10]
        ]
    )
  def test_data_counts(self):
    # Perform the Jinja template replacement.
    sql = Template(
        open("data_counts.sql", "r").read()).render(
            {"DATA_TABLE": self.src_table_name})
    result = self.client.get_query_results(sql)
    self.assertSetEqual(set(result), set([(3, 2),]))

if __name__ == "__main__":
  unittest.main()
```

### BigQuery

By default, BQTestCases will be run against BigQuery or you can explicitly pass
`use_mocks=False` to your superconstructor call.

### Mock BQ

To run BQTestCases against a mock BigQuery (implemented in mock_bq.py) pass
`use_mocks=True` to your superconstructor call. This makes tests run in less
than a second instead of the tens of seconds required for accessing actual
BigQuery tables.

The mock is backed by an in-memory SQLite database, so there are some caveats.

* Ensure that SQLite3 is installed on your system.
* SQLite3 can't do everything BigQuery does. If the queries you're testing are
pretty standard SQL you're probably fine. We do a few find-and-replace type
modifications on your behalf to transform BigQuery syntax to SQLite syntax:
  * typecasting to ensure decimal division
  * replacing the `EXTRACT(YEAR from x)` function to extract the year from a
    timestamp
  * replacing the `FORMAT` function with the equivalent SQLite3 `printf`
    function
  * replaces the `CONCAT` operator with SQLite3's `||` operator for
    concatenation

As you can imagine there is plenty of room for growth here. These are just the
functions that have come up in testing so far.

There will be some BigQuery functions that just won't work in SQLite3. In that
case pass `use_mocks=False` in your call to your class's superconstructor so
that the query is sent to BigQuery instead of SQLite3.

Whether you use the mock or not, it's a good idea to make sure your tests pass
using actual BigQuery.

## Troubleshooting

### BigQuery

If you're having problems with your tests running against real BigQuery, it can
be kind of tricky to debug because all your tables get deleted after the test is
run. To preserve your tables, override the default implementations of the
tearDown methods in you test.

```python
  def tearDown(self):
    """Disable clean up of tables after a test case is run."""
    pass

  @classmethod
  def tearDownClass(cls):
    """Disable clean up of the dataset after a test suite is run."""
    pass
```

The test tables have an expiration time of one hour so they will be deleted automatically
after that time elaspses but the empty datasets will remain, so be sure to delete those manually.

### Mock BQ

Probably the most common problem you'll run into is trying to do things in your
query that SQLite can't handle. If you try to do that, you'll get a
RuntimeException that says:

```
'We tried to reformat your query into SQLite, but it still won't work.
Check to make sure it was a valid query to begin with, then consider adding the transformation
needed to make it work in the _reformat_query method.
SQLite error: (whatever error SQLite threw)
The result query: (your query after we tried to transform it)
```

From there you can work through whatever solution you choose. There is a flag
passed into the `_reformat_query` function called `print_before_and_after`
that's False by default, but if you set it to True, it will automatically print
out your query as it's passed in to the function, then again after we tried to
reformat it, so that might be helpful for debugging.
