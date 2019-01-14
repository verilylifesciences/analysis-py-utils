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
"""Functions for building common feature-definition queries within QueryKit.

This library implements two functions for creating queries: select() allows you
to define basic single-table queries, i.e. without joins. The more interesting
one is aggregate(), which lets you create two-stage group by/aggregate queries
by passing it a list of objects called "reducers" that define Map and Reduce
steps.

The aggregate() function takes a list of column names and reducers, and
generates a two-part query. The first part picks out the specified columns
and applies the map() query fragments from each reducer to generate some new
columns. The second step groups the table by all the input columns and applies
the reduce() step from each reducer to the columns that it defined in the first
step.

For example, the BoolCondition() reducer's Map step defines a column that is
1 for each row that meets a given condition, and 0 otherwise. The Reduce step
takes the MAX() of the values under the GROUP BY, producing a 1 if any one row
in the group met the condition and 0 otherwise.

Sample Usage:

    my_graph = template.QueryGraph('test-project', 'test-dataset')
    features.aggregate(
        my_graph, 'bool_aggregate', 'source',
        ['id', features.BoolCondition('feature1', 'my_condition')])
    query = my_graph.compile('bool_aggregate', {'source': 'test-source'})

The resulting query will GROUP BY id since this is the only non-reducer passed
to aggregate().
"""

import uuid


class BoolCondition(object):
  """A reducer that produces a one if any row in a group meets a condition.

  The Map step defines a column with a 1 in each row that meets a given
  condition and a 0 otherwise. The Reduce step takes the MAX() of these values,
  so the result will be 1 if one of the rows in a given group meets the
  condition.
  """

  def __init__(self, feature_name, condition):
    """Initializes BoolCondition class.

    Args:
      feature_name: The name of the column that the reducer should define.
      condition: A query fragment defining the condition to check.
    """
    self._feature_name = feature_name
    self._condition = condition

  def map(self, unused_group_by):
    return (self._feature_name,
            'CASE WHEN {} THEN 1 ELSE 0 END'.format(self._condition))

  def reduce(self):
    return (self._feature_name, 'MAX({})'.format(self._feature_name))


class CountCondition(BoolCondition):
  """A reducer that counts the number of rows meeting a condition in each group.

  The Map step is the same as BoolCondition. The Reduce step, instead of
  taking a MAX(), takes a sum.
  """

  def reduce(self):
    return (self._feature_name, 'SUM({})'.format(self._feature_name))


class SelectByDate(object):
  """A reducer that picks a value from each group of rows based on a date.

  In particular, it uses a sql aggregator to pick a value from a given column
  among all rows in each group that meet a given condition. The Map step defines
  columns with the values and dates from only the rows that meet the given
  condition, as well as a column with the aggregated date over all such rows
  in the same group as the current row. The reducer picks one of the row whose
  date matches the aggregated date in its group.
  """

  def __init__(self, feature_name, select_column, date_column, where='true',
               where_date='MAX', if_null=''):
    """Initializes SelectByDate class.

    Args:
      feature_name: The name of the column that the reducer should define.
      select_column: Column with the value that should be selected.
      date_column: Column with the date that should be used to select a row.
          The value from the row with the oldest or newest date (depending on
          the value of where_date) will be selected.
      where: A sql fragment defining a condition. Only values from rows that
          meet this condition will be considered.
      where_date: A sql aggregator for selecting amongst dates. Use 'MIN' for
          earliest and 'MAX' for latest date. Default: 'MAX'.
      if_null: Optional string.  The value to use in place of null if no rows
          match the given condition. Default: ''.
    """

    self._feature_name = feature_name
    self._select_column = 'CASE WHEN {} THEN {} ELSE null END'.format(
        where, select_column)
    self._date_column = ('CASE WHEN {} THEN {} ELSE null END'.format(
        where, date_column))
    self._where_date = where_date
    # Names of intermediate date columns defined in one subquery, and used
    # in the next.
    self._intr_date = self._feature_name + '_date'
    self._agg_date = self._feature_name + '_agg_date'
    self._if_null = if_null

  def map(self, group_by):
    return (self._agg_date, '{select_column} {feature_name}, '
            '{date_column} {intermediate_date}, '
            '{where_date}({date_column}) OVER (PARTITION BY {group_by})'.format(
                select_column=self._select_column,
                feature_name=self._feature_name,
                date_column=self._date_column,
                intermediate_date=self._intr_date,
                where_date=self._where_date,
                group_by=', '.join(group_by)))

  def reduce(self):
    fragment = 'MAX(CASE WHEN {} = {} THEN {} ELSE null END)'.format(
        self._intr_date, self._agg_date, self._feature_name)
    if self._if_null:
      fragment = 'IFNULL({}, {})'.format(fragment, self._if_null)
    return (self._feature_name, fragment)


def map_values(conditions, default=None):
  """A helper function that turns a set of conditions into a query fragment.

  Args:
    conditions: A set of tuples (condition, value), both of which are query
        fragments.
    default: A query fragment defining the default value if none of the
        conditions are met.

  Returns:
    A string query fragment that represents a set of conditions.
  """
  fragment = 'CASE ' + ' '.join('WHEN {} THEN {}'.format(condition, value)
                                for condition, value in conditions)
  if default:
    fragment += ' ELSE ' + default
  return fragment + ' END'


def select(graph, result_name, source_placeholder, selects,
           where=None, group_by=None, order_by=None):
  """Adds a basic single-source query, i.e. without joins or nested selects.

  Args:
    graph: A QueryGraph.
    result_name: The name of the node to add to the QueryGraph.
    source_placeholder: The name of the QueryGraph node to use as the source.
    selects: A list of tuples (name, source) where source is a query
        fragment defining the value of a column and name is the column's name.
    where: A list of conditions that all rows in the result should meet.
    group_by: A list of columns to group by. This must contain every
        non-aggregate column in selects. The function does not add them
        for you.
    order_by: A list of fields to order the results by.
  """
  query = 'SELECT {} FROM {{{}}}'.format(
      ', '.join('{} {}'.format(source, name) for name, source in selects),
      source_placeholder)
  if where:
    query += ' WHERE ' + ', '.join(where)
  if group_by:
    query += ' GROUP BY ' + ', '.join(group_by)
  if order_by:
    query += ' ORDER BY ' + ', '.join(order_by)
  graph.add_query(result_name, query)


def aggregate(graph, result_name, source_placeholder, column_list, where=None):
  """Defines a map query and a reduce query based on a list of reducers.

  Args:
    graph: A QueryGraph.
    result_name: The name of the node to add to the QueryGraph.
    source_placeholder: The name of the QueryGraph node to use as the source.
    column_list: A list of strings and reducers. Each string is a column
        name which goes into the GROUP BY list. Each reducer defines a
        column in the table defined by the resulting query.
    where: Optional list of conditions used to filter the first stage of the
        query.
  """
  group_by = [name for name in column_list
              if isinstance(name, basestring)]
  group_selects = [(name, name) for name in group_by]
  reducers = [reducer for reducer in column_list
              if not isinstance(reducer, basestring)]
  intermediate_name = '{}_{}'.format(result_name, uuid.uuid4())
  select(graph, intermediate_name, source_placeholder,
         group_selects + [reducer.map(group_by) for reducer in reducers],
         where=where)
  select(graph, result_name, intermediate_name,
         [(column, column) if isinstance(column, basestring)
          else column.reduce() for column in column_list],
         group_by=group_by, where=where)
