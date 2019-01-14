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
"""Defines QueryGraph, which encodes the dependencies between query templates.

This is the central component of query_kit, whose goal is to make SQL queries
modular and testable.
"""

import re


class GraphConstructionError(Exception):
  """Raised when error occurs during graph construction."""
  pass


class CyclicGraphError(GraphConstructionError):
  """Raised when a cycle is detected while constructing the query graph."""

  def __init__(self, cycle):
    GraphConstructionError.__init__(self)
    self.cycle = cycle

  def __str__(self):
    return 'Cycle found while constructing query graph: {}'.format(
        '->'.join(self.cycle))


class GraphCompileError(Exception):
  """Raised when an error occurs while compiling the query graph."""
  pass


class QueryGraph(object):
  """Encodes a graph of queries and their dependencies.

  Args:
    project: The BigQuery project containing the source data.
    dataset: The BigQuery dataset.
  """

  def __init__(self, project=None, dataset=None):
    self.project = project
    self.dataset = dataset
    self.query_templates = {}
    self.query_dependencies = {}
    self.expected_inputs = set()

  def add_query(self, query_name, template):
    """Adds a query to overall template.

    The query template should contain placeholders inside squiggly brackets for
    tables and subqueries that it relies on.

    To define a query whose final form contains curly brackets, double each
    of the brackets that you want to preserve. I.e. {{ and }} in the template
    will be translated to { and } in the final query.

    Args:
      query_name: The name of the new query.
      template: The text of the query.

    Raises:
      GraphConstructionError: If the query has the same name as an existing
          query.
      CyclicGraphError: If the query would create a cycle.
    """

    if query_name in self.query_templates.keys():
      raise GraphConstructionError(
          'Query name ' + query_name + ' already defined.')

    # Don't interpret text inside double curly brackets as sources.
    escaped = template.replace('{{', '').replace('}}', '')
    dependencies = list(set([name[1:-1] for name in
                             re.findall(r'{.*?}', escaped)]))

    # It's inefficient to check after each added query, instead of checking once
    # during compile-time, but the benefit is that the error is raised
    # immediately on the add_query call that completes the cycle, instead of at
    # some later point in time.
    self._check_for_cycles(query_name, dependencies)  # raises CyclicGraphError

    for dep in dependencies:
      if dep not in self.query_templates.keys():
        self.expected_inputs.add(dep)
    self.query_templates[query_name] = template
    self.query_dependencies[query_name] = dependencies

  def compile(self, target, source_dictionary):
    """Creates a complete query for a specified node in the dependency graph.

    When compiling the query, this function follows the dependency graph from
    the query until it gets to a placeholder name in the source dictionary.
    If it gets to a name that's both in source_dictionary and that refers to
    another query in the graph, it will use the table in the source dictionary.
    So this allows the caller to define a subgraph of the dependency graph to
    evaluate.

    Args:
      target: The query to generate.
      source_dictionary: Keys are placeholders in the target query and the
          queries it references. Values are the names of tables within the
          project and dataset passed into the QueryGraph's constructor.

    Returns:
      A string representing a complete query for the passed-in target.

    Raises:
      GraphCompileError: The target query is not defined.
    """

    if target not in self.query_templates.keys():
      raise GraphCompileError(
          'Query ' + target + ' is not defined.')
    queries = {name: self._table_path(source_dictionary[name])
               for name in source_dictionary.keys()}
    self._add_compiled_query(target, queries)
    # Each query in the placeholder dictionary, queries, is surrounded by
    # parentheses which must be removed before the query is returned.
    return queries[target][1:-1]

  def _table_path(self, table_name):
    """Returns a formatted table path with the Graph's project and dataset.

    If the input table name contains one or more periods, the function assumes
    that it already has the project and dataset, and thus just adds angle
    quotes.

    Args:
      table_name: The name of a table in the BigQuery dataset associated with
          the QueryGraph, or a complete BigQuery table path.

    Returns:
      A string.
    """

    if table_name.find('.') > -1 or not self.project:
      return '`' + table_name + '`'
    return '`{}.{}.{}`'.format(self.project, self.dataset,
                               table_name)

  def _add_compiled_query(self, target, queries):
    """Adds a compiled query to a dictionary of placeholder names and values.

    Each value in the dictionary is either a complete table path in left
    angle quotes (`) or a query within parentheses so that it can be inserted
    directly in to the FROM clause of another query.

    Args:
      target: A key in the query_template dictionary for the query to compile.
      queries: A dictionary of placeholders.

    Raises:
      GraphCompileError: A source table expected by the query graph is not
          in source_dictionary.
    """

    for name in self.query_dependencies[target]:
      if name not in queries.keys():
        if name not in self.query_templates.keys():
          raise GraphCompileError(
              'Table ' + name + ' must be included in the source dictionary.')
        self._add_compiled_query(name, queries)
    queries[target] = ('(' + self.query_templates[target].format(**queries) +
                       ')')

  def _check_for_cycles(self, query_name, dependencies):
    """Checks to see if any new cycles are created.

    Args:
      query_name: The name of the new query to be added. May be a dependency of
        an existing query.
      dependencies: The list of dependencies of the new query.
    """
    # Use DFS. If DFS ever runs into a node it's already seen, that's a cycle.
    def _Dfs(current_chain, dependencies):
      for dep in dependencies:
        if dep in current_chain:
          raise CyclicGraphError(current_chain + [dep])

        if dep not in self.query_dependencies:
          # We've chased the dependencies to the end, no cycle found.
          return

        current_chain.append(dep)
        _Dfs(current_chain, self.query_dependencies[dep])
        current_chain.pop()

    _Dfs([query_name], dependencies)
