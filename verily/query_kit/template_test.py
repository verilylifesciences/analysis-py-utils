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
"""Tests for verily.query_kit.template."""

import unittest

from verily.query_kit import template


class TemplateTest(unittest.TestCase):

  def test_simple_query(self):
    test_graph = template.QueryGraph('test-project', 'test-dataset')
    test_graph.add_query('simple_query', 'SELECT * FROM {source}')
    query = test_graph.compile('simple_query', {'source': 'test-source'})
    self.assertEqual(query,
                     'SELECT * FROM `test-project.test-dataset.test-source`')

  def test_no_default_project(self):
    test_graph = template.QueryGraph()
    test_graph.add_query('simple_query', 'SELECT * FROM {source}')
    query = test_graph.compile('simple_query', {'source': 'test-source'})
    self.assertEqual(query, 'SELECT * FROM `test-source`')

  def test_graph_dependencies(self):
    test_graph = template.QueryGraph('test-project', 'test-dataset')
    test_graph.add_query('outer_query',
                         'SELECT * FROM {inner_query} JOIN {other_source}')
    test_graph.add_query('inner_query', 'SELECT * FROM {source}')
    full_query = test_graph.compile('outer_query', {'source': 'test-source',
                                                    'other_source': 'other'})
    self.assertEqual(full_query,
                     'SELECT * FROM '
                     '(SELECT * FROM `test-project.test-dataset.test-source`) '
                     'JOIN `test-project.test-dataset.other`')

    partial_query = test_graph.compile('outer_query',
                                       {'inner_query': 'mock-table',
                                        'other_source': 'other'})
    self.assertEqual(partial_query,
                     'SELECT * FROM `test-project.test-dataset.mock-table` '
                     'JOIN `test-project.test-dataset.other`')

  def test_cyclic_graph_detection(self):
    query_graph = template.QueryGraph('test-project', 'test-dataset')
    # Adding components of the circle are fine...
    query_graph.add_query('query1', 'SELECT * FROM {query2}')
    query_graph.add_query('query2', 'SELECT * FROM {query3}')
    # ...but completing the circle should throw an error.
    with self.assertRaises(template.CyclicGraphError):
      query_graph.add_query('query3', 'SELECT * FROM {query1}')

  def test_escaped_brackets(self):
    test_graph = template.QueryGraph()
    test_graph.add_query('simple_query', '{{ text }} SELECT * FROM {source}')
    query = test_graph.compile('simple_query', {'source': 'test-source'})
    self.assertEqual(query, '{ text } SELECT * FROM `test-source`')


if __name__ == '__main__':
  unittest.main()
