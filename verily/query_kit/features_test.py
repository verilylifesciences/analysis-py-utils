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

from verily.query_kit import features
from verily.query_kit import template


class TemplateTest(unittest.TestCase):

  def test_select(self):
    test_graph = template.QueryGraph('test-project', 'test-dataset')
    features.select(test_graph, 'simple_select', 'source',
                    [('column1', 'source1'), ('column2', 'source2')])
    query = test_graph.compile('simple_select', {'source': 'test-source'})
    self.assertEqual(query,
                     'SELECT source1 column1, source2 column2 FROM '
                     '`test-project.test-dataset.test-source`')

    features.select(test_graph, 'complex_select', 'source',
                    [('column1', 'source1'), ('column2', 'source2')],
                    where=['condition1', 'condition2'],
                    group_by=['group_by_1', 'group_by_2'],
                    order_by=['order1', 'order2'])
    query = test_graph.compile('complex_select', {'source': 'test-source'})
    self.assertEqual(query,
                     'SELECT source1 column1, source2 column2 FROM '
                     '`test-project.test-dataset.test-source` '
                     'WHERE condition1, condition2 '
                     'GROUP BY group_by_1, group_by_2 '
                     'ORDER BY order1, order2')

  def test_map_values(self):
    test_graph = template.QueryGraph('test-project', 'test-dataset')
    features.select(
        test_graph, 'map_select', 'source',
        [('column1', features.map_values([('condition1', 'value1'),
                                          ('condition2', 'value2')],
                                         default='value3')),
         ('column2', 'source2')])
    query = test_graph.compile('map_select', {'source': 'test-source'})
    self.assertEqual(query,
                     'SELECT CASE WHEN condition1 THEN value1 '
                     'WHEN condition2 THEN value2 ELSE value3 END column1, '
                     'source2 column2 FROM '
                     '`test-project.test-dataset.test-source`')

  def test_aggregate_bool(self):
    test_graph = template.QueryGraph('test-project', 'test-dataset')
    features.aggregate(
        test_graph, 'bool_aggregate', 'source',
        ['id', features.BoolCondition('feature1', 'condition')])
    query = test_graph.compile('bool_aggregate', {'source': 'test-source'})
    self.assertEqual(
        query,
        'SELECT id id, MAX(feature1) feature1 FROM '
        '(SELECT id id, CASE WHEN condition THEN 1 ELSE 0 END feature1 FROM '
        '`test-project.test-dataset.test-source`) GROUP BY id')

  def test_column_order(self):
    test_graph = template.QueryGraph('test-project', 'test-dataset')
    features.aggregate(
        test_graph, 'bool_aggregate', 'source',
        [features.BoolCondition('feature1', 'condition'), 'id'])
    query = test_graph.compile('bool_aggregate', {'source': 'test-source'})
    self.assertEqual(
        query,
        'SELECT MAX(feature1) feature1, id id FROM '
        '(SELECT id id, CASE WHEN condition THEN 1 ELSE 0 END feature1 FROM '
        '`test-project.test-dataset.test-source`) GROUP BY id')

  def test_aggregate_count(self):
    test_graph = template.QueryGraph('test-project', 'test-dataset')
    features.aggregate(
        test_graph, 'count_aggregate', 'source',
        ['id', features.CountCondition('feature1', 'condition')])
    query = test_graph.compile('count_aggregate', {'source': 'test-source'})
    self.assertEqual(
        query,
        'SELECT id id, SUM(feature1) feature1 FROM '
        '(SELECT id id, CASE WHEN condition THEN 1 ELSE 0 END feature1 FROM '
        '`test-project.test-dataset.test-source`) GROUP BY id')

  def test_select_by_date(self):
    test_graph = template.QueryGraph('test-project', 'test-dataset')
    features.aggregate(
        test_graph, 'select_by_date', 'source',
        ['id', features.SelectByDate(
            'feature1', 'value', 'date', 'cond', 'MIN')])
    query = test_graph.compile('select_by_date', {'source': 'test-source'})
    self.assertEqual(
        query,
        'SELECT id id, MAX(CASE WHEN feature1_date = feature1_agg_date '
        'THEN feature1 ELSE null END) feature1 FROM (SELECT id id, '
        'CASE WHEN cond THEN value ELSE null END feature1, '
        'CASE WHEN cond THEN date ELSE null END feature1_date, '
        'MIN(CASE WHEN cond THEN date ELSE null END) OVER (PARTITION BY id) '
        'feature1_agg_date '
        'FROM `test-project.test-dataset.test-source`) GROUP BY id')

  def test_select_by_date_if_null(self):
    test_graph = template.QueryGraph('test-project', 'test-dataset')
    features.aggregate(
        test_graph, 'select_by_date', 'source',
        ['id', features.SelectByDate(
            'feature1', 'value', 'date', 'cond', 'MIN', '"nada"')])
    query = test_graph.compile('select_by_date', {'source': 'test-source'})
    self.assertEqual(
        query,
        'SELECT id id, IFNULL(MAX(CASE WHEN feature1_date = feature1_agg_date '
        'THEN feature1 ELSE null END), "nada") feature1 FROM (SELECT id id, '
        'CASE WHEN cond THEN value ELSE null END feature1, '
        'CASE WHEN cond THEN date ELSE null END feature1_date, '
        'MIN(CASE WHEN cond THEN date ELSE null END) OVER (PARTITION BY id) '
        'feature1_agg_date '
        'FROM `test-project.test-dataset.test-source`) GROUP BY id')

if __name__ == '__main__':
  unittest.main()
