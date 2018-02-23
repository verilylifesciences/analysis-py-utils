# QueryKit Tutorial

## Overview

While SQL is well designed for describing non-iterative, rule-based computations,
it is terrible for a number of things that have become essential for
production software development. QueryKit is a library for defining SQL queries
within a Python script that fixes some (though by no means all) of these issues.

## How It Works

QueryKit allows the user to define a computation graph in which each node is a
subquery and edges define how to insert these subqueries into others. Once the
query graph is defined, the user specifies an output node and a collection of
tables to use as inputs, and QueryKit returns a complete query. It’s then up to
the user to run this query against their chosen database.

QueryKit only uses the parts of the computation graph that are required based on
the inputs and outputs that you specify. This way, a single computation graph
can be used for multiple purposes such as rerunning only a portion of a large
computation or unit testing a single subquery/node.

This tutorial walks through a made-up example on a non-existent dataset. So you
can compile the queries, but you can't run them.


## Query Graphs

To use QueryKit, you define a QueryGraph that encodes the subqueries and that will
make up the final query or queries, and the relationships between them. The
following code defines a simple QueryGraph.

Each call to AddQuery() defines a subquery node - inpatients, inpatient_count
and inpatient_and_revenue. Any references in these queries that are not the
names of subquery nodes are interpreted as input nodes and are added implicitly.
For this graph, the input nodes are visits and revenue_table.

```python
from google3.lifescience.clinical.tools.qtlib import template

graph = template.QueryGraph('my_project', 'default_dataset')

graph.AddQuery('inpatients',
               'SELECT * \nFROM {visits} \nWHERE visit_type = "INPATIENT"')

graph.AddQuery('inpatient_count',
               'SELECT facility, year, COUNT(*) AS in_count \n'
               'FROM {inpatients} \nGROUP BY facility, year')

graph.AddQuery('inpatient_and_revenue',
               'SELECT * \nFROM {inpatient_count} \nJOIN {revenue_table}')
```

## Compiling Queries

To compile the whole query defined by this computation graph, we need to specify
the tables that the two green nodes represent.

When you call Compile, QueryKit traces the computation graph in reverse from the
output node (inpatient_and_revenue) back to nodes in the source dictionary
(revenue_table and visits). To have QueryKit compile only a portion of the
computation graph, simply specify an output node higher up the graph and/or
input nodes lower down.

```python
print graph.Compile('inpatient_and_revenue',
                    {'revenue_table': 'my_dataset.revenue_20160929',
                     'visits': 'my_dataset.visits_20160921'})
```

The output isn't properly formatted, but it's syntactically correct.

```sql
SELECT *
FROM (SELECT facility, year, COUNT(*) AS in_count
FROM (SELECT *
FROM `my_dataset.visits_20160921`
WHERE visit_type = "INPATIENT")
GROUP BY facility, year)
JOIN `my_dataset.revenue_20160929`
```

In some cases, you may want to compile only a portion of a query graph, for
example if you want to unit test individual nodes, or use pre-computed
tables instead of subqueries. If you pass the name of an intermediate node
to the `Compile()` function, it will only compile the query up to that node.
Similarly, if you include an intermediate node in the source dictionary, it
will use that table instead of the subquery defined by that node.

For example, the following two calls insert source tables into individual
subqueries, creating final queries that can be used in unit tests.

```python
print graph.Compile('inpatient_and_revenue',
                    {'revenue_table': 'my_dataset.revenue_20160929',
                     'inpatient_count': 'precomputed.count'})
```

```SQL
SELECT *
FROM `precomputed.count`
JOIN `my_dataset.revenue_20160929`
```

```python
print graph.Compile('inpatient_count',
                    {'inpatients': 'precomputed.inpatients'})
```


```SQL
SELECT facility, year, COUNT(*) AS in_count
FROM `precomputed.inpatients`
GROUP BY facility, year
```

## Features Library

QueryKit also includes a library `features.py` for building common
feature-definition queries within QueryKit.

This library implements two functions for creating queries: Select() allows you
to define basic single-table queries, i.e. without joins. The more interesting
one is Aggregate(), which lets you create two-stage group by/aggregate queries
by passing it a list of objects called "reducers" that define Map and Reduce
steps.

The Aggregate() function takes a list of column names and reducers, and
generates a two-part query. The first part picks out the specified columns
and applies the Map() query fragments from each reducer to generate some new
columns. The second step groups the table by all the input columns and applies
the Reduce() step from each reducer to the columns that it defined in the first
step.

For example, the BoolCondition() reducer's Map step defines a column that is
1 for each row that meets a given condition, and 0 otherwise. The Reduce step
takes the MAX() of the values under the GROUP BY, producing a 1 if any one row
in the group met the condition and 0 otherwise.

Sample Usage:

```python
my_graph = template.QueryGraph('test-project', 'test-dataset')
features.Aggregate(
    my_graph, 'bool_aggregate', 'source',
    ['id', features.BoolCondition('feature1', 'my_condition')])
query = my_graph.Compile('bool_aggregate', {'source': 'test-source'})
```

The resulting query will GROUP BY id since this is the only non-reducer passed
to Aggregate().
