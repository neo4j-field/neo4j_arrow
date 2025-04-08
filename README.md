# neo4j_arrow (DEPRECATED)

----

> ⚠️ **Warning** ⚠️
> # This repository is no longer maintained.
> Please use the following instead: [**graph-data-science-client**](https://github.com/neo4j/graph-data-science-client).

-----

PyArrow client for working with GDS Apache Arrow Flight Service

# What is this?

This client exists to provide a lower-level means of interacting with
the Neo4j GDS Arrow Flight Service. While the official [GDS Python
Client](https://github.com/neo4j/graph-data-science-client) will
leverage Apache Arrow for some graph creation and property streaming
operations, this client allows for full usage of the Arrow Flight
Service and is designed for scaling these operations across many
clients simultaneously.

If you're looking to simply interact with Neo4j GDS via Python, please
first look to the official
[client](https://github.com/neo4j/graph-data-science-client) before
using this one.

# 3rd Party Requirements

This module is written to be embedded in other projects. As such, the
only 3rd party requirement is `pyarrow`. Any suggested additions to
this project should rely entirely on capabilities in the `pyarrow`
module or the Python standard library (v3.

# Installation

The simplest way to use the client is to install using `pip` directly
from a release tarball or zip.

Assuming you're in an active virtual environment named "venv":

```
(venv) $ pip install neo4j_arrow@https://github.com/neo4j-field/neo4j_arrow/archive/refs/tags/0.6.1.tar.gz
```

> Note: this module is _not_ distributed via PyPI or Anaconda.

If you don't want requirements on pulling this module from github via
https, simply vendor a copy of the module into your project. You only
need the `neo4j_arrow` directory.

# Build

This project uses [poetry](https://python-poetry.org/) as the build tool.
Install `poetry`, define your environment with `poetry env use` and invoke `poetry install` to install dependencies.

To build;

```
poetry build
```

To run tests;

```
poetry run tox
```

# Usage

The client is designed to be lightweight and lazy. As such, you should
follow the concept of "one client per graph" and not feel compelled to
make and reuse a single client for operations across multiple distinct
databases or graphs.

## Instantiating

In the simplest case, you only need to provide a hostname or ip
address and the name of the target graph as positional
arguments. Common keyword arguments for other settings are shown
below (along with their defaults):

```python
import neo4j_arrow as na

client = na.Neo4jArrowClient("myhost.domain.com",
                             "mygraph",
                             port=8491,
                             database="neo4j",
                             tls=True,
                             user="neo4j",
                             password="neo4j",
                             concurrency=4,
                             debug=False,
                             timeout=None)
```

> At this point, you have a client instance, but it has _not_
> attempted connecting and authenticating to the Arrow Flight service.

This instance is safe to serialize and pass around in a multi-worker
environment such as Apache Spark or Apache Beam.

## Projecting a Graph

The process of projecting a graph mirrors the protocol outlined in the
docs
for [projecting graphs using Apache Arrow](https://neo4j.com/docs/graph-data-science/current/graph-project-apache-arrow/).
While
the client tries to track and mirror the protocol state internally, it
is ultimately the server that dictates what operations are valid and
when.

> For a detailed walkthrough, see this
> [notebook](https://github.com/neo4j-field/arrow-field-training/blob/main/notebooks/answers/ex03_PyArrow.ipynb).

### 1. Start an import process

#### 1a. Import into an in-memory graph

```python
result = client.start_create_graph()
```

Calling the `start_create_graph` method will connect and authenticate the client
(if it hasn't already been connected) and send a `CREATE_GRAPH` action
to the server.

`start_create_graph()` takes the following optional arguments;

| name                      | type          | default value | description                                                                                                                  |
|---------------------------|---------------|---------------|------------------------------------------------------------------------------------------------------------------------------|
| force                     | bool          | False         | Whether to abort an existing import and restart from scratch                                                                 |
| undirected_rel_types      | Iterable[str] | []            | A list of relationship types that must be imported as undirected. A wildcard (*) can be used to include all the types.       |
| inverse_indexed_rel_types | Iterable[str] | []            | A list of relationship types that must be indexed in inverse direction. A wildcard (*) can be used to include all the types. |

#### 1b. Import into a new database (not available on AuraDS)

```python
result = client.start_create_database()
```

Calling the `start_create_database` method will connect and authenticate the client
(if it hasn't already been connected) and send a `CREATE_DATABASE` action
to the server.

`start_create_database()` takes the following optional arguments;

| name              | type | default value | description                                                                                                                         |
|-------------------|------|---------------|-------------------------------------------------------------------------------------------------------------------------------------|
| force             | bool | False         | Force deletes any existing database files prior to the import.                                                                      |
| id_type           | str  | INTEGER       | Sets the node id type used in the input data. Can be either INTEGER or STRING                                                       |
| id_property       | str  | originalId    | The node property key which stores the node id of the input data.                                                                   |
| record_format     | str  | ""            | Database record format. Valid values are blank (no value, default), standard, aligned, or high_limit.                               |
| high_io           | bool | False         | Ignore environment-based heuristics, and specify whether the target storage subsystem can support parallel IO with high throughput. |
| use_bad_collector | bool | False         | Collects bad node and relationship records during import and writes them into the log.                                              |

The response (in this case, the `result` object) will be a Python dict
containing the name of the graph/database being imported:

```python
{"name": "mygraph"}
```

### 2a. Feed Nodes

Once an import is started, you can proceed to feeding nodes to the
server using the `write_nodes` method on the client. In its simplest
for, simply provide a PyArrow `Table`, `RecordBatch`, or
`Iterable[RecordBatch]` as the positional argument, making sure to
follow the expected schema as mentioned in the
[docs](https://neo4j.com/docs/graph-data-science/current/graph-project-apache-arrow/):

```python
import pyarrow as pa

# Create two nodes, :Person and the other :Person:VIP, with an age property
t = pa.Table.from_pydict({
    "nodeId": [1, 2],
    "labels": [["User"], ["User", "Admin"]],
    "age": [21, 40],
})

result = client.write_nodes(t)
```

On success, `write_nodes` will return a tuple of the number of items
(in this case nodes) written and the approximate number of bytes
transmitted to the server. For the above example, `result` will look
something like:

```python
(2, 67)
```

You may call `write_nodes` numerous times, concurrently, until you've
finished loading your node data.

### 2b. Signalling Node Completion

Once you've finished loading your nodes, you call `nodes_done`.

```python
result = client.nodes_done()
```

On success, it will return a Python dict with details on the number of
nodes loaded (from the point of view of the server) and the name of
the graph beign imported. For example:

```python
{"name": "mygraph", "node_count": 2}
```

### 3a. Feeding Relationships

Relationships are loaded similarly to nodes, albeit with a different schema requirement.

```python
import pyarrow as pa

t = pyarrow.Table.from_pydict({
    "sourceNodeId": [1, 1, 2],
    "targetNodeId": [2, 1, 1],
    "relationshipType": ["KNOWS", "SELF", "KNOWS"],
    "weight": [1.1, 2.0, 0.3],
})

result = client.write_edges(t)
```

Like with nodes, on success the result will be a tuple of number of
items sent and the approximate number of bytes in the payload. For the
above example:

```python
(3, 98)
```

Again, like with nodes, you may call `write_edges` numerous times,
concurrently.

### 3b. Signaling Relationship Completion

Once you've finished loading your relationships, you signal to the
server using the `edges_done` method.

```python
result = client.edges_done()
```

On success, the result returned will be a Python dict containing the
name of the graph being imported and the number of relationships as
observed by the server-side:

```python
{"name": "mygraph", "relationship_count": 3}
```

### 4. Validating the Import

At this point, there's nothing left to do from the client perspective
and the data should be live in Neo4j. You can use the GDS Python
Client to quickly validate the schema of the graph:

```python
from graphdatascience import GraphDataScience

gds = GraphDataScience("neo4j+s://myhost.domain.com", auth=("neo4j", "neo4j"))
gds.set_database("neo4j")

G = gds.graph.get("mygraph")

print({
    "nodes": (G.node_labels(), G.node_count()),
    "edges": (G.relationship_types(), G.relationship_count())
})
```

For the previous examples, you should see something like:

```python
{"nodes": (["User", "Admin"], 2), "edges": (["KNOWS", "SELF"], 3)}
```

## Streaming Graph Data and Features

The Neo4j GDS Arrow Flight Service supports streaming data from the
graph in the form of node/relationship properties and relationship
topology. These rpc calls are exposed via some methods in the
`Neo4jArrowClient` to make it easier to integrate to existing
applications.

### 1. Streaming Node Properties

Node properties from a graph projection can be streamed from
GDS/AuraDS with optional filters based on node label. The stream is
exposed as a Python generator and can be consumed lazily, but **it is
important to fully consume the stream** as older versions of GDS do
not have a server-side timeout. (If the client dies or fails to
consume the full stream, server threads may be deadlocked.)

```python
nodes = client.read_nodes(["louvain", "pageRank"], labels=["User"])

# if you know the resulting dataset is small, you can eagerly consume into a
# Python list
result = list(nodes)

# the `result` is now a list of PyArrow RecordBatch objects
print(result)
```

Inspecting the schema, like in the example above, will reveal that
only the node ids and the request properties are provided. _The labels
are not provided and must be implied based on your labels filter!_
(This is a GDS limitation in the stored procedures for streaming node
properties.)

The above result from the `print()` function will look like the
following (assuming a single item in the list):

```python
[pyarrow.RecordBatch
 nodeId: int64
not null
louvain: int64
not null
pageRank: int64
not null]
```

### 2. Streaming Relationships

Relationship properties can be streamed similar to node properties,
however they support an alternative mode for streaming the "topology"
of the graph when not providing any property or relationship type
filters.

> Note: the relationship types are dictionary encoded, so if accessing
> the raw PyArrow buffer values, you will need to decode. Conversion
> to Python dicts/lists or Pandas DataFrames will decode for you back
> into strings/varchars.

Like with node streams, the stream is provided as a Python generator
and requires full consumption by the client program.

```python
# dump just the topology without properties
topology = client.read_edges()
# `topology` is a generator producing PyArrow RecordBatch objects

# if you want properties and/or to target particular relationship types...
edges = client.read_edges(properties=["score"], relationship_types=["SIMILAR"])
```

## The Graph Model

A graph model could also be used, constructed programmatically or via JSON, to
dictate how to translate the datasource fields to the appropriate parts (nodes,
edges) of the intended graph.

In Python, it looks like:

```python
from neo4j_arrow.model import Graph

G = (
    Graph(name="test", db="neo4j")
    .with_node(Node(source="gs://.*/papers.*parquet", label_field="labels",
                    key_field="paper"))
    .with_node(Node(source="gs://.*/authors.*parquet", label_field="labels",
                    key_field="author"))
    .with_node(Node(source="gs://.*/institution.*parquet", label_field="labels",
                    key_field="institution"))
    .with_edge(Edge(source="gs://.*/citations.*parquet", type_field="type",
                    source_field="source", target_field="target"))
    .with_edge(Edge(source="gs://.*/affiliation.*parquet", type_field="type",
                    source_field="author", target_field="institution"))
    .with_edge(Edge(source="gs://.*/authorship.*parquet", type_field="type",
                    source_field="author", target_field="paper"))
)
```

The same graph model, but in JSON:

```json
{
  "name": "test",
  "db": "neo4j",
  "nodes": [
    {
      "source": "gs://.*/papers.*parquet",
      "label_field": "labels",
      "key_field": "paper"
    },
    {
      "source": "gs://.*/authors.*parquet",
      "label_field": "labels",
      "key_field": "author"
    },
    {
      "source": "gs://.*/institution.*parquet",
      "label_field": "labels",
      "key_field": "institution"
    }
  ],
  "edges": [
    {
      "source": "gs://.*/citations.*parquet",
      "type_field": "type",
      "source_field": "source",
      "target_field": "target"
    },
    {
      "source": "gs://.*/affiliation.*parquet",
      "type_field": "type",
      "source_field": "author",
      "target_field": "institution"
    },
    {
      "source": "gs://.*/authorship.*parquet",
      "type_field": "type",
      "source_field": "author",
      "target_field": "paper"
    }
  ]
}
```

The fields of the Nodes and Edges in the model have specific purposes:

- `source` -- a regex pattern used to match source data against the model, i.e.
  it's used to determine which node or edge a record corresponds to.
- `label` -- the fixed label to be used for the imported nodes.
- `label_field` -- the source field name containing the node label or labels
- `key_field` -- the source field name containing the unique node identifier
  (NB: this currently must be a numeric field as of GDS 2.1.)
- `type` -- the fixed edge type for the imported edges.
- `type_field` -- the source field name containing the edge type
- `source_field` -- the source field name containing the node identifier of the
  origin of an edge.
- `target_field` -- the source field name containing the node identifier of the
  target of an edge.

Example model JSON files are provided in [example_models](./example_models).

### Streaming Caveats

There are a few known caveats to be aware of when creating and consuming Arrow-based streams from Neo4j GDS:

- You must consume the stream in its entirety to avoid blocking
  server-side threads.
    - While recent versions of GDS will include a timeout, older
      versions will consume one or many threads until the stream is
      consumed.
    - There's no API call (yet) for aborting a stream, so failing to
      consume the stream will prevent the threads from having tasks
      scheduled on them for other stream requests.

- The only way to request Nodes is to do so by property, which means
  nodes that don't have properties may not be streamable today.

# Examples in the Wild

The `neo4j_arrow` module is used by multiple existing Neo4j projects:

- Google Dataflow Flex Template for Neo4j GDS & AuraDS
    - An Apache Beam pipeline for large scale import of graphs.
    - https://github.com/neo4j-field/dataflow-flex-pyarrow-to-gds

- Google BigQuery Stored Procedure for Neo4j GDS & AuraDS
    - An Apache Spark job powering a BigQuery Stored Procedure for
      bidirectionl data integration between BigQuery Tables and Neo4j
      GDS graphs.
    - https://github.com/neo4j-field/bigquery-connector

- Neo4j GraphConnect 2022 Keynote Demo
    - A Jupyter Notebook-based approach for high-performance streaming
      of BigQuery data into Neo4j GDS as featured at Neo4j GraphConnect
        2022.
    - https://github.com/neo4j-product-examples/ds-graphconnect-2022-demo

# Copyright & License

`neo4j_arrow` is licensed under the Apache Software License version
2.0. All content is copyright © Neo4j Sweden AB.
