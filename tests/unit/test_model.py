import pytest

from neo4j_arrow.model import Graph, Node, Edge, ValidationError


def test_json_serde():
    """Test round-tripping a Graph through the JSON serialization methods."""
    g1 = (
        Graph(name="graph", db="db")
        .with_node(Node(source="a", label_field="label", key_field="key"))
        .with_node(
            Node(
                source="b",
                label="LabelB",
                label_field="label",
                key_field="key",
                prop1="prop1",
            )
        )
        .with_edge(
            Edge(
                source="r",
                edge_type="REL",
                type_field="type",
                source_field="src",
                target_field="tgt",
                prop="prop",
            )
        )
    )
    s = g1.to_json()
    g2 = Graph.from_json(s)
    print(f"g1: {g1}")
    print(f"g2: {g2}")
    assert g1 == g2


def test_reading_model_from_json():
    json = """
{
    "name": "faux-graph",
    "db": "neo4j",
    "nodes": [
        { "source": "papers", "label_field": "labels", "key_field": "paper",
          "properties": { "age": "age", "something": "else" } },
        { "source": "authors", "key_field": "author" }
    ],
    "edges": [
        { "source": "citations", "type_field": "type", "source_field": "source",
          "target_field": "target" },
        { "source": "authorship", "source_field": "author",
          "target_field": "paper" }
    ]
}
    """
    g = Graph.from_json(json)
    assert g.name == "faux-graph"
    assert g.db == "neo4j"
    assert len(g.nodes) == 2
    assert len(g.edges) == 2
    assert g.nodes[0].source == "papers"
    assert len(g.nodes[0].properties) == 2
    assert g.nodes[0].properties["something"] == "else"
    assert len(g.nodes[1].properties) == 0


def test_retrieving_by_source():
    g = (
        Graph(name="graph", db="db")
        .with_node(Node(source="alpha", label_field="label", key_field="key"))
        .with_node(
            Node(
                source="gs://.*/beta.*csv",
                label="LabelB",
                label_field="label",
                key_field="key",
                prop1="prop1",
            )
        )
        .with_edge(
            Edge(
                source="r.csv",
                edge_type="REL",
                type_field="type",
                source_field="src",
                target_field="tgt",
                prop="prop",
            )
        )
    )
    assert g.node_for_src("alpha") is not None
    assert g.node_for_src("beta.csv") is None
    assert g.node_for_src("gs://bucket/part_1/folder2/beta_01.csv") is not None
    assert g.node_for_src("gamma") is None
    assert g.edge_for_src("r.csv.001") is not None
    assert g.edge_for_src("red") is None


def test_retrieving_by_pattern():
    g = (
        Graph(name="graph", db="db")
        .with_node(Node(source="gs://.*/alpha[.]parquet", label_field="label", key_field="key"))
        .with_node(
            Node(
                source="beta",
                label="LabelB",
                label_field="label",
                key_field="key",
                prop1="prop1",
            )
        )
        .with_edge(
            Edge(
                source="r_[0-9]*.csv",
                edge_type="REL",
                type_field="type",
                source_field="src",
                target_field="tgt",
                prop="prop",
            )
        )
    )
    assert g.node_for_src("gs://bucket/nodes/alpha.parquet") is not None
    assert g.node_for_src("beta.csv.gz") is not None
    assert g.node_for_src("beta.csv") is not None
    assert g.node_for_src("alpha") is None
    assert g.edge_for_src("r_0001.csv") is not None


def test_node_validate():
    with pytest.raises(ValidationError, match="^source must be provided in"):
        Node(source="", key_field="id").validate()

    with pytest.raises(ValidationError, match="^key_field must be provided in"):
        Node(source="my_table", key_field="").validate()

    with pytest.raises(ValidationError, match="^either label or label_field must be provided in"):
        Node(source="my_table", key_field="id").validate()

    with pytest.raises(ValidationError, match="^use of label and label_field at the same time is not allowed in"):
        Node(source="my_table", key_field="id", label="label", label_field="label_field").validate()

    Node(source="my_table", key_field="id", label="my_label").validate()
    Node(source="my_table", key_field="id", label_field="my_label_field").validate()


def test_edge_validate():
    with pytest.raises(ValidationError, match="^source must be provided in"):
        Edge(source="", source_field="", target_field="").validate()

    with pytest.raises(ValidationError, match="^source_field must be provided in"):
        Edge(source="source", source_field="", target_field="").validate()

    with pytest.raises(ValidationError, match="^target_field must be provided in"):
        Edge(source="source", source_field="source", target_field="").validate()

    with pytest.raises(ValidationError, match="^either type or type_field must be provided in"):
        Edge(source="source", source_field="source", target_field="target").validate()

    with pytest.raises(ValidationError, match="^use of type and type_field at the same time is not allowed in"):
        Edge(
            source="source", source_field="source", target_field="target", edge_type="type", type_field="type_field"
        ).validate()

    with pytest.raises(ValidationError, match="^use of type and type_field at the same time is not allowed in"):
        Edge(
            source="source", source_field="source", target_field="target", type="type", type_field="type_field"
        ).validate()

    Edge(source="source", source_field="source_id", target_field="target_id", type="rel_type").validate()
    Edge(source="source", source_field="source_id", target_field="target_id", edge_type="rel_type").validate()
    Edge(source="source", source_field="source_id", target_field="target_id", type_field="type_field").validate()


def test_model_validate():
    with pytest.raises(ValidationError, match="either label or label_field must be provided in"):
        Graph(name="graph", db="db").with_node(Node(source="node", key_field="id")).validate()

    with pytest.raises(ValidationError, match="either type or type_field must be provided in"):
        Graph(name="graph", db="db").with_edge(
            Edge(source="edge", source_field="source_id", target_field="target_id")
        ).validate()

    Graph(name="graph", db="db").with_node(Node(source="node", key_field="id", label_field="label")).with_edge(
        Edge(source="edge", source_field="source_id", target_field="target_id", type_field="type")
    ).validate()
