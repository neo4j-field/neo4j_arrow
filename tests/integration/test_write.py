import pyarrow

from neo4j_arrow.model import Graph, Node, Edge

user_count = 10
user_id_base = 1000
user_node_id_base = 0
question_count = 20
question_id_base = 2000
question_node_id_base = 100
answer_count = 40
answer_id_base = 3000
answer_node_id_base = 200


def graph_model() -> Graph:
    return (
        Graph(name="graph")
        .with_node(Node(source="users", label_field="label", key_field="node_id", id="real_id"))
        .with_node(Node(source="questions", label_field="label", key_field="node_id", id="real_id"))
        .with_node(Node(source="answers", label_field="label", key_field="node_id", id="real_id"))
        .with_edge(
            Edge(source="questions", type_field="asked_by_type", source_field="node_id", target_field="user_node_id")
        )
        .with_edge(
            Edge(source="answers", type_field="answer_for", source_field="node_id", target_field="question_node_id")
        )
    )


def users_table() -> pyarrow.Table:
    node_ids = [user_node_id_base + seq for seq in range(0, user_count)]
    names = ["node_id", "id", "user_name", "full_name", "label"]
    node_ids_data = pyarrow.array(list(node_ids))
    ids_data = pyarrow.array([seq + user_id_base for seq in node_ids])
    user_names_data = pyarrow.array([f"user_{seq + user_id_base}" for seq in node_ids])
    full_name_data = pyarrow.array([f"full_{seq + user_id_base} name_{seq + user_id_base}" for seq in node_ids])
    labels_data = pyarrow.array(["User" for _ in node_ids])

    result = pyarrow.Table.from_arrays(
        [node_ids_data, ids_data, user_names_data, full_name_data, labels_data], names=names
    ).replace_schema_metadata({"_table": "users"})

    return result


def questions_table() -> pyarrow.Table:
    node_ids = [question_node_id_base + seq for seq in range(0, question_count)]
    names = ["node_id", "id", "text", "user_id", "user_node_id", "label", "asked_by_type"]
    node_ids_data = pyarrow.array(list(node_ids))
    ids_data = pyarrow.array([seq + question_id_base for seq in node_ids])
    text_data = pyarrow.array([f"question {seq}" for seq in node_ids])
    user_id_data = pyarrow.array([seq % user_count + user_id_base for seq in node_ids])
    user_node_id_data = pyarrow.array([seq % user_count + user_node_id_base for seq in node_ids])
    label_data = pyarrow.array(["Question" for _ in node_ids])
    asked_by_type = pyarrow.array(["ASKED_BY" for _ in node_ids])

    result = pyarrow.Table.from_arrays(
        [node_ids_data, ids_data, text_data, user_id_data, user_node_id_data, label_data, asked_by_type], names=names
    ).replace_schema_metadata({"_table": "questions"})

    return result


def answers_table() -> pyarrow.Table:
    node_ids = [answer_node_id_base + seq for seq in range(0, answer_count)]
    names = [
        "node_id",
        "id",
        "question_id",
        "user_id",
        "text",
        "question_node_id",
        "user_node_id",
        "label",
        "authored_by_type",
        "answer_for_type",
    ]
    node_ids_data = pyarrow.array(list(node_ids))
    ids_data = pyarrow.array([seq + answer_id_base for seq in node_ids])
    question_id_data = pyarrow.array([seq % question_count + question_id_base for seq in node_ids])
    user_id_data = pyarrow.array([seq % user_count + user_id_base for seq in node_ids])
    text_data = pyarrow.array(
        [f"answer {seq + answer_id_base} for question {seq % question_count + question_id_base}" for seq in node_ids]
    )
    question_node_id_data = pyarrow.array([seq % question_count + question_node_id_base for seq in node_ids])
    user_node_id_data = pyarrow.array([seq % user_count + user_node_id_base for seq in node_ids])
    label_data = pyarrow.array(["Answer" for _ in node_ids])
    authored_by_type_data = pyarrow.array(["AUTHORED_BY" for _ in node_ids])
    answer_for_type_data = pyarrow.array(["ANSWER_FOR" for _ in node_ids])

    result = pyarrow.Table.from_arrays(
        [
            node_ids_data,
            ids_data,
            question_id_data,
            user_id_data,
            text_data,
            question_node_id_data,
            user_node_id_data,
            label_data,
            authored_by_type_data,
            answer_for_type_data,
        ],
        names=names,
    ).replace_schema_metadata({"_table": "answers"})

    return result


def test_write_nodes_only(driver, arrow_client_factory):
    users = users_table()
    model = graph_model()

    # send data
    graph_name = "user_graph"
    arrow_client = arrow_client_factory(graph_name)
    arrow_client.start_create_graph()
    arrow_client.write_nodes(users, model, "_table")
    arrow_client.nodes_done()
    arrow_client.edges_done()

    # assert what we have
    with driver.session() as session:
        result = session.run(
            "CALL gds.graph.list() YIELD graphName, nodeCount, relationshipCount "
            + "WITH * WHERE graphName = $graph_name RETURN *",
            graph_name=graph_name,
        ).fetch(1)

        assert len(result) == 1
        assert result[0]["graphName"] == graph_name
        assert result[0]["nodeCount"] == user_count
        assert result[0]["relationshipCount"] == 0


def test_write_nodes_and_rels(driver, arrow_client_factory):
    users = users_table()
    questions = questions_table()
    model = graph_model()

    # send data
    graph_name = "user_and_questions_graph"
    arrow_client = arrow_client_factory(graph_name)
    arrow_client.start_create_graph()
    arrow_client.write_nodes(users, model, "_table")
    arrow_client.write_nodes(questions, model, "_table")
    arrow_client.nodes_done()
    arrow_client.write_edges(questions, model, "_table")
    arrow_client.edges_done()

    # assert what we have
    with driver.session() as session:
        result = session.run(
            "CALL gds.graph.list() YIELD graphName, nodeCount, relationshipCount "
            + "WITH * WHERE graphName = $graph_name RETURN *",
            graph_name=graph_name,
        ).fetch(1)

        assert len(result) == 1
        assert result[0]["graphName"] == graph_name
        assert result[0]["nodeCount"] == user_count + question_count
        assert result[0]["relationshipCount"] == question_count


def test_write_whole_graph(driver, arrow_client_factory):
    users = users_table()
    questions = questions_table()
    answers = answers_table()
    model = graph_model()

    # send data
    graph_name = "whole_graph"
    arrow_client = arrow_client_factory(graph_name)
    arrow_client.start_create_graph()
    arrow_client.write_nodes(users, model, "_table")
    arrow_client.write_nodes(questions, model, "_table")
    arrow_client.write_nodes(answers, model, "_table")
    arrow_client.nodes_done()
    arrow_client.write_edges(questions, model, "_table")
    arrow_client.write_edges(answers, model, "_table")
    arrow_client.edges_done()

    # assert what we have
    with driver.session() as session:
        result = session.run(
            "CALL gds.graph.list() YIELD graphName, nodeCount, relationshipCount "
            + "WITH * WHERE graphName = $graph_name RETURN *",
            graph_name=graph_name,
        ).fetch(1)

        assert len(result) == 1
        assert result[0]["graphName"] == graph_name
        assert result[0]["nodeCount"] == user_count + question_count + answer_count
        assert result[0]["relationshipCount"] == question_count + answer_count


def test_read_graph(driver, arrow_client_factory):
    # construct a graph
    test_write_whole_graph(driver, arrow_client_factory)

    # read data
    arrow_client = arrow_client_factory("whole_graph")
    users = list(arrow_client.read_nodes(properties=["real_id"], labels=["User"], concurrency=1))
    questions = list(arrow_client.read_nodes(properties=["real_id"], labels=["Question"], concurrency=1))
    answers = list(arrow_client.read_nodes(properties=["real_id"], labels=["Answer"], concurrency=1))

    assert len(users) == 1
    assert users[0].num_rows == user_count
    assert len(questions) == 1
    assert questions[0].num_rows == question_count
    assert len(answers) == 1
    assert answers[0].num_rows == answer_count

    asked_by = list(arrow_client.read_edges(relationship_types=["ASKED_BY"], concurrency=1))
    answer_for = list(arrow_client.read_edges(relationship_types=["ANSWER_FOR"], concurrency=1))
    assert len(asked_by) == 1
    assert asked_by[0].num_rows == question_count
    assert len(answer_for) == 1
    assert answer_for[0].num_rows == answer_count
