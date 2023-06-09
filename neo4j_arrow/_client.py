import logging
from collections import abc
from enum import Enum
import json

import pyarrow as pa
import pyarrow.flight as flight

from . import error
from .model import Graph

from typing import (
    cast, Any, Callable, Dict, Generator, Iterable, List, Optional, Union, Tuple
)

Result = Tuple[int, int]
Arrow = Union[pa.Table, pa.RecordBatch]
Nodes = Union[pa.Table, pa.RecordBatch, Iterable[pa.RecordBatch]]
Edges = Union[pa.Table, pa.RecordBatch, Iterable[pa.RecordBatch]]
MappingFn = Callable[[Arrow], Arrow]


class ClientState(Enum):
    READY = "ready"
    FEEDING_NODES = "feeding_nodes"
    FEEDING_EDGES = "feeding_edges"
    AWAITING_GRAPH = "awaiting_graph"
    GRAPH_READY = "done"


class Neo4jArrowClient:
    host: str
    port: int
    database: str
    graph: str
    user: str
    password: str
    tls: bool
    concurrency: int
    timeout: Optional[float]
    max_chunk_size: int
    debug: bool
    client: flight.FlightClient
    call_opts: flight.FlightCallOptions
    logger: logging.Logger

    def __init__(self, host: str, graph: str, *, port: int = 8491, database: str = "neo4j",
                 user: str = "neo4j", password: str = "neo4j", tls: bool = True,
                 concurrency: int = 4, timeout: Optional[float] = None, max_chunk_size: int = 10_000,
                 debug: bool = False, logger: logging.Logger = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.tls = tls
        self.client = None
        self.call_opts = None
        self.graph = graph
        self.database = database
        self.concurrency = concurrency
        self.state = ClientState.READY
        self.debug = debug
        self.timeout = timeout
        self.max_chunk_size = max_chunk_size
        if not logger:
            logger = logging.getLogger("Neo4jArrowClient")
        self.logger = logger

    def __str__(self):
        return f"Neo4jArrowClient{{{self.user}@{self.host}:{self.port}/{self.database}?graph={self.graph}" \
               f"&encrypted={self.tls}&concurrency={self.concurrency}&debug={self.debug}&timeout={self.timeout}" \
               f"&max_chunk_size={self.max_chunk_size}}}"

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the FlightClient and CallOpts as they're not serializable
        if "client" in state:
            del state["client"]
        if "call_opts" in state:
            del state["call_opts"]
        return state

    def copy(self) -> 'Neo4jArrowClient':
        client = Neo4jArrowClient(self.host, self.graph, port=self.port, database=self.database, user=self.user,
                                  password=self.password, tls=self.tls, concurrency=self.concurrency,
                                  timeout=self.timeout, max_chunk_size=self.max_chunk_size,
                                  debug=self.debug)
        client.state = self.state
        return client

    def _client(self) -> flight.FlightClient:
        """
        Lazy client construction to help pickle this class because a PyArrow
        FlightClient is not serializable.
        """
        if not hasattr(self, "client") or not self.client:
            self.call_opts = None
            if self.tls:
                location = flight.Location.for_grpc_tls(self.host, self.port)
            else:
                location = flight.Location.for_grpc_tcp(self.host, self.port)
            client = flight.FlightClient(location)
            if self.user and self.password:
                try:
                    (header, token) = client.authenticate_basic_token(
                        self.user, self.password
                    )
                    if header:
                        self.call_opts = flight.FlightCallOptions(
                            headers=[(header, token)],
                            timeout=self.timeout,
                        )
                except flight.FlightUnavailableError as e:
                    raise error.interpret(e)
            self.client = client
        return self.client

    def _send_action(self, action: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Communicates an Arrow Action message to the GDS Arrow Service.
        """
        client = self._client()
        try:
            payload = json.dumps(body).encode("utf-8")
            result = client.do_action(flight.Action(action, payload), self.call_opts)
            obj = json.loads(next(result).body.to_pybytes().decode())
            return dict(obj)
        except Exception as e:
            raise error.interpret(e)

    def _get_chunks(self, ticket: Dict) -> Generator[Arrow, None, None]:
        client = self._client()
        try:
            result = client.do_get(pa.flight.Ticket(json.dumps(ticket).encode("utf8")), self.call_opts)
            for chunk, _ in result:
                yield chunk
        except Exception as e:
            raise error.interpret(e)

    @classmethod
    def _nop(cls, data: Arrow) -> Arrow:
        """
        Used as a no-op mapping function.
        """
        return data

    @classmethod
    def _node_mapper(cls, model: Graph,
                     source_field: Optional[str] = None) -> MappingFn:
        """
        Generate a mapping function for a Node.
        """

        def _map(data: Arrow) -> Arrow:
            schema = data.schema
            if source_field:
                src = schema.metadata.get(source_field.encode("utf8"))
                node = model.node_for_src(src.decode("utf8"))
            else:  # guess at labels
                my_label = data["labels"][0].as_py()
                node = model.node_by_label(my_label)
            if not node:
                raise Exception("cannot find matching node in model given "
                                f"{data.schema}")

            columns, fields = cls._rename_and_add_column([], [], data, node.key_field, "nodeId")
            if node.label:
                columns.append(pa.array([node.label] * len(data), pa.string()))
                fields.append(pa.field("labels", pa.string()))
            if node.label_field:
                columns, fields = cls._rename_and_add_column(columns, fields, data, node.label_field, "labels")
            for name in node.properties:
                columns, fields = cls._rename_and_add_column(columns, fields, data, name, node.properties[name])

            return data.from_arrays(columns, schema=pa.schema(fields))

        return _map

    @classmethod
    def _edge_mapper(cls, model: Graph,
                     source_field: Optional[str] = None) -> MappingFn:
        """
        Generate a mapping function for an Edge.
        """

        def _map(data: Arrow) -> Arrow:
            schema = data.schema
            if source_field:
                src = schema.metadata.get(source_field.encode("utf8"))
                edge = model.edge_for_src(src.decode("utf8"))
            else:  # guess at type
                my_type = data["type"][0].as_py()
                edge = model.edge_by_type(my_type)
            if not edge:
                raise Exception("cannot find matching edge in model given "
                                f"{data.schema}")

            columns, fields = cls._rename_and_add_column([], [], data, edge.source_field, "sourceNodeId")
            columns, fields = cls._rename_and_add_column(columns, fields, data, edge.target_field, "targetNodeId")
            if edge.type:
                columns.append(pa.array([edge.type] * len(data), pa.string()))
                fields.append(pa.field("relationshipType", pa.string()))
            if edge.type_field:
                columns, fields = cls._rename_and_add_column(columns, fields, data, edge.type_field, "relationshipType")
            for name in edge.properties:
                columns, fields = cls._rename_and_add_column(columns, fields, data, name, edge.properties[name])

            return data.from_arrays(columns, schema=pa.schema(fields))

        return _map

    @classmethod
    def _rename_and_add_column(cls, columns, fields: List, data: Arrow, current_name, new_name: str) -> (List, List):
        idx = data.schema.get_field_index(current_name)
        columns.append(data.columns[idx])
        fields.append(data.schema.field(idx).with_name(new_name))
        return columns, fields

    def _write_batches(self, desc: Dict[str, Any],
                       batches: Union[pa.RecordBatch, Iterable[pa.RecordBatch]],
                       mapping_fn: MappingFn = None) -> Result:
        """
        Write PyArrow RecordBatches to the GDS Flight service.
        """
        if isinstance(batches, abc.Iterable):
            batches = iter(batches)
        else:
            batches = iter([batches])

        fn = mapping_fn or self._nop

        first = next(batches, None)
        if not first:
            raise Exception("empty iterable of record batches provided")
        first = cast(pa.RecordBatch, fn(first))

        client = self._client()
        upload_descriptor = flight.FlightDescriptor.for_command(
            json.dumps(desc).encode("utf-8")
        )
        n_rows, n_bytes = 0, 0
        try:
            writer, _ = client.do_put(upload_descriptor, first.schema, self.call_opts)
            with writer:
                writer.write_batch(first)
                n_rows += first.num_rows
                n_bytes += first.get_total_buffer_size()
                for remaining in batches:
                    writer.write_batch(fn(remaining))
                    n_rows += remaining.num_rows
                    n_bytes += remaining.get_total_buffer_size()
        except Exception as e:
            raise error.interpret(e)
        return n_rows, n_bytes

    def start(self, action: str = "CREATE_GRAPH", *,
              config: Dict[str, Any] = None,
              force: bool = False) -> Dict[str, Any]:
        if not config:
            config = {
                "name": self.graph,
                "concurrency": self.concurrency,
            }
            if action == "CREATE_DATABASE":
                if force:
                    config.update({"force": force})
            else:
                config.update({"database_name": self.database})

        return self._start(action, config=config, force=force)

    def start_create_graph(self, *, force: bool = False, undirected_rel_types: Iterable[str] = [],
                           inverse_indexed_rel_types: Iterable[str] = []) -> \
            Dict[str, Any]:
        config = {
            "name": self.graph,
            "database_name": self.database,
            "concurrency": self.concurrency,
            "undirected_relationship_types": list(undirected_rel_types),
            "inverse_indexed_relationship_types": list(inverse_indexed_rel_types)
        }

        return self._start("CREATE_GRAPH", config=config, force=force)

    def start_create_database(self, *, force: bool = False, id_type: str = "", id_property: str = "",
                              record_format: str = "", high_io: bool = True, use_bad_collector: bool = False) -> \
            Dict[str, Any]:
        config = {
            "name": self.graph,
            "concurrency": self.concurrency,
            "high_io": high_io,
            "use_bad_collector": use_bad_collector,
            "force": force
        }

        if id_type:
            config["id_type"] = id_type
        if id_property:
            config["id_property"] = id_property
        if record_format:
            config["record_format"] = record_format

        return self._start("CREATE_DATABASE", config=config, force=force)

    def _start(self, action: str = "CREATE_GRAPH", *,
               config: Dict[str, Any] = None,
               force: bool = False) -> Dict[str, Any]:
        """
        Start an import job. Defaults to graph (projection) import.
        """
        assert not self.debug or self.state == ClientState.READY

        try:
            result = self._send_action(action, config)
            if result and result.get("name", None) == config["name"]:
                self.state = ClientState.FEEDING_NODES
                return result

            raise error.Neo4jArrowException(f"failed to start {action} for {config['name']}, got {result}")
        except error.AlreadyExists as e:
            if force:
                self.logger.warning(f"forcing cancellation of existing {action} import"
                                    f" for {config['name']}")
                if self.abort():
                    return self._start(action, config=config)

            self.logger.error(f"{action} import job already exists for {config['name']}")
        except Exception as e:
            self.logger.error(f"fatal error performing action {action}: {e}")
            raise e

        return {}

    def _write_entities(self, desc: Dict[str, Any], entities: Union[Nodes, Edges], mapper: MappingFn) -> Result:
        try:
            if isinstance(entities, pa.Table):
                entities = mapper(entities).to_batches(max_chunksize=self.max_chunk_size)
                mapper = self._nop

            return self._write_batches(desc, entities, mapper)
        except error.NotFound as e:
            self.logger.error(f"no existing import job found for graph f{self.graph}")
            raise e
        except Exception as e:
            self.logger.error(f"fatal error while feeding {desc['entity_type']}s for graph {self.graph}: {e}")
            raise e

    def write_nodes(self, nodes: Nodes,
                    model: Optional[Graph] = None,
                    source_field: Optional[str] = None) -> Result:
        assert not self.debug or self.state == ClientState.FEEDING_NODES
        desc = {"name": self.graph, "entity_type": "node"}
        if model:
            model.validate()
            mapper = self._node_mapper(model, source_field)
        else:
            mapper = self._nop

        return self._write_entities(desc, nodes, mapper)

    def nodes_done(self) -> Dict[str, Any]:
        assert not self.debug or self.state == ClientState.FEEDING_NODES

        try:
            result = self._send_action("NODE_LOAD_DONE", {"name": self.graph})
            if result and result.get("name", None) == self.graph:
                self.state = ClientState.FEEDING_EDGES
                return result

            raise error.Neo4jArrowException(f"invalid response for nodes_done for graph {self.graph}, got {result}")
        except Exception as e:
            raise error.interpret(e)

    def write_edges(self, edges: Edges,
                    model: Optional[Graph] = None,
                    source_field: Optional[str] = None) -> Result:
        assert not self.debug or self.state == ClientState.FEEDING_EDGES
        desc = {"name": self.graph, "entity_type": "relationship"}
        if model:
            model.validate()
            mapper = self._edge_mapper(model, source_field)
        else:
            mapper = self._nop

        return self._write_entities(desc, edges, mapper)

    def edges_done(self) -> Dict[str, Any]:
        assert not self.debug or self.state == ClientState.FEEDING_EDGES

        try:
            result = self._send_action("RELATIONSHIP_LOAD_DONE",
                                       {"name": self.graph})
            if result and result.get("name", None) == self.graph:
                self.state = ClientState.AWAITING_GRAPH
                return result

            raise error.Neo4jArrowException(f"invalid response for edges_done for graph {self.graph}, got {result}")
        except Exception as e:
            raise error.interpret(e)

    def read_edges(self, *, properties: Optional[List[str]] = None,
                   relationship_types: List[str] = None,
                   concurrency: int = 4) -> Generator[Arrow, None, None]:
        """
        Stream edges (relationships) from a Neo4j graph projection. When
        requesting properties, they must be requested explicitly. However,
        all relationship types may be selected using the special ['*'] value.

        N.b. relationship types are dictionary-encoded.
        """
        if concurrency < 1:
            raise ValueError("concurrency cannot be negative")
        if properties:
            procedure_name = "gds.graph.relationshipProperties.stream"
            configuration = {
                "relationship_properties": list(properties),
                "relationship_types": list(relationship_types) or ["*"],
            }
        else:
            procedure_name = "gds.beta.graph.relationships.stream"
            configuration = {
                "relationship_types": list(relationship_types) or ["*"],
            }

        return self._get_chunks({
            "graph_name": self.graph,
            "database_name": self.database,
            "procedure_name": procedure_name,
            "configuration": configuration,
            "concurrency": concurrency,
        })

    def read_nodes(self, properties: List[str] = None, *,
                   labels: List[str] = None,
                   concurrency: int = 4) -> Generator[Arrow, None, None]:
        """
        Stream node properties for nodes of a given label. Oddly, this supports
        streaming back just the node ids if given an empty list, unlike the
        behavior of the corresponding stored procedure.

        N.b. Unlike read_edges, there's no analog to just requesting topology,
        i.e. we can't say "give me all the node ids and their labels".
        """
        # todo: runtime validation of args so we don't send garbage
        if concurrency < 1:
            raise ValueError("concurrency cannot be negative")

        return self._get_chunks({
            "graph_name": self.graph,
            "database_name": self.database,
            "procedure_name": "gds.graph.nodeProperties.stream",
            "configuration": {
                "node_labels": list(labels) or ["*"],
                "node_properties": list(properties) or [],
            },
            "concurrency": concurrency,
        })

    def abort(self, name: Optional[str] = None) -> bool:
        """Try aborting an existing import process."""
        config = {
            "name": name or self.graph,
        }
        try:
            result = self._send_action("ABORT", config)
            if result and result.get("name", None) == config["name"]:
                self.state = ClientState.READY
                return True

            raise error.Neo4jArrowException(f"invalid response for abort of graph {self.graph}, got {result}")
        except error.NotFound as e:
            self.logger.warning(f"no existing import for {config['name']}")
        except Exception as e:
            self.logger.error(f"error aborting {config['name']}: {e}")
        return False

    def wait(self, timeout: int = 0):
        """wait for completion"""
        assert not self.debug or self.state == ClientState.AWAITING_GRAPH
        self.state = ClientState.AWAITING_GRAPH
        # TODO: return future? what do we do?
        pass
