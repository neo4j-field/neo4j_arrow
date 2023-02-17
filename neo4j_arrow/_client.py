from collections import abc
from enum import Enum
import logging as log
import json

import pyarrow as pa
import pyarrow.lib as lib
import pyarrow.flight as flight

from . import error
from .model import Graph, Node, Edge

from typing import cast, Any, Dict, Iterable, Optional, Union, Tuple


Result = Tuple[int, int]
Arrow = Union[pa.Table, pa.RecordBatch]
Nodes = Union[pa.Table, pa.RecordBatch, Iterable[pa.RecordBatch]]
Edges = Union[pa.Table, pa.RecordBatch, Iterable[pa.RecordBatch]]


class ClientState(Enum):
    READY = "ready"
    FEEDING_NODES = "feeding_nodes"
    FEEDING_EDGES = "feeding_edges"
    AWAITING_GRAPH = "awaiting_graph"
    GRAPH_READY = "done"


class Neo4jArrowClient:
    def __init__(self, host: str, graph: str, *, port: int=8491, debug = False,
                 user: str = "neo4j", password: str = "neo4j", tls: bool = True,
                 concurrency: int = 4, database: str = "neo4j"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.tls = tls
        self.client: flight.FlightClient = None
        self.call_opts = None
        self.graph = graph
        self.database = database
        self.concurrency = concurrency
        self.state = ClientState.READY
        self.debug = debug

    def __str__(self):
        return f"Neo4jArrowClient{{{self.user}@{self.host}:{self.port}" \
            f"/{self.graph}}}"

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the FlightClient and CallOpts as they're not serializable
        if "client" in state:
            del state["client"]
        if "call_opts" in state:
            del state["call_opts"]
        return state

    def copy(self) -> 'Neo4jArrowClient':
        client = Neo4jArrowClient(self.host, port=self.port, user=self.user,
                                  password=self.password, graph=self.graph,
                                  tls=self.tls, concurrency=self.concurrency,
                                  database=self.database)
        client.state = self.state
        return client

    def _client(self) -> flight.FlightClient:
        """Lazy client construction to help pickle this class."""
        if not hasattr(self, "client") or not self.client:
            self.call_opts = None
            if self.tls:
                location = flight.Location.for_grpc_tls(self.host, self.port)
            else:
                location = flight.Location.for_grpc_tcp(self.host, self.port)
            client = flight.FlightClient(location)
            if self.user and self.password:
                (header, token) = client.authenticate_basic_token(self.user,
                                                                  self.password)
                if header:
                    self.call_opts = flight.FlightCallOptions(
                        headers=[(header, token)]
                    )
            self.client = client
        return self.client

    def _send_action(self, action: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Communicates an Arrow Action message to the GDS Arrow Service.
        """
        client = self._client()
        try:
            payload = json.dumps(body).encode("utf-8")
            result = client.do_action(
                flight.Action(action, payload),
                options=self.call_opts
            )
            obj = json.loads(next(result).body.to_pybytes().decode())
            return dict(obj)
        except lib.ArrowException as e:
            raise error.interpret(e)
        except Exception as e:
            log.error(f"send_action error: {e}")
            raise e

    @classmethod
    def _nop(cls, data: Arrow) -> Arrow:
        """Used as a no-op mapping function."""
        return data

    @classmethod
    def _node_mapper(cls, model: Graph, source_field: Optional[str] = None):
        """Generate a mapping function for a Node"""
        def _map(data: Arrow) -> Arrow:
            schema = data.schema
            if source_field:
                src = schema.metadata.get(source_field.encode("utf8"))
                node = model.node_for_src(src.decode("utf8"))
            else: # guess at labels
                my_label = data["labels"][0].as_py()
                node = model.node_by_label(my_label)
            if not node:
                raise Exception("cannot find matching node in model given "
                                f"{data.schema}")

            has_props = len(node.properties) > 0

            for idx, name in enumerate(data.schema.names):
                field = schema.field(name)
                if name in node.key_field:
                    schema = schema.set(idx, field.with_name("nodeId"))
                elif name in node.label_field:
                    schema = schema.set(idx, field.with_name("labels"))
                elif has_props and name in node.properties:
                    schema = schema.set(idx,
                                        field.with_name(node.properties[name]))
            return data.from_arrays(data.columns, schema=schema)
        return _map

    @classmethod
    def _edge_mapper(cls, model: Graph, source_field: Optional[str] = None):
        """Generate a mapping function for an Edge."""
        def _map(data: Arrow) -> Arrow:
            schema = data.schema
            if source_field:
                src = schema.metadata.get(source_field.encode("utf8"))
                edge = model.edge_for_src(src.decode("utf8"))
            else: # guess at type
                my_type = data["type"][0].as_py()
                edge = model.edge_by_type(my_type)
            if not edge:
                raise Exception("cannot find matching edge in model given "
                                f"{data.schema}")

            has_props = len(edge.properties) > 0

            for idx, name in enumerate(data.schema.names):
                f = schema.field(name)
                if name == edge.source_field:
                    schema = schema.set(idx, f.with_name("sourceNodeId"))
                elif name == edge.target_field:
                    schema = schema.set(idx, f.with_name("targetNodeId"))
                elif name == edge.type_field:
                    schema = schema.set(idx, f.with_name("relationshipType"))
                elif has_props and name in edge.properties:
                    schema = schema.set(idx, f.with_name(edge.properties[name]))
            return data.from_arrays(data.columns, schema=schema)
        return _map

    def _write_batches(self, desc: Dict[str, Any],
                       batches: Union[pa.RecordBatch, Iterable[pa.RecordBatch]],
                       mappingfn = None) -> Result:
        """
        Write PyArrow RecordBatches to the GDS Flight service.
        """
        if isinstance(batches, abc.Iterable):
            batches = iter(batches)
        else:
            batches = iter([batches])

        fn = mappingfn or self._nop

        first = next(batches, None)
        if not first:
            raise Exception("empty iterable of record batches provided")
        first = cast(pa.RecordBatch, fn(first))

        client = self._client()
        upload_descriptor = flight.FlightDescriptor.for_command(
            json.dumps(desc).encode("utf-8")
        )
        rows, nbytes = 0, 0
        writer, _ = client.do_put(upload_descriptor, first.schema,
                                  options=self.call_opts)
        with writer:
            try:
                writer.write_batch(first)
                rows += first.num_rows
                nbytes += first.get_total_buffer_size()
                for remaining in batches:
                    writer.write_batch(fn(remaining))
                    rows += remaining.num_rows
                    nbytes += remaining.get_total_buffer_size()
            except Exception as e:
                log.error(f"_write_batches error: {e}")
        return rows, nbytes

    def start(self, action: str = "CREATE_GRAPH", *,
              config: Dict[str, Any] = {},
              force: bool = False) -> Dict[str, Any]:
        assert not self.debug or self.state == ClientState.READY
        if not config:
            config = {
                "name": self.graph,
                "database_name": self.database,
                "concurrency": self.concurrency,
            }
        # TODO: assert config has mandatory fields
        try:
            result = self._send_action(action, config)
            if result and result.get("name", None) == config["name"]:
                self.state = ClientState.FEEDING_NODES
            log.error(f"failed to start {action} for {config['name']},"
                      f" got {result}")
        except error.AlreadyExists as e:
            if force:
                log.warn(f"forcing cancellation of existing {action} import"
                         f" for {config['name']}")
            if self.abort():
                return self.start(action, config=config)
            log.warn(f"{action} import job already exists for {config['name']}")
        except Exception as e:
            log.error(f"fatal error performing action {action}: {e}")
            raise e
        return {}

    def write_nodes(self, nodes: Nodes,
                    model: Optional[Graph] = None,
                    source_field: Optional[str] = None) -> Result:
        assert not self.debug or self.state == ClientState.FEEDING_NODES
        desc = { "name": self.graph, "entity_type": "node" }
        if model:
            mapper = self._node_mapper(model, source_field)
        else:
            mapper = self._nop
        if isinstance(nodes, pa.Table):
            # TODO: max_chunksize on to_batches()
            return self._write_batches(desc, nodes.to_batches(), mapper)
        return self._write_batches(desc, nodes, mapper)

    def nodes_done(self) -> Dict[str, Any]:
        assert not self.debug or self.state == ClientState.FEEDING_NODES
        result = self._send_action("NODE_LOAD_DONE", { "name": self.graph })
        if result:
            self.state = ClientState.FEEDING_EDGES
        return result

    def write_edges(self, edges: Edges,
                    model: Optional[Graph] = None,
                    source_field: Optional[str] = None) -> Result:
        assert not self.debug or self.state == ClientState.FEEDING_EDGES
        desc = { "name": self.graph, "entity_type": "relationship" }
        if model:
            mapper = self._edge_mapper(model, source_field)
        else:
            mapper = self._nop
        if isinstance(edges, pa.Table):
            # TODO: max_chunksize on to_batches()
            return self._write_batches(desc, edges.to_batches(), mapper)
        return self._write_batches(desc, edges, mapper)

    def edges_done(self) -> Dict[str, Any]:
        assert not self.debug or self.state == ClientState.FEEDING_EDGES
        result = self._send_action("RELATIONSHIP_LOAD_DONE",
                                   { "name": self.graph })
        if result:
            self.state = ClientState.AWAITING_GRAPH
        return result

    def read_edges(self, prop: str, *, concurrency: int = 4):
        ticket = {
            "graph_name": self.graph, "database_name": self.database,
            "procedure_name": "gds.graph.streamRelationshipProperty",
            "configuration": {
                "relationship_types": "*",
                "relationship_property": prop,
            },
            "concurrency": concurrency,
        }

        client = self._client()
        result = client.do_get(
            pa.flight.Ticket(json.dumps(ticket).encode("utf8")),
            options=self.call_opts
        )
        for chunk, _ in result:
            yield chunk

    def read_nodes(self, prop: str, *, concurrency: int = 4):
        ticket = {
            "graph_name": self.graph, "database_name": self.database,
            "procedure_name": "gds.graph.streamNodeProperty",
            "configuration": {
                "node_labels": "*",
                "node_property": prop,
            },
            "concurrency": concurrency,
        }

        client = self._client()
        result = client.do_get(
            pa.flight.Ticket(json.dumps(ticket).encode("utf8")),
            options=self.call_opts
        )
        for chunk, _ in result:
            yield chunk

    def abort(self, name: Optional[str] = None) -> bool:
        """Try aborting an existing import process."""
        config = {
            "name": name or self.graph,
        }
        try:
            result = self._send_action("ABORT", config)
            if result and result.get("name", None) == config["name"]:
                return True
            log.error(f"failed to abort {config['name']}, got {result}")
        except error.NotFound as e:
            log.warn(f"no existing import for {config['name']}")
        except Exception as e:
            log.error(f"error aborting {config['name']}: {e}")
        return False

    def wait(self, timeout: int = 0):
        """wait for completion"""
        assert not self.debug or self.state == ClientState.AWAITING_GRAPH
        self.state = ClientState.AWAITING_GRAPH
        # TODO: return future? what do we do?
        pass
