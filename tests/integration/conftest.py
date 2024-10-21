import os
from typing import Callable

import neo4j
import pytest
import testcontainers.neo4j

import neo4j_arrow._client
from neo4j_arrow import Neo4jArrowClient


def gds_version(driver: neo4j.Driver) -> str:
    with driver.session() as session:
        version = session.run(
            "CALL gds.debug.sysInfo() YIELD key, value WITH * WHERE key = $key RETURN value", {"key": "gdsVersion"}
        ).single(strict=True)[0]
        return version


@pytest.fixture(scope="module")
def neo4j():
    testcontainers.neo4j.Neo4jContainer.NEO4J_USER = "neo4j"
    testcontainers.neo4j.Neo4jContainer.NEO4J_ADMIN_PASSWORD = "password"

    container = (
        testcontainers.neo4j.Neo4jContainer(os.getenv("NEO4J_IMAGE", "neo4j:5-enterprise"))
        .with_volume_mapping(os.getenv("GDS_LICENSE_FILE", "/tmp/gds.license"), "/licenses/gds.license")
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_PLUGINS", '["graph-data-science"]')
        .with_env("NEO4J_gds_enterprise_license__file", "/licenses/gds.license")
        .with_env("NEO4J_dbms_security_procedures_unrestricted", "gds.*")
        .with_env("NEO4J_dbms_security_procedures_allowlist", "gds.*")
        .with_env("NEO4J_gds_arrow_enabled", "true")
        .with_env("NEO4J_gds_arrow_listen__address", "0.0.0.0")
        .with_exposed_ports(7687, 7474, 8491)
    )
    container.NEO4J_USER = "neo4j"
    container.NEO4J_ADMIN_PASSWORD = "password"
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="module")
def driver(neo4j):
    driver = neo4j.get_driver()

    yield driver

    driver.close()


@pytest.fixture(scope="module")
def arrow_client_factory(neo4j, driver) -> Callable[[str], Neo4jArrowClient]:
    def _arrow_client_factory(graph_name: str) -> Neo4jArrowClient:
        return Neo4jArrowClient(
            neo4j.get_container_host_ip(),
            graph=graph_name,
            user=neo4j.NEO4J_USER,
            password=neo4j.NEO4J_ADMIN_PASSWORD,
            port=int(neo4j.get_exposed_port(8491)),
            tls=False,
            proc_names=neo4j_arrow._client.procedure_names(gds_version(driver)),
        )

    return _arrow_client_factory


@pytest.fixture(autouse=True)
def setup(driver, arrow_client_factory):
    with driver.session() as session:
        session.run("CREATE OR REPLACE DATABASE neo4j WAIT").consume()

    yield
