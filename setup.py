from setuptools import find_packages, setup

setup(
    name="neo4j_arrow",
    version="0.3.0",

    url="https://github.com/neo4j-field/neo4j_arrow",
    maintainer="Dave Voutila",
    maintainer_email="dave.voutila@neotechnology.com",
    license="Apache License 2.0",

    install_requires=[
        "pyarrow>=4,<10",
    ],
    packages=find_packages(),
)
