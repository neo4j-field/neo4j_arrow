from setuptools import find_packages, setup

setup(
    name="neo4j_arrow",
    version="0.6.0",

    url="https://github.com/neo4j-field/neo4j_arrow",
    maintainer="Ali Ince",
    maintainer_email="ali.ince@neotechnology.com",
    license="Apache License 2.0",

    install_requires=[
        "pyarrow>=4,<13",
    ],
    packages=find_packages(),
)
