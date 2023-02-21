SHELL = /bin/sh
VERSION = 0.4.0
# TODO: read VERSION from setup.py

.PHONY: build build-py37 build-py38 build-py39
.PHONY: test test-py37 test-py38 test-py39
.PHONY: clean clean-py37 clean-py38 clean-py39

all: test

build-py37:
	@docker build -t "neo4j_arrow:${VERSION}-py37" .

build-py38:
	@docker build -t "neo4j_arrow:${VERSION}-py38" .

build-py39:
	@docker build -t "neo4j_arrow:${VERSION}-py39" .

build: build-py37 build-py38 build-py39

test-py37: build-py37
	docker run --rm "neo4j_arrow:${VERSION}-py37"

test-py38: build-py38
	@docker run --rm "neo4j_arrow:${VERSION}-py38"

test-py39: build-py39
	@docker run --rm "neo4j_arrow:${VERSION}-py39"

test: test-py37 test-py38 test-py39

clean-py37:
	@docker image rm -f "neo4j_arrow:${VERSION}-py37" 2>/dev/null

clean-py38:
	@docker image rm -f "neo4j_arrow:${VERSION}-py38" 2>/dev/null

clean-py39:
	@docker image rm -f "neo4j_arrow:${VERSION}-py39" 2>/dev/null

clean: clean-py37 clean-py38 clean-py39
