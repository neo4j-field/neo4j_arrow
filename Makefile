SHELL = /bin/sh
VERSION = 0.3.0

.PHONY: build build-py37 build-py38 build-py39
.PHONE: test test-py37 test-py38 test-py39 clean

all: test

build-py37:
	docker build -t "neo4j_arrow:${VERSION}-py37" .

build-py38:
	docker build -t "neo4j_arrow:${VERSION}-py38" .

build-py39:
	docker build -t "neo4j_arrow:${VERSION}-py39" .

build: build-py37 build-py38 build-py39

test-py37: build-py37
	docker run --rm "neo4j_arrow:${VERSION}-py37"

test-py38: build-py38
	docker run --rm "neo4j_arrow:${VERSION}-py38"

test-py39: build-py39
	docker run --rm "neo4j_arrow:${VERSION}-py39"

test: test-py37 test-py38 test-py39

clean:
	echo nop
