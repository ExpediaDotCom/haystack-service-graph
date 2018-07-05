.PHONY: all clean build report-coverage build-node-finder build-graph-builder release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn package

build-node-finder:
	mvn package -DfinalName=haystack-service-graph-node-finder -pl node-finder -am

build-graph-builder:
	mvn package -DfinalName=haystack-service-graph-graph-builder -pl graph-builder -am

all: clean build-node-finder build-graph-builder

# build all and release
release: clean build-node-finder build-graph-builder
	cd node-finder && $(MAKE) release
	cd graph-builder && $(MAKE) release

# run coverage tests
report-coverage:
	mvn clean scoverage:test scoverage:report-only
	open target/site/scoverage/index.html