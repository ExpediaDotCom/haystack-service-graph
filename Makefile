.PHONY: all finder builder release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn package

all: clean finder builder report-coverage

report-coverage:
	docker run -it -v ~/.m2:/root/.m2 -w /src -v `pwd`:/src maven:3.5.0-jdk-8 /bin/sh -c 'mvn scoverage:report-only && mvn clean'

finder: build_finder
	cd node-finder && $(MAKE) integration_test

buidler: build_builder
	cd graph-builder && $(MAKE) integration_test

build_finder:
	mvn package -pl node-finder -am

build_builder:
	mvn package -pl graph-builder -am

# build all and release
release: all
	cd node-finder && $(MAKE) release
	cd graph-builder && $(MAKE) release
	./.travis/deploy.sh

