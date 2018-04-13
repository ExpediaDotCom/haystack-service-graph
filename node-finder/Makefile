.PHONY: integration_test release

export DOCKER_ORG := expediadotcom
export DOCKER_IMAGE_NAME := haystack-service-graph-node-finder
PWD := $(shell pwd)

docker-image: all
	docker build -t $(DOCKER_IMAGE_NAME) -f build/docker/Dockerfile .

all:
	mvn clean verify

release: docker-image
	../scripts/publish-to-docker-hub.sh
