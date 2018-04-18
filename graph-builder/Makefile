.PHONY: integration_test release

export DOCKER_ORG := expediadotcom
export DOCKER_IMAGE_NAME := haystack-service-graph-builder
PWD := $(shell pwd)

all:
	mvn clean verify

docker-image: all
	docker build -t $(DOCKER_IMAGE_NAME) -f build/docker/Dockerfile .

release: docker-image
	../.travis/publish-to-docker-hub.sh
