.PHONY: integration_test release

export DOCKER_ORG := expediadotcom
export DOCKER_IMAGE_NAME := haystack-service-graph-node-finder
PWD := $(shell pwd)

docker_build:
	docker build -t $(DOCKER_IMAGE_NAME) -f build/docker/Dockerfile .

prepare_integration_test_env: docker_build
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox up -d
	sleep 10

integration_test: 

release:
	../scripts/publish-to-docker-hub.sh
