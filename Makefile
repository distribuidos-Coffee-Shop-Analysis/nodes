SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client
.PHONY: build

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up:
	docker compose up --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose down -v
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

# -----------------------------
# Chaos Monkey
# -----------------------------
CHAOS_CONTAINER_NAME := chaos-monkey
CHAOS_IMAGE_NAME := chaos-monkey:latest
NETWORK_NAME := connection-node_coffee_net

build-chaos:
	docker build -f Dockerfile.chaos -t $(CHAOS_IMAGE_NAME) .
.PHONY: build-chaos

# Ejecuta el chaos monkey en un contenedor
# Uso: make run-chaos [CHAOS_ARGS="--limit 5 --graceful-prob 0.3"]
run-chaos: build-chaos
	@echo "Empezando el Chaos Distribuido..."
	@docker rm -f $(CHAOS_CONTAINER_NAME) 2>/dev/null || true
	docker run \
		--name $(CHAOS_CONTAINER_NAME) \
		--network $(NETWORK_NAME) \
		-v /var/run/docker.sock:/var/run/docker.sock:ro \
		-v $(PWD)/docker-compose.yml:/app/docker-compose.yml:ro \
		$(CHAOS_IMAGE_NAME) \
		--compose docker-compose.yml \
		--exclude "$(CHAOS_CONTAINER_NAME)" \
		--exclude "rabbitmq" \
		$(CHAOS_ARGS)
.PHONY: run-chaos

# Detiene el chaos monkey
stop-chaos:
	@echo "Frenando el Chaos Distribuido..."
	@docker stop $(CHAOS_CONTAINER_NAME) 2>/dev/null || echo "Chaos Monkey not running"
	@docker rm -f $(CHAOS_CONTAINER_NAME) 2>/dev/null || true
.PHONY: stop-chaos

# Ver logs del chaos monkey
chaos-logs:
	docker logs -f $(CHAOS_CONTAINER_NAME)
.PHONY: chaos-logs

# Chaos enfocado en nodos stateful (Aggregates, Joiners y Coordinators)
run-chaos-stateful: build-chaos
	@echo "Empezando el Chaos Distribuido (nodos stateful solo: aggregates, joiners & coordinators)..."
	@docker rm -f $(CHAOS_CONTAINER_NAME) 2>/dev/null || true
	docker run \
		--name $(CHAOS_CONTAINER_NAME) \
		--network $(NETWORK_NAME) \
		-v /var/run/docker.sock:/var/run/docker.sock:ro \
		-v $(PWD)/docker-compose.yml:/app/docker-compose.yml:ro \
		$(CHAOS_IMAGE_NAME) \
		--compose docker-compose.yml \
		--include "aggregate|joiner|coordinator" \
		--exclude "$(CHAOS_CONTAINER_NAME)" \
		$(CHAOS_ARGS)
	@echo "Chaos Distribuido (nodos stateful) empezado. Para ver logs: make chaos-logs"
.PHONY: run-chaos-stateful

list-chaos-stateful: build-chaos
	@docker run --rm \
		-v $(PWD)/docker-compose.yml:/app/docker-compose.yml:ro \
		$(CHAOS_IMAGE_NAME) \
		--compose docker-compose.yml \
		--include "aggregate|joiner" \
		--print-targets
.PHONY: list-chaos-stateful

clean:
	@rm -rf state/*
	@echo "State directory cleaned successfully"
.PHONY: clean
