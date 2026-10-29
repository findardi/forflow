-include .env
export

GOOSE_DRIVER ?= postgres
GOOSE_DBSTRING ?= $(DATABASE_URL)
GOOSE_MIGRATION_DIR ?= migrations
export GOOSE_DRIVER GOOSE_DBSTRING GOOSE_MIGRATION_DIR

COMPOSE := docker compose -f deploy/compose.yaml

.PHONY: dev down migrate generate test test-integration lint run-api run-worker

dev:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

migrate:
	goose up

generate:
	sqlc generate

test:
	go test -race ./...

test-integration:
	go test -race -tags=integration ./...

lint:
	golangci-lint run

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
