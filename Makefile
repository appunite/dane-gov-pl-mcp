# Makefile for MCP server project

ENV_FILE := ./.env
IMAGE_NAME := dane-gov-pl-mcp
PORT := 8000
FLY_VOLUME_REGION := waw


.PHONY: set-secrets deploy build-docker run-docker run-docker-fresh docker-stop docker-clean



## LOCAL STDIO
stdio:
	@uv run python -m src.app --transport stdio --host 0.0.0.0 --port $(PORT) --debug True



## LOCAL SSE
sse:
	@uv run python -m src.app --transport sse --host 0.0.0.0 --port $(PORT) --debug True



## LOCAL STREAMABLE-HTTP
streamable-http:
	@uv run python -m src.app --transport streamable-http --host 0.0.0.0 --port $(PORT) --debug True



## DOCKER STREAMABLE-HTTP
build-docker:
	@docker build -t $(IMAGE_NAME) .

run-docker:
	@docker run --env-file $(ENV_FILE) -p $(PORT):$(PORT) --rm --name $(IMAGE_NAME) $(IMAGE_NAME)

run-docker-fresh: DOCKER run-local

docker-stop:
	-@docker stop $(IMAGE_NAME) || true
	-@docker rm $(IMAGE_NAME) || true

docker-clean:
	-@docker rmi $(IMAGE_NAME) || true



## FLY DEPLOY
set-secrets:
	@flyctl secrets import < $(ENV_FILE)
	@flyctl secrets list

deploy: set-secrets
	@flyctl deploy



## UTILS UPDATE CATEGORIES
update-categories:
	@uv run python -m src.utils.update_categories

run-%:
	@uv run python -m src.tools.$*