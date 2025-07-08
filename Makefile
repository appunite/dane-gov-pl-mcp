# Makefile for MCP server project

ENV_FILE := ./.env
IMAGE_NAME := dane-gov-pl-mcp
PORT := 8000
FLY_VOLUME_REGION := waw


.PHONY: set-secrets deploy build-docker run-docker run-docker-fresh docker-stop docker-clean



## LOCAL STDIO
stdio:
	@uv run python src/app.py --transport stdio --host 0.0.0.0 --port $(PORT)



## LOCAL SSE
sse:
	@uv run python src/app.py --transport sse --host 0.0.0.0 --port $(PORT)



## LOCAL STREAMABLE-HTTP
streamable-http:
	@uv run python src/app.py --transport streamable-http --host 0.0.0.0 --port $(PORT)



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