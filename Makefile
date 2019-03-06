# Cross compilation
GOCMD=go
GOBUILD=$(GOCMD) build

all: build-linux docker
build-linux:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o es es.go
docker:
	docker build -t $(DOCKER_REPO)/$(DOCKER_IMAGE_NAME) .
	docker push $(DOCKER_REPO)/$(DOCKER_IMAGE_NAME)
