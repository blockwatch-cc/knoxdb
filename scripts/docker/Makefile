.PHONY: default all build

PLATFORM ?= alpine
BUILD_ARTIFACT := go-dst
BUILD_TAG ?= master
BUILD_REPO := blockwatch.cc/go
BUILD_VERSION ?= 1.24.5
BUILD_COMMIT := $(shell git rev-parse --short ${BUILD_TAG})
BUILD_DATE := $(shell date -u "+%Y-%m-%dT%H:%M:%SZ")
BUILD_ID ?= $(shell uuidgen)

ifdef DOCKER_REGISTRY_ADDR
	REGISTRY := $(DOCKER_REGISTRY_ADDR)
endif
REGISTRY ?= hub.bwd.cx

TARGET_IMAGE := $(REGISTRY)/$(BUILD_ARTIFACT):$(BUILD_VERSION)-$(PLATFORM)

default: build

all: build release

build:
	@echo $@
	docker build --ssh default --pull --force-rm --build-arg BUILD_REPO=$(BUILD_REPO) --build-arg BUILD_TAG=$(BUILD_TAG) --build-arg BUILD_ARTIFACT=$(BUILD_ARTIFACT) --build-arg BUILD_DATE=$(BUILD_DATE) --build-arg BUILD_VERSION=$(BUILD_VERSION) --build-arg PLATFORM=$(PLATFORM) --build-arg BUILD_COMMIT=$(BUILD_COMMIT) --build-arg BUILD_ID=$(BUILD_ID) --build-arg BUILDKIT_CONTEXT_KEEP_GIT_DIR=1 -t $(TARGET_IMAGE) -f Dockerfile.$(PLATFORM) .

release: build
	@echo $@
	@echo "Publishing image..."
	docker push $(TARGET_IMAGE)
