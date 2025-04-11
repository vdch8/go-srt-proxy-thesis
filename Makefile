BINARY_NAME=proxy
CONFIG_FILE=config.yaml

GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) get
GOMODTIDY=$(GOCMD) mod tidy

SRC_FILES=$(wildcard *.go)

.DEFAULT_GOAL := build

build: $(SRC_FILES) go.mod
	@echo "Building $(BINARY_NAME)..."
	$(GOBUILD) -v -o $(BINARY_NAME) $(SRC_FILES)
	@echo "$(BINARY_NAME) built successfully."

run: build
	@echo "Running $(BINARY_NAME) with config: $(CONFIG_FILE)..."
	./$(BINARY_NAME) -config $(CONFIG_FILE)

run-config: build
	@echo "Running $(BINARY_NAME) with config: $(CONFIG)..."
	./$(BINARY_NAME) -config $(CONFIG)

clean:
	@echo "Cleaning up..."
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	@echo "Cleanup finished."

tidy: go.mod go.sum
	@echo "Tidying module dependencies..."
	$(GOMODTIDY)
	@echo "Dependencies tidied."

.PHONY: all build run run-config clean tidy deps help