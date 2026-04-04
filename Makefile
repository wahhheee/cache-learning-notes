# --- Project Configuration ---
PROJECT_NAME := cache
BINARY_NAME  := cache
BUILD_DIR    := bin
GO_FILES     := $(shell find . -name '*.go' -not -path "./vendor/*")
GO_VERSION   := $(shell go version | awk '{print $$3}')

# --- Visuals ---
BOLD   := \033[1m
RED    := \033[31m
GREEN  := \033[32m
YELLOW := \033[33m
BLUE   := \033[34m
MAGENTA:= \033[35m
CYAN   := \033[36m
WHITE  := \033[37m
RESET  := \033[0m

ICON_GO    := 🐹
ICON_BUILD := 🔨
ICON_RUN   := 🚀
ICON_TEST  := 🧪
ICON_LINT  := 🔍
ICON_CLEAN := 🗑️
ICON_OK    := ✔️
ICON_ERR   := ❌

# --- Targets ---

.PHONY: all
all: build

## @ Build the application binary
.PHONY: build
build: $(GO_FILES)
	@printf "$(CYAN)$(ICON_BUILD)  Building $(BOLD)$(BINARY_NAME)$(RESET) ... "
	@mkdir -p $(BUILD_DIR)
	@if go build -trimpath -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/cache; then \
		printf "$(GREEN)$(ICON_OK) Success!$(RESET)\n"; \
	else \
		printf "$(RED)$(ICON_ERR) Failed!$(RESET)\n"; exit 1; \
	fi

## @ Run the application
.PHONY: run
run:
	@printf "$(BLUE)$(ICON_RUN)  Running $(BINARY_NAME)...$(RESET)\n"
	@go run ./cmd/$(PROJECT_NAME)

## @ Run tests with race detection
.PHONY: test
test:
	@printf "$(MAGENTA)$(ICON_TEST)  Running tests...$(RESET)\n"
	@go test -v -race ./...

## @ Lint code using staticcheck/golangci-lint
.PHONY: lint
lint:
	@printf "$(YELLOW)$(ICON_LINT)  Linting code...$(RESET)\n"
	@if command -v golangci-lint >/dev/null; then \
		golangci-lint run && printf "$(GREEN)$(ICON_OK) Code looks good!$(RESET)\n"; \
	else \
		printf "$(YELLOW) golangci-lint not found, trying 'go vet'...\n"; \
		go vet ./...; \
	fi

## @ Clean build artifacts
.PHONY: clean
clean:
	@printf "$(RED)$(ICON_CLEAN)  Cleaning up...$(RESET) "
	@rm -rf $(BUILD_DIR)
	@go clean
	@printf "$(GREEN)Done.$(RESET)\n"

## @ Update dependencies
.PHONY: deps
deps:
	@printf "$(BLUE)📦  Updating dependencies...$(RESET)\n"
	@go mod tidy

## @ Show this help message
.PHONY: help
help:
	@echo ""
	@printf "$(BOLD)$(MAGENTA)  $(PROJECT_NAME)$(RESET)  $(WHITE)|  $(GO_VERSION)$(RESET)\n"
	@echo "  ------------------------------------------"
	@echo ""
	@printf "$(BOLD)  Available Targets:$(RESET)\n"
	@echo ""
	@awk '/^## @/ { doc=substr($$0, 6) } 	      /^[a-zA-Z0-9_-]+:/ && doc { 	        cmd=substr($$1, 1, index($$1, ":")-1); 	        printf "  $(CYAN)%-15s$(RESET) %s\n", cmd, doc; 	        doc="" 	      }' $(MAKEFILE_LIST)
	@echo ""
