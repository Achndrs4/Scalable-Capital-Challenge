# Define variables
IMAGE_NAME = scalable-challenge
CONTAINER_NAME = container
DOCKERFILE_PATH = .
LINTER := flake8
VENV_DIR := .venv
LINTER_PATH := $(VENV_DIR)/bin/$(LINTER)

# Build the Docker image
build:
	docker build -t $(IMAGE_NAME) $(DOCKERFILE_PATH)

# Run the Docker container
run: build
	docker run --name $(CONTAINER_NAME) -d $(IMAGE_NAME)

# Stop and remove the Docker container
shutdown:
	docker stop $(CONTAINER_NAME)
	docker rm $(CONTAINER_NAME)

# Clean up the Docker image
clean: shutdown
	docker rmi $(IMAGE_NAME) && docker rm test.db
# Install the linter (using a virtual environment for isolation)

.PHONY: install-linter
install-linter:
	@echo "Creating virtual environment and installing linter..."
	python3 -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install --upgrade pip
	$(VENV_DIR)/bin/pip install $(LINTER)

# Run the linter
.PHONY: lint
lint: $(LINTER_PATH)
	@echo "Running linter..."
	$(LINTER_PATH) .

# Ensure linter is installed before running
$(LINTER_PATH):
	@$(MAKE) install-linter

# Clean up the virtual environment
.PHONY: clean
clean:
	@echo "Cleaning up..."
	rm -rf $(VENV_DIR)