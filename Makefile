# Define variables
IMAGE_NAME = scalable-challenge
CONTAINER_NAME = container
DOCKERFILE_PATH = .

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
	docker rmi $(IMAGE_NAME)
