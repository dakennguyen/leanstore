# Docker names
CONTAINER_NAME = tum-leanstore
IMAGE_NAME = tum-leanstore
HOST_NAME = host

# Volume directory
VOLUME_DIR = $(PWD)
DOCKERFILE_DIR = $(PWD)

run: build
	@if [ -z "$$(docker ps -a -q -f name=$(CONTAINER_NAME))" ]; then \
		docker run --name $(CONTAINER_NAME) -it --privileged --network=$(HOST_NAME) -v $(VOLUME_DIR):/root/leanstore $(IMAGE_NAME) bash; \
	else \
		docker start $(CONTAINER_NAME); \
		docker attach $(CONTAINER_NAME); \
	fi

build:
	docker build -t $(IMAGE_NAME) $(DOCKERFILE_DIR)

clean:
	docker rm $(CONTAINER_NAME)
	docker rmi $(IMAGE_NAME)

.PHONY: run build clean
