PROFILE := dev
REGION := us-east-1
BUILD := 0.0.1
NAME := $(shell aws ecr describe-repositories --region ${REGION} --profile ${PROFILE} | jq -r ".repositories[] | select ( .repositoryName == \"buzzard\") | .repositoryUri")
VERSION := $(shell echo `date "+%Y%m%d"`-${BUILD})
ECR_AUTH := $(shell aws --region ${REGION} ecr get-login --profile ${PROFILE} --no-include-email)

.PHONY: clean build publish

all: clean build

clean:
	@echo "Cleaning up untagged images"
	$(eval dangling := $(shell docker images -f dangling=true -q))
	-docker rmi $(dangling)

build:
	mkdir -p build
	docker build -f Dockerfile \
				 -t ${NAME}:${VERSION} \
				 --build-arg BUILD_VERSION=${VERSION} \
				 --build-arg GEOS_VERSION=3.6.2 \
				 --build-arg GDAL_VERSION=2.2.2 \
				 --build-arg PROJ4_VERSION=4.9.3 \
				 .
	@echo built ${NAME}:${VERSION}
	@printf ${NAME}:${VERSION} > ./build/tagname

publish: build
	@echo "Authentication on AWS ECR"
	${ECR_AUTH}
	docker push ${NAME}:${VERSION}
