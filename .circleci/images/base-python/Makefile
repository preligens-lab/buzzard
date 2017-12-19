PROFILE := dev
REGION := us-east-1
BUILD := 0.0.2
NAME := $(shell aws ecr describe-repositories --region ${REGION} --profile ${PROFILE} | jq -r ".repositories[] | select ( .repositoryName == \"buzzard\") | .repositoryUri")
VERSION := $(shell echo `date "+%Y%m%d"`-${BUILD})
PY36_VERSION := 3.6.3
PY27_VERSION := 2.7.14
ECR_AUTH := $(shell aws --region ${REGION} ecr get-login --profile ${PROFILE} --no-include-email)

.PHONY: clean build-py36 build-py27 publish

all: clean build-py36 build-py27

clean:
	@echo "Cleaning up untagged images"
	$(eval dangling := $(shell docker images -f dangling=true -q))
	-docker rmi $(dangling)

build-py36:
	mkdir -p build
	docker build -f Dockerfile \
				 -t ${NAME}:${PY36_VERSION}-${VERSION} \
				 --build-arg PYTHON_VERSION=${PY36_VERSION} \
				 --build-arg BUILD_VERSION=${VERSION} \
				 --build-arg GEOS_VERSION=3.6.2 \
				 --build-arg GDAL_VERSION=2.2.2 \
				 --build-arg PROJ4_VERSION=4.9.3 \
				 .
	@echo built ${NAME}:${PY36_VERSION}-${VERSION}
	@printf ${NAME}:${PY36_VERSION}-${VERSION} > ./build/tagname-${PY36_VERSION}

build-py27:
	mkdir -p build
	docker build -f Dockerfile \
				 -t ${NAME}:${PY27_VERSION}-${VERSION} \
				 --build-arg PYTHON_VERSION=${PY27_VERSION} \
				 --build-arg BUILD_VERSION=${VERSION} \
				 --build-arg GEOS_VERSION=3.6.2 \
				 --build-arg GDAL_VERSION=2.2.2 \
				 --build-arg PROJ4_VERSION=4.9.3 \
				 .
	@echo built ${NAME}:${PY27_VERSION}-${VERSION}
	@printf ${NAME}:${PY27_VERSION}-${VERSION} > ./build/tagname-${PY27_VERSION}

publish-py27: build-py27
	@echo "Authentication on AWS ECR"
	${ECR_AUTH}
	docker push $(shell cat ./build/tagname-${PY27_VERSION})

publish-py36: build-py36
	@echo "Authentication on AWS ECR"
	${ECR_AUTH}
	docker push $(shell cat ./build/tagname-${PY36_VERSION})

publish: publish-py27 publish-py36

