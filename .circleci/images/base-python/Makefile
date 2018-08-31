PROFILE := dev
REGION := us-east-1
NAME := $(shell (aws ecr describe-repositories --region ${REGION} --profile ${PROFILE} || echo "{\"repositories\":[{\"repositoryName\":\"buzzard\", \"repositoryUri\":\"local-repository\"}]}") | jq -r ".repositories[] | select ( .repositoryName == \"buzzard\") | .repositoryUri")
VERSION := $(shell echo `date "+%Y%m%d"`)
ECR_AUTH := $(shell aws --region ${REGION} ecr get-login --profile ${PROFILE} --no-include-email)
PY27_VERSION := 2.7.15
PY34_VERSION := 3.4.9
PY36_VERSION := 3.6.6
PY37_VERSION := 3.7.0

.PHONY: clean build-py37 build-py36 build-py34 build-py27 publish

all: publish

clean:
	@echo "--> Cleaning up untagged images"
	$(eval dangling := $(shell docker images -f dangling=true -q))
	-docker rmi $(dangling)

build-py27:
	@echo "--> Building docker image"
	mkdir -p build
	docker build -f Dockerfile \
				 -t ${NAME}:${PY27_VERSION}-${VERSION} \
				 --build-arg PYTHON_VERSION=${PY27_VERSION} \
				 .
	@echo built ${NAME}:${PY27_VERSION}-${VERSION}
	@printf ${NAME}:${PY27_VERSION}-${VERSION} > ./build/tagname-${PY27_VERSION}

build-py34:
	@echo "--> Building docker image"
	mkdir -p build
	docker build -f Dockerfile \
				 -t ${NAME}:${PY34_VERSION}-${VERSION} \
				 --build-arg PYTHON_VERSION=${PY34_VERSION} \
				 .
	@echo built ${NAME}:${PY34_VERSION}-${VERSION}
	@printf ${NAME}:${PY34_VERSION}-${VERSION} > ./build/tagname-${PY34_VERSION}

build-py36:
	@echo "--> Building docker image"
	mkdir -p build
	docker build -f Dockerfile \
				 -t ${NAME}:${PY36_VERSION}-${VERSION} \
				 --build-arg PYTHON_VERSION=${PY36_VERSION} \
				 .
	@echo built ${NAME}:${PY36_VERSION}-${VERSION}
	@printf ${NAME}:${PY36_VERSION}-${VERSION} > ./build/tagname-${PY36_VERSION}

build-py37:
	@echo "--> Building docker image"
	mkdir -p build
	docker build -f Dockerfile \
				 -t ${NAME}:${PY37_VERSION}-${VERSION} \
				 --build-arg PYTHON_VERSION=${PY37_VERSION} \
				 .
	@echo built ${NAME}:${PY37_VERSION}-${VERSION}
	@printf ${NAME}:${PY37_VERSION}-${VERSION} > ./build/tagname-${PY37_VERSION}

publish-py27: build-py27
	@echo "--> Authentication on AWS ECR"
	${ECR_AUTH}
	docker push $(shell cat ./build/tagname-${PY27_VERSION})

publish-py34: build-py34
	@echo "--> Authentication on AWS ECR"
	${ECR_AUTH}
	docker push $(shell cat ./build/tagname-${PY34_VERSION})

publish-py36: build-py36
	@echo "--> Authentication on AWS ECR"
	${ECR_AUTH}
	docker push $(shell cat ./build/tagname-${PY36_VERSION})

publish-py37: build-py37
	@echo "--> Authentication on AWS ECR"
	${ECR_AUTH}
	docker push $(shell cat ./build/tagname-${PY37_VERSION})

publish: publish-py27 publish-py34 publish-py36 publish-py37
