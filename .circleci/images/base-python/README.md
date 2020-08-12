# docker image workflow for buzzard's CI

In the CI we run a test workflow for each supported python version. We could also test several GDAL versions in the future.

In [../../config.yml](../../config.yml), each tested python version is associated to a docker image with a unique identifier hosted by docker-hub. Example of such an identifier: `buzzardpython/ci:py37@sha256:790b3bfdc803d4a5a4c31e88d6d0a6ff222a7be56e80ce272e02dfd5e2e7bdfd`.

If you want to support a new python version: create and push a new image to docker hub, and add a new test workflow to [../../config.yml](../../config.yml).

If you want to update the image for a python version: create and push a new image with a to docker hub, and update the image identifier in [../../config.yml](../../config.yml).

### Process to build and push an image for a new python version, say `3.6`
- Find the latest minor version here https://github.com/CircleCI-Public/circleci-dockerfiles/tree/master/python/images, say `3.6.12`
- Build it: `docker build -t buzzardpython/ci:py36 --build-arg PYTHON_VERSION=3.6.12 .`
- Push it: `docker push buzzardpython/ci:py36`
- Retrieve the digest with `docker images --digests --no-trunk` or at https://hub.docker.com/repository/docker/buzzardpython/ci/tags
- Update the image url in `config.yml`, e.g: `buzzardpython/ci:py36@sha256:830824745288acec08dfc31116ab847f9e95d6567bbda3f52baa03a757886c2a`
