# Custom image for `circleci`

This `README.md` explains how to develop and build custom images for `circleci`.

Table of Contents
-----------------
* [Prerequisites](#prerequisites)
  * [Create the private image repository in ECS using ECR](#create-the-private-image-repository-in-ecs-using-ecr)
  * [Setting it up Docker on a Mac](#setting-it-up-docker-on-a-mac)
    * [Install Docker through brew cask](#install-docker-through-brew-cask)
    * [Launch Docker](#launch-docker)
    * [Validate Docker installation](#validate-docker-installation)
* [Build the image](#build-the-image)
  * [Get the built tagname](#get-the-built-tagname)
  * [Run the image and execute bash](#run-the-image-and-execute-bash)
* [Docker image publication](#docker-image-publication)
  * [Authenticate](#authenticate)
  * [Publish](#publish)
* [Additional Makefile details](#additional-makefile-details)
  * [Makefile options](#makefile-options)
  * [get the ECR repository](#get-the-ecr-repository)


## Prerequisites

### Create the private image repository in ECS using ECR

Example: `$> aws ecr create-repository --repository-name shrike --region us-east-1 --profile dev`

### Setting it up Docker on a Mac

#### Install Docker through `brew cask`
`$> brew cask install docker`

#### Launch Docker

To get the command line up and running, you need to launch `Docker.app` first.

- Press Command + Space to bring up Spotlight Search
- Enter Docker to launch Docker newly installed application
- Docker needs privileged access to run. A dialog box open, click OK.
- Enter your administration password and click OK.

When you launch Docker that way, the Docker whale icon appears in the status menu. As soon as the whale icon appears, the symbolic links for `docker`, `docker-compose`, `docker-credential-osxkeychain` and `docker-machine` are created in `/usr/local/bin`.

```bash
$> ls -l /usr/local/bin/docker*
lrwxr-xr-x  1 herve  staff  65 Oct  5 14:06 /usr/local/bin/docker -> /Users/herve/Library/Group Containers/group.com.docker/bin/docker
lrwxr-xr-x  1 herve  staff  73 Oct  5 14:06 /usr/local/bin/docker-compose -> /Users/herve/Library/Group Containers/group.com.docker/bin/docker-compose
lrwxr-xr-x  1 herve  staff  88 Oct  5 14:06 /usr/local/bin/docker-credential-osxkeychain -> /Users/herve/Library/Group Containers/group.com.docker/bin/docker-credential-osxkeychain
lrwxr-xr-x  1 herve  staff  73 Oct  5 14:06 /usr/local/bin/docker-machine -> /Users/herve/Library/Group Containers/group.com.docker/bin/docker-machine
```

Once the Docker whale icon show off a "Docker is running" you are ready to go

#### Validate Docker installation

```batch
$> docker run hello-world
```
You should be able to read the following in your console

```batch
Unable to find image 'hello-world:latest' locally
latest: Pulling from library/hello-world
5b0f327be733: Pull complete
Digest: sha256:b2ba691d8aac9e5ac3644c0788e3d3823f9e97f757f01d2ddc6eb5458df9d801
Status: Downloaded newer image for hello-world:latest

Hello from Docker!
This message shows that your installation appears to be working correctly.

To generate this message, Docker took the following steps:
 1. The Docker client contacted the Docker daemon.
 2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
 3. The Docker daemon created a new container from that image which runs the
    executable that produces the output you are currently reading.
 4. The Docker daemon streamed that output to the Docker client, which sent it
    to your terminal.

To try something more ambitious, you can run an Ubuntu container with:
 $ docker run -it ubuntu bash

Share images, automate workflows, and more with a free Docker ID:
 https://cloud.docker.com/

For more examples and ideas, visit:
 https://docs.docker.com/engine/userguide/
```

## Build the image

`$> make build`

### Get the built `tagname`

`$> cat ./build/tagname` after the image build.

### Run the image and execute `bash`

In order to test your image you can launch the `bash` and execute commands:
`$> docker run -it --entrypoint=/bin/bash 173654048158.dkr.ecr.us-east-1.amazonaws.com/buzzard:20171024-0.0.1 -i`

## Docker image publication

### Authenticate

`$> eval $(aws --region us-east-1 ecr get-login --profile dev --no-include-email)`

### Publish

`$> make publish`

## Additional `Makefile` details

### `Makefile` options

The `Makefile` is taking additional parameters like the AWS profile name. Ex.:

`$> make publish PROFILE=dev`

__REMINDER__: you can find the profiles you have configured in your AWS configuration file (here: `~/.aws/config`).

### get the `ECR` repository

`$> aws ecr describe-repositories --region us-east-1 --profile dev | jq -r ".repositories[] | select ( .repositoryName == \"buzzard\") | .repositoryUri"`

