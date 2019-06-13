# https://mothergeo-py.readthedocs.io/en/latest/development/how-to/gdal-ubuntu-pkg.html

FROM ubuntu:latest

# Setup python if you don't have it yet
RUN apt-get update
RUN apt-get install -y python3 python3-pip python3-dev
RUN which -a python || true ;\
    which -a python3 || true ;\
    which -a pip || true ;\
    which -a pip3 || true

# Install GDAL binaries
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:ubuntugis/ppa && apt-get update && apt-get update
RUN apt-get install -y build-essential libssl-dev libffi-dev libxml2-dev libxslt1-dev zlib1g-dev
RUN apt-get install -y gdal-bin
RUN apt-get install -y libgdal-dev
RUN gdal-config --version

# Install GDAL python
RUN python3 -m pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==`gdal-config --version`
RUN python3 -c 'from osgeo import gdal; print(gdal.__version__)'

# Install Rtree
RUN apt-get -y install python3-rtree
RUN python3 -c 'import rtree; print(rtree.__version__)'

# Instrall opencv deps
RUN apt install -y libsm6 libxext6 libxrender-dev

# Install buzzard
RUN python3 -m pip install buzzard
RUN python3 -c 'import buzzard as buzz; print(buzz.__version__)'
