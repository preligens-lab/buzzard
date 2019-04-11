##############################################
#                                            #
#      Dockerfile base image parameter       #
#             and default value              #
#                                            #
##############################################

ARG PYTHON_VERSION=3.7.0

FROM circleci/python:$PYTHON_VERSION-stretch


##############################################
#                                            #
#           Dockerfile parameters            #
#             and default values             #
#   We have to define them after the from.   #
#                                            #
##############################################

ARG NUMPY_VERSION=1.14.5
ARG GDAL_VERSION=2.4.0
ARG PROJ4_VERSION=4.9.3
ARG GRID_VERSION=1.6
ARG GEOS_VERSION=3.6.2
ARG LIBSPATIALINDEX_VERSION=1.8.5


########################################
#                                      #
#             Image labels             #
#                                      #
########################################

LABEL maintainer="Delair <nicolas.goguey@delair.aero>"

##############################################
#                                            #
#            Container preparation           #
#                                            #
##############################################

USER root

# Make sure we're up-to-date and install runtime requirements
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends \
    build-essential \
    bzip2 \
    ca-certificates \
    curl \
    file \
    git \
    gzip \
    openssl \
    sqlite \
    unzip \
    xmlsec1 \
    zip


##############################################
#                                            #
#          PROJ.4, GEOS, GDAL, GRID          #
#                                            #
##############################################

# Install build requirement
RUN apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev \
    libgdal-dev \
    libjpeg-dev \
    libncurses5-dev \
    libpng-dev \
    zlib1g-dev

# Fetch PROJ.4
WORKDIR /tmp
RUN curl -L http://download.osgeo.org/proj/proj-${PROJ4_VERSION}.tar.gz | tar zxf -

# Get the nadgrids
WORKDIR /tmp/proj-${PROJ4_VERSION}/nad
RUN \
  curl -L http://download.osgeo.org/proj/proj-datumgrid-${GRID_VERSION}.zip -o proj-datumgrid-${GRID_VERSION}.zip && \
  unzip -o -q proj-datumgrid-${GRID_VERSION}.zip

# Build and install PROJ.4
WORKDIR "/tmp/proj-${PROJ4_VERSION}"
RUN ./configure --prefix=/usr/local
RUN make -j $(( 2 * $(cat /proc/cpuinfo | egrep ^processor | wc -l) )) \
 && make install


# Fetch GEOS
WORKDIR /tmp
RUN curl -L http://download.osgeo.org/geos/geos-${GEOS_VERSION}.tar.bz2 | tar jxf -

# Build and install GEOS
WORKDIR "/tmp/geos-${GEOS_VERSION}"
RUN ./configure --prefix=/usr/local
RUN make -j $(( 2 * $(cat /proc/cpuinfo | egrep ^processor | wc -l) )) \
 && make install


# Fecth GDAL
WORKDIR /tmp
RUN curl -L http://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz | tar zxf -

# Build and install GDAL
WORKDIR "/tmp/gdal-${GDAL_VERSION}"
RUN ./configure \
    --prefix=/usr/local/gdal \
    --with-curl=yes \
    --with-geos=/usr/local/bin/geos-config \
    --with-proj=/usr/local/proj4 \
    --with-python
RUN make -j $(( 2 * $(cat /proc/cpuinfo | egrep ^processor | wc -l) )) \
 && make install

# Configuring environment
ENV PATH="/usr/local/gdal/bin:$PATH"
ENV LD_LIBRARY_PATH="/usr/local/proj4/lib:/usr/local/gdal/lib:/usr/local/lib:$LD_LIBRARY_PATH"
ENV GDAL_DATA="/usr/local/gdal/share/gdal"


########################################
#                                      #
#           libspatialindex            #
#                                      #
########################################
# optionnal / required for the python make_geom_valid like in PostGIS

# Fetch libspatialindex
WORKDIR /tmp
RUN curl -L http://download.osgeo.org/libspatialindex/spatialindex-src-${LIBSPATIALINDEX_VERSION}.tar.gz | tar zxf -
WORKDIR /tmp/spatialindex-src-${LIBSPATIALINDEX_VERSION}

# Build and install
RUN ./configure
RUN make -j $(( 2 * $(cat /proc/cpuinfo | egrep ^processor | wc -l) )) \
 && make install


##############################################
#                                            #
#             OpenCV prerequisites           #
#                                            #
##############################################

WORKDIR /tmp
RUN apt-get install -y --no-install-recommends \
    libsm6 \
    libxrender1 \
    libxext6


########################################
#                                      #
#     Python packages installation     #
#                                      #
########################################

# Upgrade pip
RUN pip install --upgrade pip

# Install numpy before GDAL
RUN pip install numpy==${NUMPY_VERSION} \
 && pip install GDAL==${GDAL_VERSION}


##############################################
#                                            #
#                  Cleaning                  #
#                                            #
##############################################

RUN apt-get autoremove -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


##############################################
#                                            #
#            Preping for Execution           #
#                                            #
##############################################

ENV PATH="/home/circleci/.local/bin:$PATH"
# USER circleci
CMD ["python"]
