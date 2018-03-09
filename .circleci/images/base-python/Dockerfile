##############################################
#                                            #
#      Dockerfile base image parameter       #
#             and default value              #
#                                            #
##############################################

ARG PYTHON_VERSION=3.6.3

FROM python:${PYTHON_VERSION}


##############################################
#                                            #
#           Dockerfile parameters            #
#             and default values             #
#   We have to define them after the from.   #
#                                            #
##############################################

ARG BUILD_VERSION=0.0.2
ARG GEOS_VERSION=3.6.2
ARG GDAL_VERSION=2.2.2
ARG PROJ4_VERSION=4.9.3


##############################################
#                                            #
#                 Image labels               #
#                                            #
##############################################

LABEL author="hnivon@airware.com"

##############################################
#                                            #
#            Container preparation           #
#                                            #
##############################################

# Make sure we're up-to-date and install runtime requirements
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends \
    binutils \
    bzip2 \
    ca-certificates \
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
#   GEOS X.X.X & GDAL X.X.X & PROJ.4 X.X.X   #
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

# Install numpy and potential additional requirements before GDAL's compilation
WORKDIR /tmp
COPY requirements.txt  /tmp/
RUN pip install --requirement requirements.txt

# Compilation work for geos
RUN mkdir "/tmp/geos-${GEOS_VERSION}-build"
WORKDIR "/tmp/geos-${GEOS_VERSION}-build"
RUN curl -o "geos-${GEOS_VERSION}.tar.bz2" \
    "http://download.osgeo.org/geos/geos-${GEOS_VERSION}.tar.bz2" \
    && bunzip2 "geos-${GEOS_VERSION}.tar.bz2" \
    && tar xvf "geos-${GEOS_VERSION}.tar"
WORKDIR "/tmp/geos-${GEOS_VERSION}-build/geos-${GEOS_VERSION}"
RUN ./configure --prefix=/usr/local/geos

# Make in parallel with 2x the number of processors.
RUN make -j $(( 2 * $(cat /proc/cpuinfo | egrep ^processor | wc -l) )) \
 && make install \
 && ldconfig


# Compilation work for Proj.4
RUN mkdir "/tmp/proj-${PROJ4_VERSION}-build"
WORKDIR "/tmp/proj-${PROJ4_VERSION}-build"
RUN curl -o "proj-${PROJ4_VERSION}.tar.gz" \
    "http://download.osgeo.org/proj/proj-${PROJ4_VERSION}.tar.gz" \
    && tar xfz "proj-${PROJ4_VERSION}.tar.gz"
WORKDIR "/tmp/proj-${PROJ4_VERSION}-build/proj-${PROJ4_VERSION}"
RUN ./configure --prefix=/usr/local/proj4

# Make in parallel with 2x the number of processors.
RUN make -j $(( 2 * $(cat /proc/cpuinfo | egrep ^processor | wc -l) )) \
 && make install \
 && ldconfig


# Compilation work for GDAL
RUN mkdir "/tmp/gdal-${GDAL_VERSION}-build"
WORKDIR "/tmp/gdal-${GDAL_VERSION}-build"
RUN curl -o "gdal-${GDAL_VERSION}.tar.gz" \
    "http://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz" \
    && tar xfz "gdal-${GDAL_VERSION}.tar.gz"
WORKDIR "/tmp/gdal-${GDAL_VERSION}-build/gdal-${GDAL_VERSION}"
RUN ./configure --prefix=/usr/local/gdal \
                --with-curl=yes \
                --with-static-proj4=/usr/local/proj4 \
                --with-python=yes

# Make in parallel with 2x the number of processors.
RUN make -j $(( 2 * $(cat /proc/cpuinfo | egrep ^processor | wc -l) )) \
 && make install \
 && ldconfig

# Configuring environment
ENV PATH="/usr/local/gdal/bin:$PATH"
ENV LD_LIBRARY_PATH="/usr/local/proj4/lib:/usr/local/gdal/lib:$LD_LIBRARY_PATH"
ENV GDAL_DATA="/usr/local/gdal/share/gdal"


##############################################
#                                            #
#             OpenCV prerequisites           #
#                                            #
##############################################

WORKDIR /tmp

RUN apt-get install -y --no-install-recommends \
    libsm6 \
    libxrender1 \
    libXext6


##############################################
#                                            #
#                  Cleaning                  #
#                                            #
##############################################

RUN apt-get autoremove -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/partial/* /tmp/* /var/tmp/*


##############################################
#                                            #
#            Preping for Execution           #
#                                            #
##############################################

CMD ["python"]
