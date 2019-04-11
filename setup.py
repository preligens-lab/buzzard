import os
import sys
from setuptools import setup, find_packages

# Requirements
# https://caremad.io/posts/2013/07/setup-vs-requirement/
reqs = [
	'opencv-python',
	'gdal',
	'shapely',
	'affine',
	'scipy',
	'pint',
	'six',
    'sortedcontainers',
    'rtree',
]


if sys.version_info < (3, 5) and sys.version_info >= (3, 4):
    # https://github.com/scikit-image/scikit-image/issues/3820
    reqs += ['scikit-image<=0.14.1', 'numpy<1.16']
else:
    reqs += ['scikit-image', 'numpy']

if sys.version_info < (3, 2):
    reqs += ['chainmap>=1.0.2']

readme_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'README.md'
)
readme = open(readme_path, 'rb').read().decode("UTF-8")

# Classifiers
# https://pypi.python.org/pypi?%3Aaction=list_classifiers
classifiers = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Topic :: Scientific/Engineering :: GIS',
]

setup(
    name='buzzard',
    version='0.5.0',
    author='ngoguey',
    author_email='nicolas.goguey@delair.aero',
    license='Apache License 2.0',
    maintainer='ngoguey',
    maintainer_email='nicolas.goguey@delair.aero',
    description='GIS files manipulations',
    long_description=readme,
    long_description_content_type='text/markdown',
    classifiers=classifiers,
    url='https://github.com/airware/buzzard',
    download_url='https://github.com/airware/buzzard/archive/0.5.0.tar.gz',
    keywords=['gdal gis raster shp dxf tif vector'],
    packages=find_packages(),
    install_requires=reqs,
    python_requires='>=3.4',
)
