import os
import sys
from setuptools import setup, find_packages

# Requirements
# https://caremad.io/posts/2013/07/setup-vs-requirement/
reqs = [
	'numpy',
	'opencv-python',
	'gdal',
	'shapely',
	'affine',
	'scipy',
	'pint',
	'six',
]

if sys.version_info < (3, 2):
    reqs += ['chainmap>=1.0.2']

readme_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'README.md'
)
readme = open(readme_path).read()

# Classifiers
# https://pypi.python.org/pypi?%3Aaction=list_classifiers
classifiers = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Topic :: Scientific/Engineering :: GIS',
]

setup(
    name='buzzard',
    version='0.4.0',
    author='ngoguey',
    author_email='ngoguey@airware.com',
    license='Apache License 2.0',
    maintainer='ngoguey',
    maintainer_email='ngoguey@airware.com',
    description='GIS files manipulations',
    long_description=readme,
    classifiers=classifiers,
    url='https://github.com/airware/buzzard',
    download_url='https://github.com/airware/buzzard/archive/0.4.0.tar.gz',
    keywords=['gdal gis raster shp dxf tif vector'],
    packages=find_packages(),
    install_requires=reqs,
)
