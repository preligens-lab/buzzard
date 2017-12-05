# from distutils.core import setup
import sys
from setuptools import setup, find_packages

from pip.req import parse_requirements

reqs = [
    str(ir.req)
    for ir in parse_requirements('requirements.txt', session='hack')
]

if sys.version_info < (3, 2):
    reqs += ['chainmap>=1.0.2']

setup(
    name='buzzard',
    version='0.3.0',
    author='ngoguey',
    author_email='ngoguey@airware.com',
    description='GIS environment',
    url='https://github.com/airware/buzzard',
    packages=find_packages(),
    install_requires=reqs,
)
