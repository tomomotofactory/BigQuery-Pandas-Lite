#!/usr/bin/env python
# coding: utf-8
from setuptools import setup

setup(
    name='BigQuery-Pandas-Lite',
    version='1.0.0',
    description='Simple bigQuery client.',
    license='Apache 2.0',
    author='tomomoto',
    author_email='tomomoto1983@gmail.com',
    url='https://github.com/tomomotofactory/bq_python_lite.git',
    keywords='bigQuery BigQuery client',
    packages=['bqlite'],
    install_requires=['pandas', 'google-api-python-client']
)
