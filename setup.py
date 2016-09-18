#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

setup(
    name='peewee',
    version='2.8.3',
    license='MIT',
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    packages=find_packages('src'),
    package_dir={'': 'src'},
)
