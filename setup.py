#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name='peewee',
    version='2.8.3',
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    packages=['playhouse'],
    py_modules=['peewee', 'pwiz'],
    scripts=['pwiz.py'],
)
