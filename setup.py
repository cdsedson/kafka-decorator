#!/usr/bin/python
# -*- coding: <encoding name> -*-

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kafka_client_decorators",
    version="0.2.0",
    author="Edson Cardoso",
    author_email="edsonsn2@hotmail.com",
    description="Decorator interface to pykafka",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cdsedson/kafka-decorator.git",
    packages=setuptools.find_packages(),
    tests_require=['nose2', 'mock'],
    test_suite = 'nose2.collector.collector',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
