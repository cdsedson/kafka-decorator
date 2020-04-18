#!/usr/bin/python
# -*- coding: <encoding name> -*-

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

with open('requirements_test.txt') as f:
    required_test = f.read().splitlines() + required

setuptools.setup(
    name="kafka_client_decorators",
    version="0.8.6",
    author="Edson Cardoso",
    author_email="edsonsn2@hotmail.com",
    description="Decorator interface to pykafka",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cdsedson/kafka-decorator.git",
    packages=setuptools.find_packages(),
    tests_require=required_test,
    test_suite = 'nose.collector',
    install_requires=required,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
