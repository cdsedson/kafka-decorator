#!/bin/bash -x 

pip install --upgrade setuptools wheel twine
tox
python setup.py sdist bdist_wheel
#python -m twine upload  dist/*

