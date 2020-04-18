#!/bin/bash -x 

pip install --upgrade setuptools wheel twine
python  setup.py nosetests
python setup.py sdist bdist_wheel
#python -m twine upload  dist/*

