#!/bin/sh


find src -name "*.egg-info" | xargs rm -rf
rm -rf build
rm -rf dist
python setup.py sdist bdist_wheel --universal
