#!/bin/sh

if [ -z "${PROJECT_ROOT}" ]
then
    echo "Environment variable PROJECT_ROOT is not set!"
    exit 1
fi

echo "Creating python virtual environment at ${PROJECT_ROOT}/test/.venv"
rm -rf ${PROJECT_ROOT}/test/.venv
mkdir ${PROJECT_ROOT}/test/.venv
python3 -m venv ${PROJECT_ROOT}/test/.venv
. ${PROJECT_ROOT}/test/.venv/bin/activate
pip install pip setuptools --upgrade
pip install wheel
pip install -r ${PROJECT_ROOT}/test_requirements.txt
pip install -e ${PROJECT_ROOT}