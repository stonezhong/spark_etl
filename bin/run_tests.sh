if [ -z "${PROJECT_ROOT}" ]
then
    echo "Environment variable PROJECT_ROOT is not set!"
    exit 1
fi

# export PYTHONPATH=${PROJECT_ROOT}/src
cd ${PROJECT_ROOT}
. ${PROJECT_ROOT}/test/.venv/bin/activate
python -m pytest -s ${PROJECT_ROOT}/test/spark_etl --cov-report=html --cov=src/spark_etl
rm -rf ${PROJECT_ROOT}/docs/htmlcov
mv ${PROJECT_ROOT}/htmlcov ${PROJECT_ROOT}/docs