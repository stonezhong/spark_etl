if [ -z "${PROJECT_ROOT}" ]
then
    echo "Environment variable PROJECT_ROOT is not set!"
    exit 1
fi

mkdir -p ${PROJECT_ROOT}/pydocs
cd ${PROJECT_ROOT}/pydocs
rm -rf *

. ${PROJECT_ROOT}/test/.venv/bin/activate
python -m pydoc -w \
    spark_etl \
    spark_etl.application

rm -rf ${PROJECT_ROOT}/docs/pydocs
mv ${PROJECT_ROOT}/pydocs ${PROJECT_ROOT}/docs