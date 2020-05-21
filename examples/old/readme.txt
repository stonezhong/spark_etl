1 测试
1.1 建立虚拟环境

mkdir -p ~/.venvs/etl_test
python3 -m venv ~/.venvs/etl_test
source ~/.venvs/etl_test/bin/activate
python -m pip install pip setuptools --upgrade
python -m pip install -e ~/DATA_DISK/projects/spark_etl
python -m pip install pywebhdfs
python -m pip install requests


1.1 平时进入虚拟环境
source ~/.venvs/etl_test/bin/activate

2 介绍
2.1 Job Submitter
它负责递交任务，再不同的系统，递交任务的形式可能不同，比如，可以用Livy，也可以ssh到某个节点，直接执行spark-submit。或者其他，比如Oracle的Data Flow，就有自己的API接口。


