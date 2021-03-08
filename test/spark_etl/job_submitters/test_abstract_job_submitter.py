import pytest

from spark_etl.job_submitters import AbstractJobSubmitter


def test_abstract_deployer():
    job_submitter = AbstractJobSubmitter({})

    with pytest.raises(NotImplementedError):
        job_submitter.run("/foo/bar")

