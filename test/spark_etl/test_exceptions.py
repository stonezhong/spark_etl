import pytest

from spark_etl import SparkETLException, SparkETLDeploymentFailure, SparkETLLaunchFailure, \
    SparkETLKillFailure, SparkETLGetStatusFailure

def test_create_exceptions():
    exception = SparkETLException("foo")
    assert isinstance(exception, Exception)
    with pytest.raises(SparkETLException):
        raise exception


    exception = SparkETLDeploymentFailure("foo")
    assert isinstance(exception, SparkETLException)
    with pytest.raises(SparkETLDeploymentFailure):
        raise exception

    exception = SparkETLLaunchFailure("foo")
    assert isinstance(exception, SparkETLException)
    with pytest.raises(SparkETLLaunchFailure):
        raise exception

    exception = SparkETLKillFailure("foo")
    assert isinstance(exception, SparkETLException)
    with pytest.raises(SparkETLKillFailure):
        raise exception

    exception = SparkETLGetStatusFailure("foo")
    assert isinstance(exception, SparkETLException)
    with pytest.raises(SparkETLGetStatusFailure):
        raise exception
