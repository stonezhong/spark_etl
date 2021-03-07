import pytest
from unittest import mock
import os
from uuid import UUID

from spark_etl.deployers.hdfs_deployer import HDFSDeployer, _execute
from spark_etl.exceptions import SparkETLDeploymentFailure
import spark_etl

TEST_UUID_STR = 'abbafaa4-8626-480d-8206-e4934698e8ef'

@pytest.fixture()
def hdfs_deployer():
    deployer = HDFSDeployer({
        'bridge': 'bridge_host',
        'stage_dir': "/root/.stage"
    })
    return deployer

def test_hdfs_deployer_bad_location(hdfs_deployer):
    with pytest.raises(SparkETLDeploymentFailure):
        hdfs_deployer.deploy("foo", "/foo/bar")

@mock.patch("uuid.uuid4", return_value=UUID(TEST_UUID_STR))
@mock.patch("spark_etl.deployers.hdfs_deployer._execute")
@mock.patch("subprocess.call")
@mock.patch('spark_etl.deployers.hdfs_deployer.Build')
def test_hdfs_deployer(MockBuild, mock_subprocess_call, mock_execute, mock_uuid4, hdfs_deployer):
    mock_build = mock.Mock(
        version="1.0.0.0",
        artifacts=["X", "Y"],
        build_dir="/tmp/foo"
    )
    MockBuild.return_value = mock_build

    hdfs_deployer.deploy("foo", "hdfs://foo/bar")

    # make sure we are creating staging directory
    mock_execute.assert_has_calls(
        [
            mock.call("bridge_host", f"mkdir -p /root/.stage/{TEST_UUID_STR}")
        ]
    )

    # make sure we are copying over the artifacts to staging host
    mock_subprocess_call.assert_has_calls(
        [
            mock.call(
                ["scp", "-q", "foo/X", f"bridge_host:/root/.stage/{TEST_UUID_STR}/X"]
            ),
            mock.call(
                ["scp", "-q", "foo/Y", f"bridge_host:/root/.stage/{TEST_UUID_STR}/Y"]
            ),
        ]
    )

    # make sure we are copying over the job loader
    mock_subprocess_call.assert_called_with(
        [
            "scp", "-q",
            f"{os.path.join(spark_etl.__path__[0], 'deployers/job_loader.py')}",
            f"bridge_host:/root/.stage/{TEST_UUID_STR}/job_loader.py"
        ]
    )

    # make sure we are creating directory for the hdfs deployment
    mock_execute.assert_has_calls(
        [
            mock.call("bridge_host", f"hdfs dfs -rm -r hdfs://foo/bar/1.0.0.0", error_ok=True),
            mock.call("bridge_host", f"hdfs dfs -mkdir -p hdfs://foo/bar/1.0.0.0"),

        ]
    )

    # make sure we are copying the artifacts from staging host to hdfs
    mock_execute.assert_has_calls(
        [
            mock.call(
                "bridge_host",
                f"hdfs dfs -copyFromLocal /root/.stage/{TEST_UUID_STR}/X hdfs://foo/bar/1.0.0.0/X",
            ),
            mock.call(
                "bridge_host",
                f"hdfs dfs -copyFromLocal /root/.stage/{TEST_UUID_STR}/Y hdfs://foo/bar/1.0.0.0/Y",
            ),
        ]
    )

    # make sure we are removing what's left in staging area
    mock_execute.assert_has_calls(
        [
            mock.call(
                "bridge_host",
                f"rm -rf /root/.stage/{TEST_UUID_STR}",
            ),
        ]
    )


@mock.patch("subprocess.call", return_value=0)
def test_execute_normal(mock_subprocess_call):
    _execute("foohost", "ls -al")
    mock_subprocess_call.assert_called_with(
        ["ssh", "-q", "-t", "foohost", "ls -al"], shell=False
    )

@mock.patch("subprocess.call", return_value=1)
def test_execute_failed_supressed(mock_subprocess_call):
    _execute("foohost", "ls -al", error_ok=True)
    mock_subprocess_call.assert_called_with(
        ["ssh", "-q", "-t", "foohost", "ls -al"], shell=False
    )

@mock.patch("subprocess.call", return_value=1)
def test_execute_failed_not_supressed(mock_subprocess_call):
    with pytest.raises(Exception, match="command \"ls -al\" failed with exit code 1"):
        _execute("foohost", "ls -al", error_ok=False)
        mock_subprocess_call.assert_called_with(
            ["ssh", "-q", "-t", "foohost", "ls -al"], shell=False
        )
