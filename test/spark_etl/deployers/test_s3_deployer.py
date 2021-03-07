import pytest
from unittest import mock
import os

from spark_etl.deployers.s3_deployer import S3Deployer
from spark_etl.exceptions import SparkETLDeploymentFailure

# @pytest.fixture()
# def server_channel():
#     return ServerChannelInterface()

# @pytest.fixture()
# def client_channel():
#     return ClientChannelInterface()


@mock.patch('boto3.client')
def test_s3_deployer_bad_location(mock_s3_client):
    s3_deployer = S3Deployer({})
    with pytest.raises(SparkETLDeploymentFailure):
        s3_deployer.deploy("foo", "/foo/bar")


@mock.patch('boto3.client')
@mock.patch('spark_etl.deployers.s3_deployer.Build')
def test_s3_deployer(MockBuild, mock_boto_client):
    mock_build = mock.Mock(
        version="1.0.0.0",
        artifacts=["X", "Y"],
        build_dir="/tmp/foo"
    )
    MockBuild.return_value = mock_build

    mock_s3_client = mock.Mock()
    mock_boto_client.return_value = mock_s3_client


    s3_deployer = S3Deployer({
        'aws_access_key_id': 'XXX',
        'aws_secret_access_key': '***'
    })
    s3_deployer.deploy("foo", "s3://foo/bar")

    os.environ
    mock_s3_client.upload_file.assert_has_calls(
        [
            mock.call("/tmp/foo/X", "foo", "bar/1.0.0.0/X"),
            mock.call("/tmp/foo/Y", "foo", "bar/1.0.0.0/Y"),
            mock.call(
                f"{os.environ['PROJECT_ROOT']}/src/spark_etl/deployers/job_loader.py",
                "foo",
                "bar/1.0.0.0/job_loader.py"),
        ]
    )

