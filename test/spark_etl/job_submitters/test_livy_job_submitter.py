import pytest
from unittest import mock
import os
from uuid import UUID
import json

from spark_etl.job_submitters.livy_job_submitter import LivyJobSubmitter
from spark_etl.exceptions import SparkETLLaunchFailure
from requests.auth import HTTPBasicAuth

TEST_UUID_STR = 'abbafaa4-8626-480d-8206-e4934698e8ef'

@pytest.fixture()
def livy_job_submitter():
    return LivyJobSubmitter({
        'service_url': 'http://10.0.0.18:60008/',
        'bridge': 'bridge_host',
        'stage_dir': '/root/.stage',
        'run_dir': 'hdfs:///beta/etl/runs',
    })

@pytest.fixture()
def livy_job_submitter_with_auth():
    return LivyJobSubmitter({
        'service_url': 'http://10.0.0.18:60008/',
        'bridge': 'bridge_host',
        'stage_dir': '/root/.stage',
        'run_dir': 'hdfs:///beta/etl/runs',
        'username': "foo",
        'password': "***"
    })

# special case: deployment_location is bad, it is neither hdfs, nor s3
def test_livy_job_submitter_with_unsupported_deployment_location(livy_job_submitter):
    args = {
        "foo": 1,
        "bar": 2
    }

    with pytest.raises(SparkETLLaunchFailure):
        livy_job_submitter.run(
            "oci:///beta/etl/apps/dummy/1.0.0.0",
            args=args
        )

# successful case, no auth info
@mock.patch("requests.get")
@mock.patch("requests.post")
@mock.patch("uuid.uuid4", return_value=UUID(TEST_UUID_STR))
@mock.patch("subprocess.check_call")
@mock.patch("spark_etl.job_submitters.livy_job_submitter.ClientChannel")
def test_livy_job_submitter_noauth(
    MockClientChannel,
    mock_subprocess_check_call,
    mock_uuid4,
    requests_post,
    requests_get,
    livy_job_submitter
):
    mock_client_channel = mock.Mock()
    MockClientChannel.return_value = mock_client_channel

    requests_post.side_effect = [
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "running", "appId": "abc123"}'
        )
    ]

    requests_get.side_effect = [
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "running", "appId": "abc123"}'
        ),
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "success", "appId": "abc123"}'
        ),
    ]

    args = {
        "foo": 1,
        "bar": 2
    }

    livy_job_submitter.run(
        "hdfs:///beta/etl/apps/dummy/1.0.0.0",
        args=args
    )

    # it will create a run directory
    mock_subprocess_check_call.assert_has_calls([
        mock.call(
            [
                "ssh", "-q", "bridge_host", "hdfs", "dfs", "-mkdir", "-p",
                f"hdfs:///beta/etl/runs/{TEST_UUID_STR}"
            ]
        ),
    ])

    # then it creates a client channel and write input.json
    mock_client_channel.write_json.assert_called_with(
        "input.json", args
    )

    # make sure we call livy service to submit the job
    requests_post.assert_called_with(
        "http://10.0.0.18:60008/batches",
        data = json.dumps({
            'file': 'hdfs:///beta/etl/apps/dummy/1.0.0.0/job_loader.py',
            'pyFiles': [
                "hdfs:///beta/etl/apps/dummy/1.0.0.0/app.zip",
            ],
            'args': [
                "--run-id",
                TEST_UUID_STR,
                "--run-dir", f"hdfs:///beta/etl/runs/{TEST_UUID_STR}",
                "--lib-zip", "hdfs:///beta/etl/apps/dummy/1.0.0.0/lib.zip"
            ],
        }),
        headers = {
            'Content-Type': 'application/json',
            'X-Requested-By': 'root',
            'proxyUser': 'root'
        },
        verify=False
    )


# successful case, with auth info
@mock.patch("requests.get")
@mock.patch("requests.post")
@mock.patch("uuid.uuid4", return_value=UUID(TEST_UUID_STR))
@mock.patch("subprocess.check_call")
@mock.patch("spark_etl.job_submitters.livy_job_submitter.ClientChannel")
def test_livy_job_submitter(
    MockClientChannel,
    mock_subprocess_check_call,
    mock_uuid4,
    requests_post,
    requests_get,
    livy_job_submitter_with_auth
):
    mock_client_channel = mock.Mock()
    MockClientChannel.return_value = mock_client_channel

    requests_post.side_effect = [
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "running", "appId": "abc123"}'
        )
    ]

    requests_get.side_effect = [
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "running", "appId": "abc123"}'
        ),
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "success", "appId": "abc123"}'
        ),
    ]

    args = {
        "foo": 1,
        "bar": 2
    }

    livy_job_submitter_with_auth.run(
        "hdfs:///beta/etl/apps/dummy/1.0.0.0",
        args=args
    )

    # it will create a run directory
    mock_subprocess_check_call.assert_has_calls([
        mock.call(
            [
                "ssh", "-q", "bridge_host", "hdfs", "dfs", "-mkdir", "-p",
                f"hdfs:///beta/etl/runs/{TEST_UUID_STR}"
            ]
        ),
    ])

    # then it creates a client channel and write input.json
    mock_client_channel.write_json.assert_called_with(
        "input.json", args
    )

    # make sure we call livy service to submit the job
    requests_post.assert_called_with(
        "http://10.0.0.18:60008/batches",
        data = json.dumps({
            'file': 'hdfs:///beta/etl/apps/dummy/1.0.0.0/job_loader.py',
            'pyFiles': [
                "hdfs:///beta/etl/apps/dummy/1.0.0.0/app.zip",
            ],
            'args': [
                "--run-id",
                TEST_UUID_STR,
                "--run-dir", f"hdfs:///beta/etl/runs/{TEST_UUID_STR}",
                "--lib-zip", "hdfs:///beta/etl/apps/dummy/1.0.0.0/lib.zip"
            ],
        }),
        headers = {
            'Content-Type': 'application/json',
            'X-Requested-By': 'root',
            'proxyUser': 'root'
        },
        auth=HTTPBasicAuth("foo", "***"),
        verify=False
    )



# submit job failed
@mock.patch("requests.get")
@mock.patch("requests.post")
@mock.patch("uuid.uuid4", return_value=UUID(TEST_UUID_STR))
@mock.patch("subprocess.check_call")
@mock.patch("spark_etl.job_submitters.livy_job_submitter.ClientChannel")
def test_livy_job_submitter_submit_failed(
    MockClientChannel,
    mock_subprocess_check_call,
    mock_uuid4,
    requests_post,
    requests_get,
    livy_job_submitter
):
    mock_client_channel = mock.Mock()
    MockClientChannel.return_value = mock_client_channel

    requests_post.side_effect = [
        mock.PropertyMock(
            status_code=500,
            content=b'{"msg": "job failed to submit"}'
        )
    ]

    requests_get.side_effect = [
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "running", "appId": "abc123"}'
        ),
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "success", "appId": "abc123"}'
        ),
    ]

    args = {
        "foo": 1,
        "bar": 2
    }

    with pytest.raises(SparkETLLaunchFailure, match="Failed to submit the job"):
        livy_job_submitter.run(
            "hdfs:///beta/etl/apps/dummy/1.0.0.0",
            args=args
        )


# successful case, no auth info
@mock.patch("requests.get")
@mock.patch("requests.post")
@mock.patch("uuid.uuid4", return_value=UUID(TEST_UUID_STR))
@mock.patch("subprocess.check_call")
@mock.patch("spark_etl.job_submitters.livy_job_submitter.ClientChannel")
def test_livy_job_submitter_job_failed(
    MockClientChannel,
    mock_subprocess_check_call,
    mock_uuid4,
    requests_post,
    requests_get,
    livy_job_submitter
):
    mock_client_channel = mock.Mock()
    MockClientChannel.return_value = mock_client_channel

    requests_post.side_effect = [
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "running", "appId": "abc123"}'
        )
    ]

    requests_get.side_effect = [
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "running", "appId": "abc123"}'
        ),
        mock.PropertyMock(
            status_code=200,
            content=b'{"id": 1, "state": "dead", "appId": "abc123"}'
        ),
    ]

    args = {
        "foo": 1,
        "bar": 2
    }

    with pytest.raises(SparkETLLaunchFailure, match="Job failed"):
        livy_job_submitter.run(
            "hdfs:///beta/etl/apps/dummy/1.0.0.0",
            args=args
        )

# TODO: test handler and CLI
