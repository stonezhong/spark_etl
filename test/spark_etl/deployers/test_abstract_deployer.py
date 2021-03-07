import pytest
from unittest import mock
import os

from spark_etl.deployers import AbstractDeployer


def test_abstract_deployer():
    deployer = AbstractDeployer({})

    with pytest.raises(NotImplementedError):
        deployer.deploy("/foo/bar", "/abc")

