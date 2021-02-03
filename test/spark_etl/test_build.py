import pytest
import os
from tempfile import TemporaryDirectory

from spark_etl import Application, Build

PROJECT_ROOT = os.environ["PROJECT_ROOT"]

def test_load_build():
    location = os.path.join(PROJECT_ROOT, "test", "resources", "test-app1")
    app = Application(location)
    with TemporaryDirectory() as build_dir:
        app.build(build_dir, default_libs=['six==1.11.0'])

        build = Build(build_dir)

        # test build.artifacts
        assert len(build.artifacts) == 4
        for filename in ("app.zip", "lib.zip", "main.py", "manifest.json"):
            assert filename in build.artifacts

        # test build.version
        assert build.version == "1.0.0.1"




