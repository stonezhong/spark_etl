import pytest
import os
from tempfile import TemporaryDirectory

from spark_etl import Application

PROJECT_ROOT = os.environ["PROJECT_ROOT"]

def test_create():
    location = os.path.join(PROJECT_ROOT, "test", "resources", "test-app1")
    app = Application(location)

    assert app.location == location
    assert app.version == "1.0.0.1"

def test_build():
    location = os.path.join(PROJECT_ROOT, "test", "resources", "test-app1")
    app = Application(location)
    with TemporaryDirectory() as build_dir:
        # make sure the line that delete the file is covered
        with open(os.path.join(build_dir, "main.py"), "wt") as f:
            f.write("foo")
        app.build(build_dir, default_libs=['six==1.11.0'])
        for filename in ("app.zip", "lib.zip", "main.py", "manifest.json"):
            assert os.path.isfile(os.path.join(build_dir, filename))==True



