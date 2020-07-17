import os
import subprocess
import tempfile
from contextlib import contextmanager, suppress
import shutil
import json
from distutils.dir_util import copy_tree

@contextmanager
def _act_in_dir(new_dir):
    current_dir = os.getcwd()
    try:
        os.chdir(new_dir)
        yield
    finally:
        os.chdir(current_dir)

def delete_file_if_exist(filename):
    if os.path.isfile(filename):
        os.remove(filename)

class Application:
    """Represent a spark application
    """

    def __init__(self, location):
        """
        Parameters
        ----------
        location: str
            The location of the application
        """
        self.location = location
        with open(f"{self.location}/manifest.json", "r") as f:
            self.manifest = json.load(f)

    @property
    def version(self):
        return self.manifest['version']

    def create_venv(self):
        tmpdir = tempfile.mkdtemp()
        subprocess.check_call(['/usr/bin/python3', "-m", "venv", tmpdir])
        subprocess.check_call([
            os.path.join(tmpdir, "bin", "python"),
            "-m", "pip",
            "install","pip", "setuptools", "--upgrade"
        ])
        subprocess.check_call([
            os.path.join(tmpdir, "bin", "python"),
            "-m", "pip", "install","wheel"
        ])
        return tmpdir

    def build(self, destination):
        """
        Build the application
        """

        os.makedirs(destination, exist_ok=True)
        build_stage_dir = tempfile.mkdtemp()

        venv_dir = self.create_venv()

        lib_dir = os.path.join(build_stage_dir, "lib")
        os.mkdir(lib_dir)
        subprocess.check_call([
            os.path.join(venv_dir, "bin", "python"),
            "-m", "pip",
            "install", "-r", os.path.join(self.location, "requirements.txt"),
            "-t", lib_dir
        ])

        # generate archive for lib
        lib_filename = os.path.join(build_stage_dir, 'lib.zip')
        with _act_in_dir(lib_dir):
            subprocess.run(['zip', "-r", lib_filename, "."])

        # generate archive for app
        app_filename = os.path.join(build_stage_dir, 'app.zip')
        with _act_in_dir(self.location):
            subprocess.run(['zip', "-r", app_filename, "."])

        for filename in ["main.py", "manifest.json", "lib.zip", "app.zip"]:
            delete_file_if_exist(os.path.join(destination, filename))

        shutil.copyfile(lib_filename, os.path.join(destination, 'lib.zip'))
        shutil.copyfile(app_filename, os.path.join(destination, 'app.zip'))
        shutil.copyfile(
            os.path.join(self.location, "manifest.json"),
            os.path.join(destination, 'manifest.json')
        )
        shutil.copyfile(
            os.path.join(self.location, "main.py"),
            os.path.join(destination, 'main.py')
        )
        shutil.rmtree(venv_dir)
        shutil.rmtree(build_stage_dir)
