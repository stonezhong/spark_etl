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

def _delete_file_if_exist(filename):
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
            The directory path for the source code location of the application. See examples/myapp for example.
        """
        self.location = location
        with open(f"{self.location}/manifest.json", "r") as f:
            self.manifest = json.load(f)

    @property
    def version(self):
        """The application version specified by the manifest file"""
        return self.manifest['version']

    def _create_venv(self):
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

    def _get_tmp_requirements(self, default_libs):
        with tempfile.NamedTemporaryFile(mode="w+t", delete=False) as f:
            for line in default_libs:
                print(line, file=f)
            app_req_filename = os.path.join(self.location, "requirements.txt")
            if os.path.isfile(app_req_filename):
                with open(app_req_filename, "rt") as arf:
                    for line in arf:
                        print(line, file=f)
            return f.name


    def build(self, destination, default_libs=[]):
        """
        Build the application, it generates the necessary artifacts for the application including:
            app.zip         -- the archive for the application code
            lib.zip         -- the archive for the library
            main.py         -- the main application entry
            manifest.json   -- the application manifest file

        Parameters
        ----------
        location: str
            The directory where those generated artifacts is stored.

        default_libs: list of str
            A list of libraries needed which is not specified in the requirements.txt.
            E.g. ["six==1.11.0"]
        """

        os.makedirs(destination, exist_ok=True)
        build_stage_dir = tempfile.mkdtemp()

        venv_dir = self._create_venv()

        lib_dir = os.path.join(build_stage_dir, "lib")
        os.mkdir(lib_dir)
        tmp_requirements = self._get_tmp_requirements(default_libs)
        subprocess.check_call([
            os.path.join(venv_dir, "bin", "python"),
            "-m", "pip",
            "install", "-r", tmp_requirements,
            "-t", lib_dir
        ])
        os.remove(tmp_requirements)

        # generate archive for lib
        lib_filename = os.path.join(build_stage_dir, 'lib.zip')
        with _act_in_dir(lib_dir):
            subprocess.run(['zip', "-r", lib_filename, "."])

        # generate archive for app
        app_filename = os.path.join(build_stage_dir, 'app.zip')
        with _act_in_dir(self.location):
            subprocess.run(['zip', "-r", app_filename, "."])

        for filename in ["main.py", "manifest.json", "lib.zip", "app.zip"]:
            _delete_file_if_exist(os.path.join(destination, filename))

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
