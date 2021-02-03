import json

class Build:
    """ This represent an application build """
    def __init__(self, build_dir):
        """
        Parameters
        ----------
        build_dir: str
            The directory for the build.
        """
        self.build_dir = build_dir
        with open(f"{self.build_dir}/manifest.json", "r") as f:
            self.manifest = json.load(f)

    @property
    def artifacts(self):
        """List of artifacts of the build"""
        return ("app.zip", "lib.zip", "main.py", "manifest.json")

    @property
    def version(self):
        """Build version"""
        return self.manifest["version"]
