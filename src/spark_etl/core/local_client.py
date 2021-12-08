import os
import json

from .main import ClientChannelInterface

"""Client channel using local file system
"""
class LFSClientChannel(ClientChannelInterface):
    def __init__(self, run_home_dir):
        self.run_home_dir = run_home_dir

    def read_json(self, name):
        with open(os.path.join(self.run_home_dir, name), "r") as f:
            return json.load(f)

    def has_json(self, name):
        return os.path.isfile(os.path.join(self.run_home_dir, name))

    def write_json(self, name, payload):
        with open(os.path.join(self.run_home_dir, name), "wt") as f:
            json.dump(payload, f)

    def delete_json(self, name):
        os.remove(os.path.join(self.run_home_dir, name))
