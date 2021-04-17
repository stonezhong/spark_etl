import os
import tempfile
import shutil
import subprocess
import socket
from contextlib import closing
import random
import time

random.seed()

_DEBUG_SSH = False

class SSHConfig:
    def __init__(self, config):
        self.config = config
        self.home_dir = None

    def line_to_key_value(self, line):
        pos = line.find(' ')
        key = line[:pos]
        value = line[pos+1:]
        return key, value

    def transform_line_host(self, value):
        return [
            f"Host {value}",
            "    StrictHostKeyChecking no",
            "    UserKnownHostsFile /dev/null"
        ]

    def transform_line_identity_file(self, value):
        return [
            f"    IdentityFile {self.home_dir}/keys/{value}"
        ]

    def transform_line_proxy_command(self, value):
        if not value.startswith("ssh "):
            return [
                f"    ProxyCommand {value}"
            ]
        return [
            f"    ProxyCommand ssh -F {os.path.join(self.home_dir, 'config')} {value[4:]}"
        ]

    def transform_line(self, line):
        key, value = self.line_to_key_value(line)
        ret = []

        if key == 'Host':
            return self.transform_line_host(value)

        if key == "IdentityFile":
            return self.transform_line_identity_file(value)

        if key == 'ProxyCommand':
            return self.transform_line_proxy_command(value)

        return [f"    {key} {value}"]


    def generate(self):
        self.home_dir = tempfile.mkdtemp(suffix="-ssh")
        os.mkdir(os.path.join(self.home_dir, "keys"))

        keys = self.config["keys"]
        config = self.config['config']

        # save all keys
        for k, v in keys.items():
            key_filename = os.path.join(self.home_dir, "keys", k)
            with open(key_filename, "wt") as kf:
                kf.write(v)
            os.chmod(key_filename, 0o600)

        config_filename = os.path.join(self.home_dir, "config")
        with open(config_filename, "wt") as ssh_cfg_f:
            effective_lines = []
            for line in config.split("\n"):
                l = line.strip()
                if len(l) == 0 or l[0] == '#':
                    continue
                effective_lines.append(l)

            for line in effective_lines:
                ls = self.transform_line(line)
                for l in ls:
                    print(l, file=ssh_cfg_f)
        os.chmod(config_filename, 0o664)


    def destroy(self):
        if self.home_dir is not None:
            shutil.rmtree(self.home_dir)

    def get_scp_cmds(self):
        return [
            "scp",
            "-F", f"{os.path.join(self.home_dir, 'config')}",
            "-q",
        ]

    def get_ssh_cmds(self):
        return [
            "ssh",
            "-F", f"{os.path.join(self.home_dir, 'config')}",
            "-q",
        ]

    def execute(self, host, cmds, error_ok=False, retry_count=5, wait_interval=5):
        for i in range(0, retry_count):
            ret = self._execute(host, cmds)
            if ret == 255:
                # this means the command is not even executed on remote host
                time.sleep(random.randrange(wait_interval)+1)
                continue
            if ret == 0 or error_ok:
                return ret
            raise Exception(f"{' '.join(cmds)} failed on host {host} with exit code {ret}")
        raise Exception(f"{' '.join(cmds)} failed on host {host} with exit code {ret}")

    def _execute(self, host, cmds):
        ssh_cmds = self.get_ssh_cmds()
        ssh_cmds.append(host)
        ssh_cmds.extend(cmds)
        if _DEBUG_SSH:
            print(f"running {' '.join(ssh_cmds)}")
        ret = subprocess.call(
            ssh_cmds,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            shell=False
        )
        return ret


    def scp(self, src, target, error_ok=False):
        scp_cmds = self.get_scp_cmds()
        scp_cmds.extend([src, target])
        if _DEBUG_SSH:
            print(f"running {' '.join(scp_cmds)}")
        if error_ok:
            subprocess.call(
                scp_cmds,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                shell=False
            )
        else:
            subprocess.check_call(
                scp_cmds,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                shell=False
            )

    def find_free_local_tcp_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('127.0.0.1', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    # create a TCP port forwarding
    def tunnel(self, host, remote_host, remote_port):
        local_port = self.find_free_local_tcp_port()
        ssh_cmds = self.get_ssh_cmds()
        ssh_cmds.extend([
            "-N",
            "-L",
            f"{local_port}:{remote_host}:{remote_port}",
            host
        ])
        if _DEBUG_SSH:
            print(f"running {' '.join(ssh_cmds)}")
        proc = subprocess.Popen(
            ssh_cmds,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            shell=False
        )
        return local_port, proc

