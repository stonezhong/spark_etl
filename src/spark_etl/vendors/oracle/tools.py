import subprocess

# Check a OCI api call response
def check_response(r, error_factory=None):
    if r.status != 200:
        if error_factory is None:
            error = Exception("request failed")
        else:
            error = error_factory()
        raise error

def remote_execute(host, cmd, error_ok=False):
    r = subprocess.call(["ssh", "-q", "-t", host, cmd], shell=False)
    if not error_ok and r != 0:
        raise Exception(f"command \"{cmd}\" failed with exit code {r}")

