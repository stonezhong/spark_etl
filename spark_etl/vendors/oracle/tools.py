import tempfile
import json
import os

import oci

# TODO Complete it!
REGION_CONFIG = {
    "IAD": {
        'object_storage': 'https://objectstorage.us-ashburn-1.oraclecloud.com',
        'dataflow'      : 'https://dataflow.us-ashburn-1.oci.oraclecloud.com',
    }
}

_READ_CHUNK_SIZE = 4*1024*1024

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

# get a dataflow client using instance principle
def get_df_client(region):
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    client = oci.data_flow.DataFlowClient(
        {}, signer=signer, service_endpoint=REGION_CONFIG[region]['dataflow']
    )        
    return client

# get a object storage client using instance principle
def get_os_client(region):
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    client = oci.object_storage.ObjectStorageClient(
        {}, signer=signer, service_endpoint=REGION_CONFIG[region]['object_storage']
    )
    return client

# upload file to object storage
def os_upload(os_client, local_filename, namespace, bucket, object_name):
    try:
        os_client.delete_object(namespace, bucket, object_name)
    except oci.exceptions.ServiceError as e:
        if e.status != 404:
            raise
    with open(local_filename, "rb") as f:
        os_client.put_object(namespace, bucket, object_name, f)

def os_download(os_client, local_filename, namespace, bucket, object_name):
    r = os_client.get_object(namespace, bucket, object_name)

    with open(local_filename, "wb") as f:
        for chunk in r.data.raw.stream(_READ_CHUNK_SIZE, decode_content=False):
            f.write(chunk)

# create a temp file and dump json to it, return the filename
def dump_json(data):
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(json.dumps(data).encode('utf-8'))
    f.close()
    return f.name

# get a small json file from oci object storage
def os_download_json(os_client, namespace, bucket, object_name):
    tmp_f = tempfile.NamedTemporaryFile(delete=False)
    tmp_f.close()
    os_download(os_client, tmp_f.name, namespace, bucket, object_name)

    with open(tmp_f.name) as f:
        data = json.load(f)
    
    os.remove(tmp_f.name)
    return data
    
