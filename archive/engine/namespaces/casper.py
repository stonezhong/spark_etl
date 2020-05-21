from .base import Namespace
from ..core import DataObject

# casper object path is
# /region/bucket/object_name

_READ_CHUNK_SIZE        = 64*1024*1024

def get_os_client(region):
    import oci
    oci_regions = {
        "PHX"           : "https://objectstorage.us-phoenix-1.oraclecloud.com",
        "IAD"           : "https://objectstorage.us-ashburn-1.oraclecloud.com",
        "UK_LONDON_1"   : "https://objectstorage.uk-london-1.oraclecloud.com",
        "EU_FRANKFURT_1": "https://objectstorage.eu-frankfurt-1.oraclecloud.com",
        "AP_SEOUL_1"    : "https://objectstorage.ap-seoul-1.oraclecloud.com",
        "CA_TORONTO_1"  : "https://objectstorage.ca-toronto-1.oraclecloud.com",
        "AP_TOKYO_1"    : "https://objectstorage.ap-tokyo-1.oraclecloud.com",
        "AP_MUMBAI_1"   : "https://objectstorage.ap-mumbai-1.oraclecloud.com",
        "SA_SAOPAULO_1" : "https://objectstorage.sa-saopaulo-1.oraclecloud.com",
        "AP_SYDNEY_1"   : "https://objectstorage.ap-sydney-1.oraclecloud.com",
        "EU_ZURICH_1"   : "https://objectstorage.eu-zurich-1.oraclecloud.com",
    }
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    os_client = oci.object_storage.ObjectStorageClient(
        {}, signer=signer, service_endpoint=oci_regions[region]
    )
    return os_client


class CasperDataObject(DataObject):
    def __init__(self, namespace, path, format):
        super(CasperDataObject, self).__init__(namespace, path, format)
    
    def download(self, local_filename):
        self.namespace.download(self.path, self.format, local_filename)

class CasperNamespace(Namespace):
    def __init__(self, name, os_namespace_name):
        super(CasperNamespace, self).__init__(name)
        self.os_namespace_name = os_namespace_name

    def get_data_object(self, path, format):
        return CasperDataObject(self, path, format)
    
    def download(self, path, format, local_filename):
        p1 = path.find('/')
        p2 = path.find('/', p1+1)
        p3 = path.find('/', p2+1)
        if p1 == -1 or p2 == -1 or p3 == -1:
            raise ValueError("wrong path: {}".format(path))

        region = path[p1+1:p2]
        bucket = path[p2+1:p3]
        object_name = path[p3+1:]

        os_client = get_os_client(region)

        r = os_client.get_object(self.os_namespace_name, bucket, object_name)

        with open(local_filename, "wb") as f:
            for chunk in r.data.raw.stream(_READ_CHUNK_SIZE, decode_content=False):
                f.write(chunk)




