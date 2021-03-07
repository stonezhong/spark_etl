import pytest

from spark_etl.core import  ServerChannelInterface, ClientChannelInterface

@pytest.fixture()
def server_channel():
    return ServerChannelInterface()

@pytest.fixture()
def client_channel():
    return ClientChannelInterface()


def test_ServerChannelInterface(server_channel):
    with pytest.raises(NotImplementedError):
        server_channel.read_json(None, "foo")

    with pytest.raises(NotImplementedError):
        server_channel.has_json(None, "foo")

    with pytest.raises(NotImplementedError):
        server_channel.write_json(None, "foo", {})

    with pytest.raises(NotImplementedError):
        server_channel.delete_json(None, "foo")

def test_ClientChannelInterface(client_channel):
    with pytest.raises(NotImplementedError):
        client_channel.read_json("foo")

    with pytest.raises(NotImplementedError):
        client_channel.has_json("foo")

    with pytest.raises(NotImplementedError):
        client_channel.write_json("foo", {})

    with pytest.raises(NotImplementedError):
        client_channel.delete_json("foo")
