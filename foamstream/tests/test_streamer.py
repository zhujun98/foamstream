import time

import pytest

from foamclient import ZmqConsumer
from foamstream import Streamer


_PORT = 12345


@pytest.mark.parametrize("server_sock,client_sock", [("PUSH", "PULL"), ("PUB", "SUB"), ("REP", "REQ")])
def test_zmq_client_push_pull(server_sock, client_sock):
    with Streamer(port=_PORT, sock=server_sock) as streamer:
        with ZmqConsumer(f"tcp://localhost:{_PORT}",
                         deserializer=lambda x: x,
                         sock=client_sock,
                         timeout=1.0) as client:
            if server_sock == "PUB":
                time.sleep(0.1)
            for i in range(3):
                streamer.feed(f"data{i}".encode())
                assert bytes(client.next()) == bytes(f"data{i}", encoding="utf-8")
