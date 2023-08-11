import time

import pytest

from foamclient import SerializerType, ZmqConsumer
from foamstream import Streamer

from .conftest import assert_result_equal, AvroDataGenerator, StringDataGenerator


_PORT = 12345


@pytest.mark.parametrize("daemon", [True, False])
@pytest.mark.parametrize("server_sock,client_sock", [("PUSH", "PULL"), ("PUB", "SUB"), ("REP", "REQ")])
@pytest.mark.parametrize(
    "serializer, deserializer", [(SerializerType.AVRO, SerializerType.AVRO),
                                 (SerializerType.PICKLE, SerializerType.PICKLE),
                                 (lambda x: x.encode(), lambda x: x.bytes.decode())])
def test_zmq_streamer(serializer, deserializer, server_sock, client_sock, daemon):

    if serializer == SerializerType.AVRO:
        gen = AvroDataGenerator()
    else:
        gen = StringDataGenerator()

    with Streamer(_PORT,
                  serializer=serializer,
                  schema=gen.schema,
                  sock=server_sock,
                  daemon=daemon) as streamer:
        with ZmqConsumer(f"tcp://localhost:{_PORT}",
                         deserializer=deserializer,
                         schema=gen.schema,
                         sock=client_sock,
                         timeout=1.0) as client:
            if server_sock == "PUB":
                time.sleep(0.1)
            for i in range(3):
                data_gt = gen.next()
                streamer.feed(data_gt)
                assert_result_equal(client.next(), data_gt)
