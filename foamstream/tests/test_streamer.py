import json
import time

import pytest

from foamclient import ZmqConsumer
from foamstream import Streamer

from .conftest import assert_result_equal, AvroDataGenerator, StringDataGenerator


_PORT = 12345


@pytest.mark.parametrize("daemon", [True, False])
@pytest.mark.parametrize("server_sock,client_sock", [("PUSH", "PULL"), ("PUB", "SUB"), ("REP", "REQ")])
@pytest.mark.parametrize(
    "serializer, deserializer", [("avro", "avro"),
                                 ("pickle", "pickle"),
                                 (lambda x: x.encode(), lambda x: x.bytes.decode())])
def test_zmq_streamer(serializer, deserializer, server_sock, client_sock, daemon):

    if serializer == "avro":
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


@pytest.mark.parametrize(
    "serializer, deserializer", [("pickle", "pickle"),
                                 (lambda x: (json.dumps(x[0]).encode("utf8"), json.dumps(x[1]).encode("utf8")),
                                  lambda x: [json.loads(x[0].bytes), json.loads(x[1].bytes)])])
def test_zmq_streamer_with_multipart_message(serializer, deserializer):
    with pytest.raises(ValueError, match="does not support multipart message"):
        Streamer(_PORT, sock="REP", multipart=True)

    data_gt = [
        {"a": 123},
        {"b": "Hello world"}
    ]

    with Streamer(_PORT,
                  sock="PUSH",
                  serializer=serializer,
                  multipart=True) as streamer:
        with ZmqConsumer(f"tcp://localhost:{_PORT}",
                         sock="PULL",
                         deserializer=deserializer,
                         multipart=True,
                         timeout=1.0) as client:
            streamer.feed(data_gt)
            assert client.next() == [{'a': 123}, {'b': 'Hello world'}]


@pytest.mark.parametrize("early_serialization", [True, False])
def test_zmq_streamer_early_serialization(early_serialization):
    gen = AvroDataGenerator()

    with Streamer(_PORT,
                  sock="PUSH",
                  schema=gen.schema,
                  early_serialization=early_serialization) as streamer:
        with ZmqConsumer(f"tcp://localhost:{_PORT}",
                         sock="PULL",
                         schema=gen.schema,
                         timeout=1.0) as client:

            data_gt = gen.next()
            streamer.feed(data_gt)
            assert_result_equal(client.next(), data_gt)
