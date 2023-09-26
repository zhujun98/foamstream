import json
import time

import pytest
from unittest.mock import patch

from foamclient import ZmqConsumer
from foamstream import Streamer

from .conftest import assert_result_equal, AvroDataGenerator, StringDataGenerator


_PORT = 12345

@pytest.mark.parametrize("server_sock,client_sock,protocol",
                         [("PUSH", "PULL", "iPc"),
                          ("PUB", "SUB", "tcp"),
                          ("REP", "REQ", "TCP")])
@pytest.mark.parametrize(
    "serializer, deserializer", [("avro", "avro"),
                                 ("pickle", "pickle"),
                                 (lambda x: x.encode(), lambda x: x.bytes.decode())])
def test_zmq_streamer(serializer, deserializer, server_sock, client_sock, protocol):

    if serializer == "avro":
        gen = AvroDataGenerator()
    else:
        gen = StringDataGenerator()

    with Streamer(_PORT,
                  protocol=protocol,
                  serializer=serializer,
                  schema=gen.schema,
                  sock=server_sock) as streamer:
        if protocol.upper() == "TCP":
            connect_to = f"tcp://localhost:{_PORT}"
        else:
            connect_to = f"ipc://foamstream.ipc"
        with ZmqConsumer(connect_to,
                         deserializer=deserializer,
                         schema=gen.schema,
                         sock=client_sock,
                         timeout=1.0) as client:
            if server_sock == "PUB":
                time.sleep(0.1)

            records = [gen.next() for _ in range(3)]

            for record in records:
                streamer.feed(record)

            for record in records:
                assert_result_equal(client.next(), record)


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
            time.sleep(0.01)
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
            time.sleep(0.01)
            assert_result_equal(client.next(), data_gt)


def test_zmq_streamer_report():
    gen = AvroDataGenerator()

    with Streamer(_PORT,
                  sock="PUSH",
                  schema=gen.schema) as streamer:
        with patch.object(streamer, "_report") as patched:
            with ZmqConsumer(f"tcp://localhost:{_PORT}",
                             sock="PULL",
                             schema=gen.schema,
                             timeout=1.0) as client:
                    num_items = streamer._report_every + 1
                    for _ in range(num_items):
                        data_gt = gen.next()
                        streamer.feed(data_gt)
                        time.sleep(0.01)
                        assert_result_equal(client.next(), data_gt)
                    patched.assert_called_once()
                    assert streamer._records_sent == num_items
                    assert streamer._bytes_sent > 0

                    patched.reset_mock()
                    streamer.reset_counter()
                    time.sleep(0.01)
                    patched.assert_called_once()
                    assert streamer._records_sent == 0
                    assert streamer._bytes_sent == 0


@pytest.mark.parametrize("frequency", [100, 100.5])
def test_zmq_streamer_sent_frequency(frequency):
    gen = AvroDataGenerator()

    with Streamer(_PORT,
                  sock="PUB",
                  schema=gen.schema,
                  frequency=frequency) as streamer:
        t0 = time.monotonic()
        for _ in range(int(frequency) + 1):
            streamer.feed(gen.next())
        # It should take roughly 1 second
        assert abs(time.monotonic() - t0 - 1.0) < 0.2
