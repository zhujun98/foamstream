"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
from queue import Empty, Queue
from threading import Event, Thread
import time
from typing import Callable, Optional, Union

import zmq

from foamclient import create_serializer, SerializerType


_reset_counter_sentinel = object()

class Streamer:

    def __init__(self, port: int, *,
                 serializer: Union[SerializerType, Callable] = SerializerType.AVRO,
                 schema: Optional[object] = None,
                 sock: str = "PUSH",
                 recv_timeout: float = 0.1,
                 multipart: bool = False,
                 request: bytes = b"READY",
                 buffer_size: int = 10,
                 daemon: bool = False,
                 early_serialization: bool = False,
                 report_every: int = 100):
        """Initialization.

        :param port: port of the ZMQ server.
        :param serializer: serializer type or a callable object which serializes
            the data.
        :param schema: optional data schema for the serializer.
        :param sock: socket type of the ZMQ server.
        :param recv_timeout: maximum time in seconds before a recv operation raises
            zmq.error.Again.
        :param multipart: whether the data will be sent as a multipart message.
        :param request: acknowledgement expected from the REQ server when the socket
            type is REP.
        :param buffer_size: size of the internal buffer for holding the data
            to be sent.
        :param daemon: True for making the thread in which the socket runs a daemon
            thread.
        :param early_serialization:
        :param report_every:
        """
        self._port = port
        self._ctx = zmq.Context()
        self._recv_timeout = int(recv_timeout * 1000)
        self._request = request
        self._multipart = multipart

        self._hwm = 1

        sock = sock.upper()
        if sock == 'PUSH':
            self._sock_type = zmq.PUSH
        elif sock == 'REP':
            self._sock_type = zmq.REP
        elif sock == 'PUB':
            self._sock_type = zmq.PUB
        else:
            raise ValueError('Unsupported ZMQ socket type: %s' % str(sock))

        if callable(serializer):
            self._pack = serializer
        else:
            self._pack = create_serializer(
                serializer, schema, multipart=multipart)

        self._buffer = Queue(maxsize=buffer_size)

        self._thread = Thread(target=self._run, daemon=daemon)
        self._ev = Event()

        self._early_serialization = early_serialization

        self._counter = 0
        self._report_every = report_every

    def _init_socket(self):
        # It is not necessary to set HWM here since the number of messages is bound by
        # the buffer size. Btw. setting hwm to 1 could result in message loss in
        # pub-sub connection.
        socket = self._ctx.socket(self._sock_type)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.RCVTIMEO, self._recv_timeout)
        socket.bind(f"tcp://*:{self._port}")
        return socket

    def feed(self, data: object) -> None:
        if self._early_serialization:
            data = self._pack(data)
        self._buffer.put(data)

    def start(self) -> None:
        self._thread.start()

    def _send(self, socket, payload):
        if self._multipart:
            for i, item in enumerate(payload):
                if i == len(payload) - 1:
                    socket.send(item)
                else:
                    socket.send(item, zmq.SNDMORE)
        else:
            socket.send(payload)

        self._counter += 1
        if self._report_every > 0 and self._counter % self._report_every == 0:
            print(f"Number of items sent: {self._counter:>6d}")

    def _run(self) -> None:
        socket = self._init_socket()
        rep_ready = False
        while not self._ev.is_set():
            if self._sock_type == zmq.REP and not rep_ready:
                try:
                    request = socket.recv()
                    # TODO: We could ignore the request, but we need to inform
                    #       the client.
                    assert request == self._request
                    rep_ready = True
                except zmq.error.Again:
                    continue
                except zmq.error.ContextTerminated:
                    break

            try:
                data = self._buffer.get(timeout=0.1)
                if data == _reset_counter_sentinel:
                    self.__reset_counter()
                    continue

                if not self._early_serialization:
                    data = self._pack(data)
                self._send(socket, data)

                if self._sock_type == zmq.REP:
                    rep_ready = False
            except Empty:
                continue

    def stop(self) -> None:
        self._ev.set()
        if not self._thread.isDaemon():
            self._thread.join()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        while not self._buffer.empty():
            time.sleep(1.)
        self.stop()
        self._ctx.destroy()

    def __reset_counter(self) -> None:
        print(f"Number of items sent: {self._counter:>6d}")
        self._counter = 0

    def reset_counter(self) -> None:
        self._buffer.put(_reset_counter_sentinel)

