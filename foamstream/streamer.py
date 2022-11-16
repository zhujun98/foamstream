"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
from queue import Queue
from threading import Event
import time
from typing import Any, Callable, Union

import zmq

from .serializer import create_serializer, SerializerType
from .utils import run_in_thread


class Streamer:
    def __init__(self, port: int, serializer: Union[SerializerType, Callable], *,
                 sock: str = "PUSH",
                 buffer_size: int = 10):
        """Initialization.

        :param port: port of the ZMQ server.
        :param serializer: serializer type or a callable object which serializes
            the data.
        :param sock: socket type of the ZMQ server.
        :param buffer_size: size of the internal buffer for holding the data
            to be sent.
        """
        self._ctx = zmq.Context()

        sock = sock.upper()
        if sock == 'PUSH':
            self._socket = self._ctx.socket(zmq.PUSH)
        elif sock == 'PUB':
            self._socket = self._ctx.socket(zmq.SUB)
        else:
            raise ValueError('Unsupported ZMQ socket type: %s' % str(sock))

        self._sock_type = self._socket.type
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.set_hwm(1)
        self._socket.bind(f"tcp://*:{port}")

        if callable(serializer):
            self._pack = serializer
        else:
            self._pack = create_serializer(serializer)

        self._buffer = Queue(maxsize=buffer_size)
        self._ev = Event()

    def feed(self, data: Any) -> None:
        self._buffer.put(data)

    @run_in_thread(daemon=True)
    def start(self) -> None:
        while not self._ev.is_set():
            payload = self._pack(self._buffer.get())
            self._socket.send(payload)

    def stop(self) -> None:
        self._ev.set()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        while not self._buffer.empty():
            time.sleep(1.)
        self.stop()
        self._ctx.destroy(linger=0)
