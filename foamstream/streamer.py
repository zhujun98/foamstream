"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
from queue import Empty, Queue
import sys
from threading import Event, Thread
import time
from typing import Callable, Optional, Union

import zmq

from foamclient import create_serializer


_reset_counter_sentinel = object()


class Streamer:

    _mega_bytes = 1 / (1024 * 1024)

    def __init__(self, port: int, *,
                 serializer: Union[str, Callable] = "avro",
                 schema: Optional[object] = None,
                 sock: str = "PUSH",
                 recv_timeout: float = 0.1,
                 multipart: bool = False,
                 request: bytes = b"READY",
                 hwm: Optional[int] = None,
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
        :param hwm: high water mark in ZMQ.
        :param buffer_size: size of the internal buffer for holding the data
            to be sent.
        :param daemon: True for making the thread in which the socket runs a daemon
            thread.
        :param early_serialization: If True, the data will be serialized before queued
            for being sent.
        :param report_every: the interval of reporting (e.g. print out) the number
            of data sent.
        """
        self._port = port
        self._ctx = zmq.Context()
        self._recv_timeout = int(recv_timeout * 1000)
        self._request = request
        self._multipart = multipart

        sock = sock.upper()
        if sock == 'PUSH':
            self._sock_type = zmq.PUSH
        elif sock == 'REP':
            self._sock_type = zmq.REP
        elif sock == 'PUB':
            self._sock_type = zmq.PUB
        else:
            raise ValueError('Unsupported ZMQ socket type: %s' % str(sock))

        self._hwm = 1000 if hwm is None else hwm

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
        self._t0 = time.time()
        self._bytes_sent = 0
        self._report_every = report_every

    def _init_socket(self):
        socket = self._ctx.socket(self._sock_type)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.RCVTIMEO, self._recv_timeout)
        socket.set_hwm(self._hwm)
        socket.bind(f"tcp://*:{self._port}")
        return socket

    def feed(self, data: object) -> None:
        if self._early_serialization:
            data = self._pack(data)
        self._buffer.put(data)

    def start(self) -> None:
        self.reset_counter()
        self._thread.start()

    def _report(self):
        rate = self._bytes_sent * self._mega_bytes / (time.time() - self._t0)
        print(f"Number of items sent: {self._counter:>6d}. "
              f"Average data rate: {rate:.1f} MB/s")

    def _send(self, socket, payload):
        if self._multipart:
            for i, item in enumerate(payload):
                if i == len(payload) - 1:
                    socket.send(item)
                else:
                    socket.send(item, zmq.SNDMORE)
                self._bytes_sent += sys.getsizeof(item)
        else:
            socket.send(payload)
            self._bytes_sent += sys.getsizeof(payload)

        self._counter += 1
        if self._report_every > 0 and self._counter % self._report_every == 0:
            self._report()

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
        if self._counter > 0:
            self._report()
        self._counter = 0
        self._t0 = time.time()
        self._bytes_sent = 0

    def reset_counter(self) -> None:
        self._buffer.put(_reset_counter_sentinel)
