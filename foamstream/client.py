"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
import pickle
from typing import Optional

import zmq


class ZmqClient:
    """Client using the ZeroMQ protocols.

    It uses pickle to serialize and deserialize data.
    """
    def __init__(self, endpoint: str, timeout: Optional[float] = None):
        """Initialization

        :param endpoint: Server endpoint.
        :param timeout: Connection timeout in seconds.
        """

        self._ctx = zmq.Context()

        self._socket = self._ctx.socket(zmq.REQ)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(endpoint)

        if timeout is not None:
            self._socket.setsockopt(zmq.RCVTIMEO, int(timeout * 1000))
        self._recv_ready = False

    def next(self):
        """Request next data container.

        This function call is blocking.

        :return: data, meta
        :rtype: dict, dict

        :raise TimeoutError: If timeout is reached before receiving data.
        """
        if not self._recv_ready:
            self._socket.send(b'')
            self._recv_ready = True

        try:
            msg = self._socket.recv(copy=False)
        except zmq.error.Again:
            raise TimeoutError(
                'No data received from {} in the last {} ms'.format(
                    self._socket.getsockopt_string(zmq.LAST_ENDPOINT),
                    self._socket.getsockopt(zmq.RCVTIMEO)))
        self._recv_ready = False
        return pickle.loads(msg)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._ctx.destroy(linger=0)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()
