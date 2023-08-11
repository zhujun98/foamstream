from abc import ABC, abstractmethod
import json

import fastavro
import numpy as np

from foamclient import AvroSchemaExt


class AbstractDataGenerator(ABC):

    schema = None

    def __init__(self):
        self._counter = 0

    @abstractmethod
    def next(self):
        ...


class StringDataGenerator(AbstractDataGenerator):
    def next(self):
        self._counter += 1
        return f"data{self._counter}"


class AvroDataGenerator(AbstractDataGenerator):
    schema = fastavro.parse_schema({
        "namespace": "unittest",
        "type": "record",
        "name": "test_data",
        "fields": [
            {
                "name": "index",
                "type": "long"
            },
            {
                "name": "name",
                "type": "string"
            },
            {
                "name": "array1d",
                "type": AvroSchemaExt.ndarray
            },
        ]
    })

    def next(self):
        data = {
            "index": self._counter,  # integer
            "name": f"data{self._counter}",  # string
            "array1d": np.ones(10, dtype=np.int32) * self._counter,
        }
        self._counter += 1
        return data


def assert_result_equal(left, right):
    if isinstance(left, dict):
        for k, v in left.items():
            assert k in right
            assert isinstance(right[k], type(v))
            if isinstance(v, np.ndarray):
                np.testing.assert_array_equal(v, right[k])
            else:
                assert_result_equal(v, right[k])
    else:
        assert left == right

