from abc import ABC, abstractmethod
import json

import avro.schema
import numpy as np


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
    schema = avro.schema.parse(json.dumps({
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
                "type": {
                    "type": "record",
                    "logicalType": "ndarray",
                    "name": "Array1D",
                    "fields": [
                        {"name": "shape", "type": {"items": "int", "type": "array"}},
                        {"name": "dtype", "type": "string"},
                        {"name": "data", "type": "bytes"}
                    ]
                }
            },
        ]
    }))

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
                assert v == right[k]
    else:
        assert left == right
