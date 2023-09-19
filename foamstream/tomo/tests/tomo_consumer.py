import json
import numpy as np

from foamclient import ZmqConsumer


def unpack(item: tuple):
    meta, data = item
    meta = json.loads(meta.bytes)
    data = np.frombuffer(data.bytes, dtype=meta["type"]).reshape(meta["shape"])
    return meta, data


if __name__ == "__main__":
    with ZmqConsumer(f"tcp://localhost:9667",
                     deserializer=unpack,
                     sock="PULL",
                     multipart=True) as consumer:
        for _ in range(1000):
            print(consumer.next())
