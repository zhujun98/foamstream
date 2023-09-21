"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import argparse
from pathlib import Path
import time

from faker import Faker
from fastavro.schema import load_schema
import numpy as np

from foamstream import Streamer


def main():
    parser = argparse.ArgumentParser(description='Datahouse data stream')

    parser.add_argument('--port', default="45454", type=int,
                        help="ZMQ socket port (default=45454)")
    parser.add_argument('--sock', default='pub', type=str,
                        help="ZMQ socket type (default=PUB)")
    parser.add_argument('--count', default=0, type=int,
                        help="Number of records to be sent. Infinite if count <=0")

    args = parser.parse_args()

    schema = load_schema(Path(__file__).parent.joinpath("datahouse"))

    fake = Faker()
    with Streamer(args.port,
                  serializer="avro",
                  schema=schema,
                  sock=args.sock) as streamer:
        print("Start data streaming ...")
        counter = 0
        while True:
            streamer.feed({
                "name": fake.name(),
                "age": np.random.randint(18, 100),
                "stress": np.random.randn(86400)
            })
            time.sleep(0.01)

            counter += 1
            if counter == args.count > 0:
                break


if __name__ == "__main__":
    main()
