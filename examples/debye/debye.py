"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import argparse
from pathlib import Path
import time

from fastavro.schema import load_schema
import numpy as np

from foamstream import Streamer


def load_data():
    folder = Path(__file__).parent.joinpath("data")
    data_file = "Cu2O-1Hz-1MHz.npy"
    encoder_file = "Cu2O-1Hz-1MHz_Encoder.npy"

    samples = np.load(str(folder.joinpath(data_file)))
    encoder = np.load(str(folder.joinpath(encoder_file)))
    return samples, encoder


def main():
    parser = argparse.ArgumentParser(description='Debye data Stream')

    parser.add_argument('--port', default="45454", type=int,
                        help="ZMQ socket port (default=45454)")
    parser.add_argument('--sock', default='pub', type=str,
                        help="ZMQ socket type (default=PUB)")
    parser.add_argument('--frequency', default=0, type=float,
                        help="Data sent frequency")
    parser.add_argument('--repeat', action='store_true',
                        help="Repeat data streaming when reaching the end of "
                             "the dataset")

    args = parser.parse_args()

    schema = load_schema(Path(__file__).parent.joinpath("debye"))

    with Streamer(args.port,
                  sock=args.sock,
                  serializer="avro",
                  schema=schema,
                  frequency=args.frequency) as streamer:
        samples, encoder = load_data()
        npts = 1000000
        n = int(samples.shape[1] / npts)
        idx = 0
        print("Start data streaming ...")
        while True:
            for i in range(n):
                idx += 1
                streamer.feed({
                    "index": idx,  # for debug
                    "samples": samples[:, i * npts:(i + 1) * npts],
                    "encoder": encoder[i * npts:(i + 1) * npts]
                })

            if not args.repeat:
                break


if __name__ == "__main__":
    main()
