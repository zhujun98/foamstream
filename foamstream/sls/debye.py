"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import argparse
from pathlib import Path

import numpy as np

from ..streamer import Streamer, SerializerType


def read_sample_data(sampling_rate: float = 1):
    foldername = Path.home().joinpath("Cu2O")
    if sampling_rate == 1:
        data_file = "Cu2O-1Hz-1MHz.npy"
        encoder_file = "Cu2O-1Hz-1MHz_Encoder.npy"
    elif sampling_rate == 2:
        data_file = "Cu2O-1Hz-2MHz.npy"
        encoder_file = "Cu2O-1Hz-2MHz_Encoder.npy"
    elif sampling_rate == 0.5:
        data_file = "Cu2O-1Hz-500kHz.npy"
        encoder_file = "Cu2O-1Hz-500kHz_Encoder.npy"
    else:
        raise ValueError

    samples = np.load(str(foldername.joinpath(data_file)))
    encoder = np.load(str(foldername.joinpath(encoder_file)))
    return samples, encoder


def main():
    parser = argparse.ArgumentParser(description='Fake GigaFrost Data Stream')

    parser.add_argument('--port', default="45454", type=int,
                        help="ZMQ socket port (default=45454)")
    parser.add_argument('--sock', default='push', type=str,
                        help="ZMQ socket type (default=PUSH)")

    args = parser.parse_args()

    with Streamer(args.port, SerializerType.SLS, sock=args.sock) as streamer:
        # The following parameters should be included in meta data
        data_rate = 1  # data rate in Hz
        sampling_rate = 1  # sampling rate in MHz

        samples, encoder = read_sample_data(sampling_rate)
        npts = sampling_rate * 1000000
        n = int(samples.shape[1] / npts)
        for i in range(n):
            streamer.feed({
                "samples": samples[:, i * npts:(i + 1) * npts],
                "encoder": encoder[i * npts:(i + 1) * npts]
            })


if __name__ == "__main__":
    main()
