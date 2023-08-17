"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import argparse
from collections import deque
import json

import numpy as np
import h5py

from ..streamer import Streamer


def gen_index(start: int, end: int, ordered: bool = True):
    if ordered:
        yield from range(start, end)

    prob = [0.2, 0.2, 0.1, 0.1, 0.1, 0.1, 0.05, 0.05, 0.05, 0.05]
    q = deque()
    for i in range(start, end):
        if len(q) == len(prob):
            idx = np.random.choice(q, p=prob)
            q.remove(idx)
            yield int(idx)
        q.append(i)

    np.random.shuffle(q)
    for idx in q:
        yield idx


def create_meta(scan_index, frame_index, shape):
    return {
        'image_attributes': {
            'scan_index': scan_index,
            'image_is_complete': True,
        },
        'frame': frame_index,
        'source': 'gigafrost',
        'shape': shape,
        'type': 'uint16'
    }


def gen_fake_data(scan_index, n, *, shape):
    darks = [np.random.randint(500, size=shape, dtype=np.uint16)
             for _ in range(10)]
    whites = [3596 + np.random.randint(500, size=shape, dtype=np.uint16)
              for _ in range(10)]
    projections = [np.random.randint(4096, size=shape, dtype=np.uint16)
                   for _ in range(10)]

    if n == 0:
        n = 500

    for i in gen_index(0, n):
        meta = create_meta(scan_index, i, shape)
        if scan_index == 0:
            data = darks[np.random.choice(len(darks))]
        elif scan_index == 1:
            data = whites[np.random.choice(len(whites))]
        else:
            data = projections[np.random.choice(len(projections))]

        yield meta, data


def stream_data_file(filepath,  scan_index, *,
                     start, end):
    with h5py.File(filepath, "r") as fp:
        if scan_index == 0:
            ds = fp["/exchange/data_dark"]
            print(f"Dark data shape: {ds.shape}")
        elif scan_index == 1:
            ds = fp["/exchange/data_white"]
            print(f"Flat data shape: {ds.shape}")
        elif scan_index == 2:
            ds = fp["/exchange/data"]
            print(f"Projection data shape: {ds.shape}")
        else:
            raise ValueError(f"Unsupported scan_index: {scan_index}")

        shape = ds.shape[1:]
        n_images = ds.shape[0]
        if start == end:
            end = start + n_images

        for i in gen_index(start, end):
            meta = create_meta(scan_index, i, shape)
            # Repeating reading data from chunks if data size is smaller
            # than the index range.
            data = np.zeros(shape, dtype=np.uint16)
            ds.read_direct(data, np.s_[i % n_images, ...], None)

            yield meta, data


def parse_datafile(name: str) -> str:
    p19730 = "/das/work/p19/p19730/recastx_example_data"
    if name in ["pet1", "pet2", "pet3"]:
        # number of projections per scan: 400, 500, 500
        # "/sls/X02DA/Data10/e16816/disk1/PET_55um_40_{idx}/PET_55um_40_{idx}.h5
        idx = name[-1]
        return f"{p19730}/e16816/PET_55um_40_{idx}/PET_55um_40_{idx}.h5"
    if name == "asm":
        # number of projections per scan: 400
        # /sls/X02DA/Data10/e16816/disk1/15_ASM_UA_ASM/15_ASM_UA_ASM.h5
        return f"{p19730}/e16816/15_ASM_UA_ASM.h5"
    if name == "h1":
        # number of projections per scan: 500
        # /sls/X02DA/Data10/e16816/disk1/32_050_300_H1/32_050_300_H1.h5
        return f"{p19730}/e16816/32_050_300_H1.h5"
    return name


def main():
    parser = argparse.ArgumentParser(description='Fake GigaFrost Data Stream')

    parser.add_argument('--port', default="9667", type=int,
                        help="ZMQ socket port (default=9667)")
    parser.add_argument('--sock', default='push', type=str,
                        help="ZMQ socket type (default=PUSH)")
    parser.add_argument('--darks', default=20, type=int,
                        help="Number of dark images (default=20)")
    parser.add_argument('--flats', default=20, type=int,
                        help="Number of flat images (default=20)")
    parser.add_argument('--projections', default=0, type=int,
                        help="Number of projection images (default=0, i.e. "
                             "the whole projection dataset when streaming from files or "
                             "500 otherwise")
    parser.add_argument('--start', default=0, type=int,
                        help="Starting index of the projection images (default=0)")
    parser.add_argument('--ordered', action='store_true',
                        help="Send out images with frame IDs in order. "
                             "Note: enable ordered frame IDs to achieve higher throughput")
    parser.add_argument('--rows', default=1200, type=int,
                        help="Number of rows of the generated image (default=1200)")
    parser.add_argument('--cols', default=2016, type=int,
                        help="Number of columns of the generated image (default=2016)")
    parser.add_argument('--datafile', type=str,
                        help="Path or code of the data file. Available codes "
                             "with number of projection denoted in the bracket are: "
                             "pet1 (400), pet2 (500), pet3 (500), asm (400), h1 (500)")

    args = parser.parse_args()

    datafile = parse_datafile(args.datafile)

    def pack(item_: tuple):
        meta, data = item_
        return json.dumps(meta).encode("utf8"), data

    with Streamer(args.port,
                  serializer=pack,
                  multipart=True,
                  sock=args.sock) as streamer:

        if datafile:
            print(f"Streaming data from {datafile} ...")
        else:
            print("Streaming randomly generated data ...")

        for scan_index, n in enumerate([args.darks, args.flats, args.projections]):
            if not datafile:
                gen = gen_fake_data(scan_index, n, shape=(args.rows, args.cols))
            else:
                if scan_index == 2:
                    gen = stream_data_file(datafile, scan_index,
                                           start=args.start,
                                           end=args.start + n)
                else:
                    gen = stream_data_file(datafile, scan_index,
                                           start=0,
                                           end=n)

            for item in gen:
                streamer.feed(item)


if __name__ == "__main__":
    main()
