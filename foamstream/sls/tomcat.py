"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import argparse
from collections import deque
from queue import Queue
from threading import Thread
import time

import numpy as np
import zmq
import h5py

from ..streamer import Streamer


sentinel = object()
frange = None


def shuffled_range(start, end):
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


def send(socket, queue):
    t0 = time.time()
    t_start = t0
    byte_sent = 0
    total_byte_sent = 0
    counter = 0
    to_mb = 1024. * 1024.
    print_every = 100
    while True:
        meta, data = queue.get()

        if data is sentinel:
            break

        socket.send_json(meta, flags=zmq.SNDMORE)
        socket.send(data)

        counter += 1
        byte_sent += data.nbytes
        total_byte_sent += data.nbytes
        if counter % print_every == 0:
            print(f"Sent type {meta['image_attributes']['scan_index']}, "
                  f"frame {meta['frame']}")

            dt = time.time() - t0
            print(f"Number of data sent: {counter:>6d}, "
                  f"throughput: {byte_sent / dt / to_mb:>6.1f} MB/s")
            t0 = time.time()
            byte_sent = 0

    dt = time.time() - t_start
    print(f"Total number of data sent: {counter:>6d}, "
          f"average throughput: {total_byte_sent / dt / to_mb:>6.1f} MB/s")


def gen_meta(scan_index, frame_index, shape):
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

    for i in frange(0, n):
        meta = gen_meta(scan_index, i, shape)
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

        for i in frange(start, end):
            meta = gen_meta(scan_index, i, shape)
            # Repeating reading data from chunks if data size is smaller
            # than the index range.
            data = np.zeros(shape, dtype=np.uint16)
            ds.read_direct(data, np.s_[i % n_images, ...], None)

            yield meta, data


def parse_datafile(name: str):
    if name in ["pet1", "pet2", "pet3"]:
        # number of projections per scan: 400, 500, 500
        idx = name[-1]
        return f"/sls/X02DA/Data10/e16816/disk1/PET_55um_40_{idx}/PET_55um_40_{idx}.h5"
    if name == "asm":
        # number of projections per scan: 400
        return f"/sls/X02DA/Data10/e16816/disk1/15_ASM_UA_ASM/15_ASM_UA_ASM.h5"
    if name == "h1":
        # number of projections per scan: 500
        return f"/sls/X02DA/Data10/e16816/disk1/32_050_300_H1/32_050_300_H1.h5"
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

    with Streamer(args.port,
                  serializer=lambda x: x,
                  sock=args.sock) as streamer:

        global frange
        frange = range if args.ordered else shuffled_range

        if datafile:
            print(f"Streaming data from {datafile} ...")
        else:
            print("Streaming randomly generated data ...")

        for scan_index, n in enumerate([args.darks, args.flats, args.projections]):
            if not datafile:
                item = gen_fake_data(scan_index, n, shape=(args.rows, args.cols))
            else:
                if scan_index == 2:
                    item = stream_data_file(datafile, scan_index,
                                            start=args.start,
                                            end=args.start + n)
                else:
                    item = stream_data_file(datafile, scan_index,
                                            start=0,
                                            end=n)

            streamer.feed(item)


if __name__ == "__main__":
    main()
