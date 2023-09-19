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


sentinel = object()


def index2string(index):
    if index == 0:
        return "DARK"
    if index == 1:
        return "FLAT"
    if index == 2:
        return "PROJECTION"
    raise ValueError(f"Unknown scan index: {index}")


def gen_index(start: int, end: int, *, ordered: bool = True):
    if ordered:
        yield from range(start, end)
    else:
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


def gen_fake_data(counts, *, shape, ordered):
    print("Streaming randomly generated data ...")

    darks = [np.random.randint(500, size=shape, dtype=np.uint16)
             for _ in range(10)]
    whites = [3596 + np.random.randint(500, size=shape, dtype=np.uint16)
              for _ in range(10)]
    projections = [np.random.randint(4096, size=shape, dtype=np.uint16)
                   for _ in range(10)]

    for scan_index, n in enumerate(counts):
        if n == 0:
            if scan_index in (0, 1):
                n = 10
            else:
                n = 400

        print(f"{index2string(scan_index)}: Image shape: {shape}. "
              f"Number of images: {n}")

        for i in gen_index(0, n, ordered=ordered):
            meta = create_meta(scan_index, i, shape)
            if scan_index == 0:
                data = darks[np.random.choice(len(darks))]
            elif scan_index == 1:
                data = whites[np.random.choice(len(whites))]
            else:
                data = projections[np.random.choice(len(projections))]

            yield meta, data

        if scan_index < 2:
            yield sentinel, None


def rgb2grayscale(src, dst):
    dst[...] = 0.2989 * src[..., 0] + 0.5870 * src[..., 1] + 0.1140 * src[..., 2]


def stream_data_file(datafile,  counts, *, ordered, starts, datapaths):
    with h5py.File(datafile, "r") as fp:
        print(f"Streaming data from {datafile} ...")
        for scan_index, (n, start, path) in enumerate(zip(counts, starts, datapaths)):
            if path not in fp:
                print(f"{index2string(scan_index)}: data not found")
                continue

            ds = fp[path]
            shape = ds.shape[1:]
            is_rgb = False
            if len(shape) == 3:
                is_rgb = True
                assert ds.shape[-1] == 3
            n_images = ds.shape[0]

            end = start + n
            if start == end:
                end = start + n_images

            print(f"{index2string(scan_index)}: Image shape: {shape}. "
                  f"Number of images: {end - start} ({n_images})")

            raw_data = np.zeros(shape, dtype=np.uint16)
            proc_data = np.zeros(shape[:2], dtype=np.uint16) if is_rgb else None
            for i in gen_index(start, end, ordered=ordered):
                meta = create_meta(scan_index, i, shape[:2])
                # Repeating reading data from chunks if data size is smaller
                # than the index range.
                ds.read_direct(raw_data, np.s_[i % n_images, ...], None)
                if is_rgb:
                    rgb2grayscale(raw_data, proc_data)
                else:
                    proc_data = raw_data

                yield meta, proc_data

            if scan_index < 2:
                yield sentinel, None


def parse_datafile(name: str, root: str) -> str:
    if name in ["pet1", "pet2", "pet3"]:
        # "/sls/X02DA/Data10/e16816/disk1/PET_55um_40_{idx}/PET_55um_40_{idx}.h5
        return f"{root}/e16816/PET_55um_40_{name[-1]}.h5"
    if name == "asm":
        # /sls/X02DA/Data10/e16816/disk1/15_ASM_UA_ASM/15_ASM_UA_ASM.h5
        return f"{root}/e16816/15_ASM_UA_ASM.h5"
    if name == "h1":
        # /sls/X02DA/Data10/e16816/disk1/32_050_300_H1/32_050_300_H1.h5
        return f"{root}/e16816/32_050_300_H1.h5"
    if name == "foam":
        return f"{root}/tomobank/dk_MCFG_1_p_s1_.h5"
    if name == "fuel0":
        return f"{root}/tomobank/fuelcell_dryHQ_i1.h5"
    if name in ["fuel1", "fuel2", "fuel3"]:
        return f"{root}/tomobank/fuelcell_i{name[-1]}.h5"
    if name == "beads":
        return f"{root}/tomobank/2_plastic_beeds_RGB.h5"
    return name


def main():
    parser = argparse.ArgumentParser(
        description='Tomography data streamer',
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument('--port', default="9667", type=int,
                        help="ZMQ socket port (default=9667)")
    parser.add_argument('--sock', default='push', type=str,
                        help="ZMQ socket type (default=PUSH)")
    parser.add_argument('--darks', default=0, type=int,
                        help="Number of dark images (default=0, i.e. "
                             "the whole dark dataset when streaming from files or "
                             "10 when generating fake data")
    parser.add_argument('--flats', default=0, type=int,
                        help="Number of flat images (default=0, i.e. "
                             "the whole flat dataset when streaming from files or "
                             "10 when generating fake data")
    parser.add_argument('--projections', default=0, type=int,
                        help="Number of projection images (default=0, i.e. "
                             "the whole projection dataset when streaming from files or "
                             "400 otherwise")
    parser.add_argument('--start', default=0, type=int,
                        help="Starting index of the projection images (default=0)")
    parser.add_argument('--unordered', action='store_true',
                        help="Send out images with frame IDs not in order. "
                             "Note: unordered frame IDs results in lower throughput")
    parser.add_argument('--rows', default=1200, type=int,
                        help="Number of rows of the generated image (default=1200)")
    parser.add_argument('--cols', default=2016, type=int,
                        help="Number of columns of the generated image (default=2016)")
    parser.add_argument('--datafile', type=str,
                        help="Path or code of the data file. Available codes are: \n"
                             "> pet1 - shape: 501 x 400 x 800 x 384, sample: PET_55um_40_1, source: TOMCAT (e16816)\n"
                             "> pet2 - shape: 201 x 500 x 1008 x 1008, sample: PET_55um_40_2, source: TOMCAT (e16816)\n"
                             "> pet3 - shape: 201 x 500 x 600 x 576, sample: PET_55um_40_3, source: TOMCAT (e16816)\n"
                             "> asm - shape: 1001 x 400 x 520 x 768, sample: 15_ASM_UA_ASM, source: TOMCAT (e16816)\n"
                             "> h1 - shape: 142 x 500 x 2016 x 288, sample: 32_050_300_H1, source: TOMCAT (e16816)\n"
                             "> foam - shape: 130 x 300 x 1800 x 2016, sample: dk_MCFG_1_p_s1, source: Tomobank\n"
                             "> fuel0 - shape: 1 x 1001 x 1100 x 1440, sample: fuelcell_dryHQ_i1, source: Tomobank\n"
                             "> fuel1 - shape: 60 x 301 x 1100 x 1440, sample: fuelcell_i1, source: Tomobank\n"
                             "> fuel2 - shape: 60 x 301 x 1100 x 1440, sample: fuelcell_i2, source: Tomobank\n"
                             "> fuel3 - shape: 60 x 301 x 1100 x 1440, sample: fuelcell_i3, source: Tomobank\n"
                             "> beads - shape: 200 x 440 x 130, sample: plastic bead, source: Tomobank\n"
                             )
    parser.add_argument('--datafile-root', type=str,
                        default="/das/work/p19/p19730/recastx_example_data",
                        help="Root directory of the data file")
    parser.add_argument('--pdata', type=str, default="/exchange/data")
    parser.add_argument('--pdark', type=str, default="/exchange/data_dark")
    parser.add_argument('--pflat', type=str, default="/exchange/data_white")

    args = parser.parse_args()

    datafile = parse_datafile(args.datafile, args.datafile_root)

    def pack(item_: tuple):
        meta, data = item_
        return json.dumps(meta).encode("utf8"), data

    with Streamer(args.port,
                  serializer=pack,
                  multipart=True,
                  sock=args.sock,
                  report_every=1000) as streamer:

        if datafile:
            gen = stream_data_file(datafile, [args.darks, args.flats, args.projections],
                                   ordered=not args.unordered,
                                   starts=[0, 0, args.start],
                                   datapaths=[args.pdark, args.pflat, args.pdata])
        else:
            gen = gen_fake_data([args.darks, args.flats, args.projections],
                                shape=(args.rows, args.cols),
                                ordered=not args.unordered)

        for item in gen:
            if item[0] is sentinel:
                streamer.reset_counter()
            else:
                streamer.feed(item)


if __name__ == "__main__":
    main()
