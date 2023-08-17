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


def gen_fake_data(scan_index, n, *, shape, ordered):
    darks = [np.random.randint(500, size=shape, dtype=np.uint16)
             for _ in range(10)]
    whites = [3596 + np.random.randint(500, size=shape, dtype=np.uint16)
              for _ in range(10)]
    projections = [np.random.randint(4096, size=shape, dtype=np.uint16)
                   for _ in range(10)]

    for i in gen_index(0, n, ordered=ordered):
        meta = create_meta(scan_index, i, shape)
        if scan_index == 0:
            data = darks[np.random.choice(len(darks))]
        elif scan_index == 1:
            data = whites[np.random.choice(len(whites))]
        else:
            data = projections[np.random.choice(len(projections))]

        yield meta, data


def stream_data_file(filepath,  scan_index, *, start, end, ordered):
    p_dark = "/exchange/data_dark"
    p_flats = "/exchange/data_white"
    p_projections = "/exchange/data"

    with h5py.File(filepath, "r") as fp:
        print(f"Data shapes - "
              f"dark: {fp[p_dark].shape}, "
              f"flat: {fp[p_flats].shape}, "
              f"projection: {fp[p_projections].shape}")

        if scan_index == 0:
            ds = fp[p_dark]
        elif scan_index == 1:
            ds = fp[p_flats]
        elif scan_index == 2:
            ds = fp[p_projections]
        else:
            raise ValueError(f"Unsupported scan_index: {scan_index}")

        shape = ds.shape[1:]
        n_images = ds.shape[0]
        if start == end:
            end = start + n_images

        for i in gen_index(start, end, ordered=ordered):
            meta = create_meta(scan_index, i, shape)
            # Repeating reading data from chunks if data size is smaller
            # than the index range.
            data = np.zeros(shape, dtype=np.uint16)
            ds.read_direct(data, np.s_[i % n_images, ...], None)

            yield meta, data


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
    return name


def main():
    parser = argparse.ArgumentParser(
        description='Fake GigaFrost Data Stream',
        formatter_class=argparse.RawTextHelpFormatter
    )

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
                             )
    parser.add_argument('--datafile-root', type=str,
                        default="/das/work/p19/p19730/recastx_example_data",
                        help="Root directory of the data file")

    args = parser.parse_args()

    datafile = parse_datafile(args.datafile, args.datafile_root)

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
            if scan_index == 0:
                print("Streaming darks ...")
            elif scan_index == 1:
                print("Streaming flats ...")
            else:
                print("Streaming projections ...")

            if not datafile:
                if n == 0:
                    n = 500
                gen = gen_fake_data(scan_index, n,
                                    shape=(args.rows, args.cols),
                                    ordered=not args.unordered)
            else:
                if scan_index == 2:
                    gen = stream_data_file(datafile, scan_index,
                                           start=args.start,
                                           end=args.start + n,
                                           ordered=not args.unordered)
                else:
                    gen = stream_data_file(datafile, scan_index,
                                           start=0,
                                           end=n,
                                           ordered=not args.unordered)

            for item in gen:
                streamer.feed(item)

            if scan_index < 2:
                streamer.reset_counter()


if __name__ == "__main__":
    main()
