import pytest
from tempfile import NamedTemporaryFile

import numpy as np
import h5py

from foamstream.tomo.app import (
    gen_fake_data, gen_index, sentinel, stream_data_file
)


def test_gen_index_ordered():
    ret = []
    for i in gen_index(0, 5):
        ret.append(i)
    assert ret == [0, 1, 2, 3, 4]


def test_gen_index_unordered():
    ret = []
    for i in gen_index(0, 5, ordered=False):
        ret.append(i)

    ret_gt = [0, 1, 2, 3, 4]
    assert ret != ret_gt
    assert len(ret) == 5
    for i in ret_gt:
        assert i in ret


IMAGE_SHAPE = (3, 4)
IMAGE_COUNTS = [2, 3, 4]


def check_data(meta, data, *, scan_index, frame_id):
    assert meta['image_attributes']['scan_index'] == scan_index
    assert meta['frame'] == frame_id
    assert meta['shape'] == IMAGE_SHAPE
    assert meta['type'] == "uint16"
    assert data.shape == IMAGE_SHAPE
    assert data.dtype == np.uint16


def check_result(ret, counts):
    assert len(ret) == np.sum(counts) + 2
    for i in range(counts[0]):
        check_data(*ret[i], frame_id=i, scan_index=0)
    assert ret[counts[0]] == (sentinel, None)
    for i in range(counts[1]):
        check_data(*ret[counts[0] + 1 + i], frame_id=i, scan_index=1)
    assert ret[np.sum(counts[:2]) + 1] == (sentinel, None)
    for i in range(counts[2]):
        check_data(*ret[np.sum(counts[:2]) + 2 + i], frame_id=i, scan_index=2)


def test_gen_fake_data():
    ret = []
    for item in gen_fake_data(IMAGE_COUNTS, shape=IMAGE_SHAPE, ordered=True):
        ret.append(item)
    check_result(ret, IMAGE_COUNTS)


def write_temp_file(filepath):
    with h5py.File(filepath, 'w') as fp:
        fp["darks"] = np.ones((IMAGE_COUNTS[0], ) + IMAGE_SHAPE)
        fp["flats"] = np.ones((IMAGE_COUNTS[1], ) + IMAGE_SHAPE)
        fp["projections"] = np.ones((IMAGE_COUNTS[2], ) + IMAGE_SHAPE)


def write_temp_file_rgb(filepath):
    with h5py.File(filepath, 'w') as fp:
        fp["darks"] = np.ones((IMAGE_COUNTS[0], ) + IMAGE_SHAPE + (3,))
        fp["flats"] = np.ones((IMAGE_COUNTS[1], ) + IMAGE_SHAPE + (3,))
        fp["projections"] = np.ones((IMAGE_COUNTS[2], ) + IMAGE_SHAPE + (3,))


@pytest.mark.parametrize("file_generator", [write_temp_file, write_temp_file_rgb])
def test_stream_data_file(file_generator):
    with NamedTemporaryFile(suffix=".h5") as tempfile:
        file_generator(tempfile.name)

        ret = []
        for item in stream_data_file(tempfile.name, [0, 0, 0],
                                     ordered=True,
                                     starts=[0, 0, 0],
                                     datapaths=["darks", "flats", "projections"]):
            ret.append(item)
        check_result(ret, IMAGE_COUNTS)

        # the request numbers of images are larger than the size of the datasets
        ret = []
        counts = [2 * x for x in IMAGE_COUNTS]
        for item in stream_data_file(tempfile.name, counts,
                                     ordered=True,
                                     starts=[0, 0, 0],
                                     datapaths=["darks", "flats", "projections"]):
            ret.append(item)
        check_result(ret, counts)
