"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
from pathlib import Path
import re
from string import Template
from typing import Union

import h5py


def create_next_run_folder(parent: Union[str, Path]) -> Path:
    """Create and return the next run folder to store the output data."""
    parent = Path(parent)
    # It is allowed to use an existing parent directory,
    # but not a run folder.
    parent.mkdir(exist_ok=True)

    next_run_index = 1  # starting from 1
    for d in parent.iterdir():
        # Here d could also be a file
        if re.search(fr'r\d{{4}}', d.name):
            seq = int(d.name[1:])
            if seq >= next_run_index:
                next_run_index = seq + 1

    next_output_dir = parent.joinpath(f'r{next_run_index:04d}')
    next_output_dir.mkdir(parents=True, exist_ok=False)
    return next_output_dir


class Writer:

    __name_template = Template("$namespace-$topic-$run-$seq.h5")

    def __init__(self, parent_directory: Union[str, Path], *,
                 max_events_per_file: int = 1000):
        run_directory = create_next_run_folder(parent_directory)
        self._filepath = Path(run_directory).resolve()

        self._max_events_per_file = max_events_per_file

        self._fp = None
        self._index = 0
        self._file_count = 0

        self._group_name = "exchange"

    def write(self, data: dict, schema: dict) -> None:
        if self._index % self._max_events_per_file == 0:
            self.close()
            self._touch_next_file(data, schema)
            self._index = 0

        for k, v in data.items():
            self._fp[self._group_name][k][self._index] = v

        self._index += 1

    def _touch_next_file(self, data, schema):
        filepath = self._filepath.joinpath(self.__name_template.substitute(
            namespace=schema["namespace"].lower(),
            topic=schema["name"].lower(),
            run=self._filepath.parts[-1].lower(),
            seq=f"{self._file_count:06d}"))
        self._fp = h5py.File(filepath, 'w-')
        self._file_count += 1

        # init dataset
        for item in schema["fields"]:
            name = item['name']
            if item['type'] == 'record':
                if item['logicalType'] == 'ndarray':
                    shape = data[name].shape
                    self._fp.create_dataset(
                        f"{self._group_name}/{name}",
                        shape=(self._max_events_per_file, *shape),
                        dtype=data[name].dtype,
                        chunks=(1, *shape),
                        maxshape=(self._max_events_per_file, *shape)
                    )
            else:
                self._fp.create_dataset(
                    f"{self._group_name}/{name}",
                    shape=(self._max_events_per_file,),
                    dtype=item['type'],
                    chunks=True,
                    maxshape=(self._max_events_per_file,)
                )

    def close(self) -> None:
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
