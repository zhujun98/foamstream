# foamstream

[![PyPi](https://img.shields.io/pypi/v/foamstream.svg)](https://pypi.org/project/foamstream/)
![Build status](https://github.com/zhujun98/foamstream/actions/workflows/python-package.yml/badge.svg)

**foamstream** facilitates streaming data acquired at scientific user facilities from, typically 
[HDF5](https://www.hdfgroup.org/solutions/hdf5/), files.

## Tomography

```sh
foamstream-tomo --datafile <Your/file/path>
```

## EuXFEL

Start the Qt GUI, which is also implemented in 
[EXtra-foam](https://extra-foam.readthedocs.io/en/latest/tutorial_file_stream.html) with
```sh
foamstream-euxfel
```