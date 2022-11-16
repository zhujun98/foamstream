"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import logging


def create_logger():
    """Create the logger object for the whole API."""
    _logger = logging.getLogger("foamstream")

    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    stream_handler.setFormatter(formatter)

    _logger.addHandler(stream_handler)

    return _logger


logger = create_logger()
logger.setLevel(logging.INFO)
