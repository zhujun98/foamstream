import logging


def create_logger(name: str):
    """Create the logger object for the whole API."""
    _logger = logging.getLogger(name)

    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(filename)s - %(levelname)s - %(message)s'
    )
    stream_handler.setFormatter(formatter)

    _logger.addHandler(stream_handler)

    return _logger


logger = create_logger("foamstream")
logger.setLevel(logging.INFO)
