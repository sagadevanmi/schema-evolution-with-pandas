import logging


def set_logger():
    """Create logging object"""
    MSG_FORMAT = "%(asctime)s.%(msecs)03d;%(levelname)s;%(message)s"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
    logger = logging.getLogger("glue")

    logger.setLevel(logging.INFO)
    return logger
