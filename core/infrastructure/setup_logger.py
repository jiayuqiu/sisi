"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  setup_logger.py
@DateTime:  20/10/2024 3:48 pm
@DESC    :  set up logger
"""


import datetime
import logging
import os.path


DATE = datetime.datetime.now().strftime('%Y%m%d')


def set_logger(
    logger_name: str,
    log_file: str = f'logs/sisi_logger-{DATE}.log',
    level=logging.INFO
):
    """
    Sets up the logger for the project. This can be imported at any module for standard logging.

    :param logger_name:
    :param log_file:
    :param level:
    :return:
    """
    log_dir = "/".join(log_file.split("/")[:-1])
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(logger_name)

    formatter = logging.Formatter(
        '%(asctime)s, %(levelname)s %(filename)s, %(lineno)d: %(message)s')

    if log_file is not None:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.setLevel(level)
    logger.addHandler(stream_handler)
    return logger