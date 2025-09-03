import logging
import os
import sys


class LoggerFactory:
    @staticmethod
    def get_logger(name="routing"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            os.makedirs("logs", exist_ok=True)

            file_handler = logging.FileHandler(
                f"logs/{name}.log", encoding="utf-8"
            )
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

        return logger
