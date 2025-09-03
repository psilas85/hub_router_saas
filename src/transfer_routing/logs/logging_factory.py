# transfer_routing/logs/logging_factory.py

import logging
import os


class LoggerFactory:
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

            # File handler
            os.makedirs("logs", exist_ok=True)
            file_handler = logging.FileHandler(f"logs/{name}.log")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger
