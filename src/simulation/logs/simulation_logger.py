# logs/simulation_logger.py
import logging
from logging.handlers import TimedRotatingFileHandler
import os

def configurar_logger(nome_logger="simulation", nivel=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(nome_logger)
    logger.setLevel(nivel)
    logger.propagate = False  # Evita logs duplicados

    if not logger.handlers:
        # Criar pasta de logs se necessário
        os.makedirs("logs/output", exist_ok=True)

        # Console Handler
        ch = logging.StreamHandler()
        ch.setLevel(nivel)
        ch_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(ch_formatter)

        # File Handler com rotacionamento diário
        fh = TimedRotatingFileHandler("logs/output/simulation.log", when="midnight", backupCount=10, encoding='utf-8')
        fh.setLevel(nivel)
        fh_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(fh_formatter)

        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger
