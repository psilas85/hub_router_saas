#hub_router_1.0.1/src/data_input/infrastructure/db_connection.py

import os
import time
import psycopg2
from psycopg2 import OperationalError, InterfaceError, DatabaseError
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DB_PARAMS = {
    "dbname": os.getenv("DB_DATABASE", "clusterization_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
    "application_name": "hub_router_data_input",
}


def get_connection(retries: int = 5, delay: int = 2, backoff: float = 1.5):

    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            conn.autocommit = False
            logger.info(f"✅ Conexão PostgreSQL estabelecida (tentativa {attempt})")
            return conn

        except OperationalError as e:
            wait = delay * (backoff ** (attempt - 1))
            logger.warning(f"⚠️ Erro conexão ({attempt}/{retries}): {e} — retry em {wait:.1f}s")
            time.sleep(wait)

        except Exception as e:
            logger.error(f"❌ Erro inesperado: {e}", exc_info=True)
            time.sleep(delay)

    raise ConnectionError("❌ Falha ao conectar no banco")


@contextmanager
def get_connection_context(retries: int = 3):

    conn = None

    try:
        conn = get_connection(retries=retries)
        yield conn
        conn.commit()

    except (OperationalError, InterfaceError) as e:
        if conn:
            conn.rollback()
        logger.error(f"💥 Erro conexão: {e}")
        raise

    except DatabaseError as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ Erro DB: {e}")
        raise

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"⚠️ Erro geral: {e}", exc_info=True)
        raise

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def test_db_connection():
    try:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except:
        return False