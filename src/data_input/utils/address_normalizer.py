#hub_router_1.0.1/src/data_input/utils/address_normalizer.py

import unicodedata
import re


import unicodedata
import re


def normalize_address(addr: str) -> str:

    if not addr:
        return None

    addr = str(addr).strip()

    # remove acento
    addr = unicodedata.normalize("NFKD", addr)
    addr = "".join(c for c in addr if not unicodedata.combining(c))

    # uppercase
    addr = addr.upper()

    # remove lixo comum
    addr = addr.replace(" BRASIL", "")

    # mantém vírgula, hífen e número
    addr = re.sub(r"[^A-Z0-9,\- ]", " ", addr)

    # 🔥 remove vírgula duplicada
    addr = re.sub(r",\s*,+", ",", addr)

    # 🔥 remove espaço antes da vírgula
    addr = re.sub(r"\s+,", ",", addr)

    # 🔥 garante espaço após vírgula
    addr = re.sub(r",(\S)", r", \1", addr)

    # normaliza espaços
    addr = re.sub(r"\s+", " ", addr).strip()

    return addr