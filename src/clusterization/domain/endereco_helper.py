import re

class EnderecoHelper:

    @staticmethod
    def preprocessar(endereco: str) -> str:
        if not isinstance(endereco, str) or not endereco.strip():
            return "Desconhecido"

        endereco = endereco.strip()
        endereco = re.sub(r'\b0\b', 'SN', endereco, flags=re.IGNORECASE)
        endereco = re.sub(r'\s+', ' ', endereco)
        endereco = re.sub(r'[^a-zA-ZÀ-ÖØ-öø-ÿ0-9,.\- ]', '', endereco)
        parts = endereco.split(",")

        logradouro = parts[0].strip() if len(parts) > 0 else ""
        cidade = parts[-2].strip() if len(parts) > 1 else ""
        uf = parts[-1].strip().upper() if len(parts) > 2 else ""

        return f"{logradouro}, {cidade}, {uf}".strip()

    @staticmethod
    def montar_endereco_completo(rua: str, numero: str, cidade: str, uf: str) -> str:
        endereco = f"{rua} {numero}, {cidade} {uf}"
        endereco = re.sub(r'\b\d{5,}\b', '', endereco)
        endereco = " ".join(dict.fromkeys(endereco.split()))
        return EnderecoHelper.preprocessar(endereco)