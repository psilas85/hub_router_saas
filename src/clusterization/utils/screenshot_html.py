# clusterization/utils/screenshot_html.py

import os
import imgkit

def capturar_screenshot_html(caminho_html: str, caminho_saida_png: str, largura=1200, altura=800):
    """
    Converte um arquivo HTML local em PNG usando wkhtmltoimage (sem ChromeDriver/Selenium).
    Requer que o pacote wkhtmltopdf/wkhtmltoimage esteja instalado no container.
    """
    if not os.path.exists(caminho_html):
        raise FileNotFoundError(f"Arquivo HTML não encontrado: {caminho_html}")

    caminho_wkhtml = '/usr/bin/wkhtmltoimage'
    if not os.path.exists(caminho_wkhtml):
        raise FileNotFoundError(
            f"❌ O executável wkhtmltoimage não foi encontrado em {caminho_wkhtml}. "
            "Instale no container: apt-get update && apt-get install -y wkhtmltopdf"
        )

    # Configuração do imgkit
    config = imgkit.config(wkhtmltoimage=caminho_wkhtml)
    options = {
        'width': largura,
        'height': altura,
        'quality': 90
    }

    imgkit.from_file(caminho_html, caminho_saida_png, config=config, options=options)
    print(f"📸 Screenshot salva em: {caminho_saida_png}")
    return caminho_saida_png
