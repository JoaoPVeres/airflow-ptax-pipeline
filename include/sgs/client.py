import requests
import logging

from datetime import date 
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

def buscar_serie(serie: int, data_inicial: date, data_final: date, base_url: str = "https://api.bcb.gov.br") -> list[dict]:

    url = f"{base_url.rstrip('/')}/dados/serie/bcdata.sgs.{serie}/dados"

    politica_retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=frozenset(['GET']),
        respect_retry_after_header=True
    )

    parametros = {
        "formato": "json",
        "dataInicial": f"{data_inicial:%d/%m/%Y}",                    
        "dataFinal": f"{data_final:%d/%m/%Y}"                    
    }
    
    with requests.Session() as sessao:
        sessao.mount('https://', HTTPAdapter(max_retries=politica_retry))

        resposta = sessao.get(
            url=url,
            params=parametros,
            timeout=(5, 30)
        )

        if resposta.status_code == 404 and "Value(s) not found" in resposta.text:
            logger.info("SGS %s %s a %s: sem valores no periodo", serie, data_inicial, data_final)
            return []
        
        resposta.raise_for_status()

        try:
            dados = resposta.json()
        except ValueError as e:
            raise ValueError('Json invalido!') from e

        if not isinstance(dados, list):
            raise ValueError(f"Formato invalido!: {str(dados)[:200]}")

    logger.info("SGS %s %s: %d registro(s)", serie, data_inicial, len(dados))
    return dados