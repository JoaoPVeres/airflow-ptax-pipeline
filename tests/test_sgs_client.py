from datetime import date

import pytest
import requests

from sgs.client import buscar_serie


class RespostaFalsa:
    def __init__(self, status_code=200, corpo=None, texto=""):
        self.status_code = status_code
        self._corpo = corpo
        self.text = texto

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} erro simulado")

    def json(self):
        if self._corpo is None:
            raise ValueError("sem json")
        return self._corpo


def _simular(monkeypatch, resposta):
    monkeypatch.setattr(requests.Session, "get", lambda self, *a, **k: resposta)


def test_serie_com_valores_devolve_a_lista(monkeypatch):
    lista = [{"data": "09/11/2026", "valor": "0.050788"}]
    _simular(monkeypatch, RespostaFalsa(200, lista))
    assert buscar_serie(11, date(2026, 11, 9), date(2026, 11, 9)) == lista


def test_404_valores_nao_encontrados_vira_lista_vazia(monkeypatch):
    texto = '{"erro":{"statusCode":404,"detail":"SGSNegocioException: Value(s) not found"}}'
    _simular(monkeypatch, RespostaFalsa(404, None, texto))
    assert buscar_serie(11, date(2026, 11, 8), date(2026, 11, 8)) == []


def test_404_de_outro_tipo_continua_sendo_erro(monkeypatch):
    _simular(monkeypatch, RespostaFalsa(404, None, "pagina nao encontrada"))
    with pytest.raises(requests.HTTPError):
        buscar_serie(999, date(2026, 11, 9), date(2026, 11, 9))


def test_formato_inesperado_levanta_erro(monkeypatch):
    _simular(monkeypatch, RespostaFalsa(200, {"value": []}))
    with pytest.raises(ValueError):
        buscar_serie(11, date(2026, 11, 9), date(2026, 11, 9))