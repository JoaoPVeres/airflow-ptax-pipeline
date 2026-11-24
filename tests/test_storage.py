import json
from datetime import date

import pytest

from ptax.storage import caminho_arquivo, ler_json, salvar_json


def test_caminho_usa_particao_hive_e_moeda_maiuscula(tmp_path):
    caminho = caminho_arquivo(str(tmp_path), "usd", date(2026, 11, 9))
    assert caminho == tmp_path / "ptax" / "dt=2026-11-09" / "USD.json"


def test_salvar_e_ler_devolve_o_mesmo_conteudo(tmp_path):
    dados = {"value": [{"tipoBoletim": "Fechamento PTAX", "cotacaoVenda": 5.5146}]}
    caminho = salvar_json(dados, str(tmp_path), "USD", date(2026, 11, 9))
    assert ler_json(caminho) == dados


def test_salvar_duas_vezes_nao_duplica_nem_deixa_temporario(tmp_path):
    for _ in range(2):
        salvar_json({"value": []}, str(tmp_path), "USD", date(2026, 11, 9))
    pasta = tmp_path / "ptax" / "dt=2026-11-09"
    assert sorted(p.name for p in pasta.iterdir()) == ["USD.json"]


def test_moeda_vazia_ou_com_barra_e_rejeitada(tmp_path):
    with pytest.raises(ValueError):
        caminho_arquivo(str(tmp_path), "", date(2026, 11, 9))
    with pytest.raises(ValueError):
        caminho_arquivo(str(tmp_path), "../USD", date(2026, 11, 9))


def test_ler_arquivo_inexistente_levanta_erro(tmp_path):
    with pytest.raises(FileNotFoundError):
        ler_json(str(tmp_path / "nao_existe.json"))