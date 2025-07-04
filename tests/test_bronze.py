import json
from pathlib import Path

import pandas as pd
import pytest
from unittest.mock import MagicMock

import src.bronze as bronze_module

def test_fetch_all_breweries_pagination(monkeypatch):
    # Prepara os objetos MagicMock para simular as respostas HTTP #
    resp1 = MagicMock()
    resp1.raise_for_status.return_value = None              # sem exceção
    resp1.json.return_value = [{"id": 1}, {"id": 2}]       # primeiro batch

    resp2 = MagicMock()
    resp2.raise_for_status.return_value = None              # sem exceção
    resp2.json.return_value = []                            # segundo batch vazio → encerra paginação

    # Cola os mocks numa lista para serem consumidos em sequência #
    calls = [resp1, resp2]

    # Stub de requests.get que retorna os mocks em ordem #
    def fake_get(*args, **kwargs):
        return calls.pop(0)

    # Substitui requests.get dentro do módulo bronze pelo nosso fake_get
    monkeypatch.setattr(bronze_module.requests, "get", fake_get)

    # Chama a função real e verifica o resultado final
    result = bronze_module.fetch_all_breweries(page_size=2)
    assert isinstance(result, list)
    assert result == [{"id": 1}, {"id": 2}]


def test_main_writes_json(tmp_path, monkeypatch):
    # Override do RAW_PATH para usar tmp_path isolado
    fake_raw = tmp_path / "bronze"
    monkeypatch.setattr(bronze_module, "RAW_PATH", fake_raw)

    # Mocka fetch_all_breweries para não chamar HTTP e retornar dados fixos
    sample = [{"id": 42, "name": "Test Brewery"}]
    monkeypatch.setattr(bronze_module, "fetch_all_breweries", lambda **kw: sample)

    # Executa o main
    bronze_module.main()

    # Verifica que exatamente um arquivo JSON Lines foi criado
    files = list(fake_raw.glob("breweries_*.json"))
    assert len(files) == 1

    # Lê o JSON Lines e valida colunas e valores
    df = pd.read_json(files[0], lines=True)
    assert {"id", "name", "ingest_timestamp"}.issubset(df.columns)
    assert df.loc[0, "id"] == 42
    assert df.loc[0, "name"] == "Test Brewery"
    pd.to_datetime(df.loc[0, "ingest_timestamp"])