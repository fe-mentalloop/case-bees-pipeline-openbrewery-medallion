import os
import shutil
import pytest

@pytest.fixture(autouse=True)
def clear_datalake():
    # Antes e depois de cada teste, limpa o datalake/
    if os.path.exists("datalake"):
        shutil.rmtree("datalake")
    yield
    if os.path.exists("datalake"):
        shutil.rmtree("datalake")