import os
import tempfile
from pathlib import Path

# 必须在导入 app 之前设置
_tmp = Path(tempfile.mkdtemp())
os.environ.setdefault("DATABASE_PATH", str(_tmp / "test.db"))
os.environ.setdefault("DECIMAL_SCALE", "9")
os.environ.setdefault("PROPORTION_SCALE", "6")

import pytest
from fastapi.testclient import TestClient

from app import service
from app.db import connect, init_db
from app.main import app
from decimal import Decimal


@pytest.fixture(scope="session", autouse=True)
def _init():
    init_db()
    yield


@pytest.fixture(autouse=True)
def _clean_tables():
    conn = connect()
    try:
        conn.execute("DELETE FROM components")
        conn.execute("DELETE FROM containers")
        yield
    finally:
        conn.close()


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c
