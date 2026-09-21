"""POST /transfers/batch 幂等键（idempotency_key）端到端测试。"""
import sqlite3
import threading
from decimal import Decimal

from helpers import container, sample, volume_of

from app.config import settings
from app.db import connect
from app import service


def _batch(client, transfers, key=None, expected_versions=None, header=None):
    body = {"transfers": transfers}
    if key is not None:
        body["idempotency_key"] = key
    if expected_versions is not None:
        body["expected_versions"] = expected_versions
    headers = {"Idempotency-Key": header} if header is not None else None
    return client.post("/transfers/batch", json=body, headers=headers)


def _setup(client):
    sample(client, "S1", "10")
    container(client, "V1")


# --------------------------------------------------------------------------- #
# 同键同体：重放首次结果，不二次扣减、不递增 version
# --------------------------------------------------------------------------- #
def test_same_key_same_body_replays_first_response_without_double_deduction(client):
    _setup(client)
    payload = [{"source": "S1", "destination": "V1", "volume": "3"}]

    r1 = _batch(client, payload, key="txn-001")
    assert r1.status_code == 200, r1.text
    assert r1.json() == {
        "applied": 1,
        "message": "已依次提交 1 条转移",
        "versions": {"S1": 1, "V1": 1},
    }

    r2 = _batch(client, payload, key="txn-001")
    assert r2.status_code == 200
    # 重放：与首次响应逐字节（JSON）一致
    assert r2.json() == r1.json()

    r3 = _batch(client, payload, key="txn-001")
    assert r3.json() == r1.json()

    # 体积只扣减一次，version 只递增一次
    assert volume_of(client, "S1") == "7.000000000"
    assert volume_of(client, "V1") == "3.000000000"
    assert client.get("/containers/S1").json()["version"] == 1
    assert client.get("/containers/V1").json()["version"] == 1

    # 幂等记录仅一条
    conn = connect()
    try:
        rows = conn.execute("SELECT * FROM idempotent_requests").fetchall()
        assert len(rows) == 1
        assert rows[0]["idempotency_key"] == "txn-001"
        assert len(rows[0]["request_fingerprint"]) == 64  # SHA-256 十六进制
        assert rows[0]["status_code"] == 200
    finally:
        conn.close()


def test_replay_key_normalizes_whitespace(client):
    _setup(client)
    payload = [{"source": "S1", "destination": "V1", "volume": "1"}]
    r1 = _batch(client, payload, key="  k-1  ")
    r2 = _batch(client, payload, key="k-1")
    assert r1.status_code == r2.status_code == 200
    assert r2.json() == r1.json()
    assert volume_of(client, "S1") == "9.000000000"  # 只扣一次


# --------------------------------------------------------------------------- #
# 同键异体：409 idempotency_conflict，数据库不变
# --------------------------------------------------------------------------- #
def test_same_key_different_body_conflicts_and_changes_nothing(client):
    _setup(client)
    r1 = _batch(
        client, [{"source": "S1", "destination": "V1", "volume": "3"}], key="txn-002"
    )
    assert r1.status_code == 200

    # 同键但体积不同
    r2 = _batch(
        client, [{"source": "S1", "destination": "V1", "volume": "4"}], key="txn-002"
    )
    assert r2.status_code == 409
    err = r2.json()["error"]
    assert err["code"] == "idempotency_conflict"
    assert "txn-002" in err["message"]

    # 冲突不改变任何状态，也不覆盖已存响应
    assert volume_of(client, "S1") == "7.000000000"
    assert volume_of(client, "V1") == "3.000000000"
    r3 = _batch(
        client, [{"source": "S1", "destination": "V1", "volume": "3"}], key="txn-002"
    )
    assert r3.status_code == 200
    assert r3.json() == r1.json()
    assert volume_of(client, "S1") == "7.000000000"


def test_conflict_check_happens_inside_write_transaction_before_any_deduction(client):
    sample(client, "S1", "10")
    container(client, "V1")
    container(client, "V2")

    # 首次：S1 -> V1
    first = [{"source": "S1", "destination": "V1", "volume": "2"}]
    assert _batch(client, first, key="k").status_code == 200

    # 同键异体，且该异体本身会源液不足：必须以 409 冲突为准（指纹先判定），
    # 整批不执行、不留下扣减痕迹
    conflict = [
        {"source": "S1", "destination": "V2", "volume": "1"},
        {"source": "V1", "destination": "V2", "volume": "999"},
    ]
    r = _batch(client, conflict, key="k")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_conflict"
    assert volume_of(client, "S1") == "8.000000000"
    assert volume_of(client, "V1") == "2.000000000"
    assert volume_of(client, "V2") == "0"


# --------------------------------------------------------------------------- #
# 失败请求不写幂等记录：同键修正后可重试成功
# --------------------------------------------------------------------------- #
def test_failed_request_is_not_persisted_and_same_key_can_retry(client):
    sample(client, "S1", "5")
    container(client, "V1")
    bad = [{"source": "S1", "destination": "V1", "volume": "999"}]

    r = _batch(client, bad, key="txn-003")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "insufficient_volume"

    # 失败不写记录
    conn = connect()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM idempotent_requests WHERE idempotency_key = ?",
            ("txn-003",),
        ).fetchone()["c"] == 0
    finally:
        conn.close()

    # 同键换成合法请求体：正常执行（非重放）
    good = [{"source": "S1", "destination": "V1", "volume": "1"}]
    r = _batch(client, good, key="txn-003")
    assert r.status_code == 200
    assert r.json()["applied"] == 1
    assert volume_of(client, "S1") == "4.000000000"

    # 再次同键同体重放
    r = _batch(client, good, key="txn-003")
    assert r.status_code == 200
    assert r.json()["applied"] == 1
    assert volume_of(client, "S1") == "4.000000000"


# --------------------------------------------------------------------------- #
# 不携带幂等键：保持完全兼容（相同请求体可重复扣减）
# --------------------------------------------------------------------------- #
def test_request_without_key_stays_compatible(client):
    _setup(client)
    payload = [{"source": "S1", "destination": "V1", "volume": "1"}]
    r1 = _batch(client, payload)
    r2 = _batch(client, payload)
    assert r1.status_code == r2.status_code == 200
    # 无幂等键：两次都真实执行，各扣一次、各 +1
    assert volume_of(client, "S1") == "8.000000000"
    assert client.get("/containers/S1").json()["version"] == 2

    conn = connect()
    try:
        assert conn.execute("SELECT COUNT(*) AS c FROM idempotent_requests").fetchone()[
            "c"
        ] == 0
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Idempotency-Key 请求头
# --------------------------------------------------------------------------- #
def test_idempotency_key_via_header_replays(client):
    _setup(client)
    payload = [{"source": "S1", "destination": "V1", "volume": "2"}]
    r1 = _batch(client, payload, header="hdr-1")
    r2 = _batch(client, payload, header="hdr-1")
    assert r1.status_code == r2.status_code == 200
    assert r2.json() == r1.json()
    assert volume_of(client, "S1") == "8.000000000"
    assert client.get("/containers/V1").json()["version"] == 1


def test_header_and_body_with_same_value_share_record(client):
    _setup(client)
    payload = [{"source": "S1", "destination": "V1", "volume": "1"}]
    r1 = _batch(client, payload, key="dup")
    r2 = _batch(client, payload, header="dup")
    assert r2.json() == r1.json()
    assert volume_of(client, "S1") == "9.000000000"


def test_header_and_body_conflict_422(client):
    _setup(client)
    payload = [{"source": "S1", "destination": "V1", "volume": "1"}]
    r = _batch(client, payload, key="from-body", header="from-header")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_idempotency_key"
    # 未执行
    assert volume_of(client, "S1") == "10.000000000"


def test_blank_key_rejected_422(client):
    _setup(client)
    payload = [{"source": "S1", "destination": "V1", "volume": "1"}]

    r = _batch(client, payload, key="   ")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_idempotency_key"

    r = _batch(client, payload, header="   ")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_idempotency_key"

    assert volume_of(client, "S1") == "10.000000000"


# --------------------------------------------------------------------------- #
# 请求指纹语义：经校验规范化后等价的请求视为同体
# --------------------------------------------------------------------------- #
def test_fingerprint_equivalent_numeric_forms_replay(client):
    _setup(client)
    r1 = _batch(
        client, [{"source": "S1", "destination": "V1", "volume": "3"}], key="canon"
    )
    # JSON 数字 3 与字符串 "3" 在校验规范化后等价 -> 同指纹 -> 重放
    r2 = _batch(
        client, [{"source": "S1", "destination": "V1", "volume": 3}], key="canon"
    )
    assert r2.status_code == 200
    assert r2.json() == r1.json()
    assert volume_of(client, "S1") == "7.000000000"


def test_expected_versions_participates_in_fingerprint(client):
    _setup(client)
    base = [{"source": "S1", "destination": "V1", "volume": "1"}]

    r1 = _batch(client, base, key="ev", expected_versions={"S1": 0, "V1": 0})
    assert r1.status_code == 200

    # 同转移但 expected_versions 不同（且按当前状态会 stale）：请求体不同 -> 409，
    # 而非 stale_container；指纹比对先于业务执行
    r2 = _batch(client, base, key="ev", expected_versions={"S1": 5, "V1": 5})
    assert r2.status_code == 409
    assert r2.json()["error"]["code"] == "idempotency_conflict"

    # 与首次完全一致（即便版本早已过期）也重放首次结果，不触发 stale_container
    r3 = _batch(client, base, key="ev", expected_versions={"S1": 0, "V1": 0})
    assert r3.status_code == 200
    assert r3.json() == r1.json()
    assert client.get("/containers/S1").json()["version"] == 1


def test_different_keys_with_same_body_both_execute(client):
    _setup(client)
    payload = [{"source": "S1", "destination": "V1", "volume": "1"}]
    r1 = _batch(client, payload, key="a")
    r2 = _batch(client, payload, key="b")
    assert r1.status_code == r2.status_code == 200
    # 不同键视为不同请求：真实执行两次
    assert volume_of(client, "S1") == "8.000000000"
    assert r2.json()["versions"] == {"S1": 2, "V1": 2}


# --------------------------------------------------------------------------- #
# 预演不写幂等记录
# --------------------------------------------------------------------------- #
def test_preview_never_writes_idempotency_record(client):
    _setup(client)
    payload = [{"source": "S1", "destination": "V1", "volume": "2"}]

    # 预演携带同样的键：不落任何记录、不扣体积
    r = client.post(
        "/transfers/preview",
        json={"transfers": payload, "idempotency_key": "preview-key"},
    )
    assert r.status_code == 200

    conn = connect()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM idempotent_requests"
        ).fetchone()["c"] == 0
    finally:
        conn.close()
    assert volume_of(client, "S1") == "10.000000000"
    assert client.get("/containers/S1").json()["version"] == 0

    # 预演后用同键正式提交：首次执行（非重放）
    r = _batch(client, payload, key="preview-key")
    assert r.status_code == 200
    assert r.json()["applied"] == 1
    assert volume_of(client, "S1") == "8.000000000"

    # 再次预演同键也不影响后续重放
    r = client.post(
        "/transfers/preview",
        json={"transfers": payload, "idempotency_key": "preview-key"},
    )
    assert r.status_code == 200
    r = _batch(client, payload, key="preview-key")
    assert r.status_code == 200
    assert r.json()["applied"] == 1
    assert volume_of(client, "S1") == "8.000000000"


# --------------------------------------------------------------------------- #
# 并发：同键并发请求只有一次真实扣减，另一次拿到重放
# --------------------------------------------------------------------------- #
def test_concurrent_same_key_requests_deduct_once():
    conn0 = connect()
    try:
        service.register_initial_sample(conn0, "S1", Decimal("10"))
        service.register_empty_container(conn0, "V1")
    finally:
        conn0.close()

    barrier = threading.Barrier(2)
    outcomes: list[tuple[bool, dict]] = []
    errors: list[Exception] = []

    def worker():
        conn = connect()
        try:
            barrier.wait()
            fp = service.fingerprint_request(
                {"transfers": [{"source": "S1", "destination": "V1", "volume": "1"}]}
            )
            result, replayed = service.apply_transfers_idempotent(
                conn,
                [("S1", "V1", Decimal("1"))],
                None,
                "concurrent-key",
                fp,
            )
            outcomes.append((replayed, result))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            conn.close()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(outcomes) == 2
    assert sorted(replayed for replayed, _ in outcomes) == [False, True]
    assert outcomes[0][1] == outcomes[1][1]

    conn = connect()
    try:
        assert service.get_container_status(conn, "S1")["volume"] == "9.000000000"
        assert service.get_container_status(conn, "V1")["volume"] == "1.000000000"
        assert service.get_container_status(conn, "S1")["version"] == 1
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM idempotent_requests"
            ).fetchone()["c"]
            == 1
        )
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 旧库迁移：init_db 自动补建 idempotent_requests
# --------------------------------------------------------------------------- #
def test_init_db_migrates_legacy_database_without_idempotency_table(
    tmp_path, monkeypatch
):
    legacy_path = tmp_path / "legacy-idem.db"

    raw = sqlite3.connect(legacy_path)
    try:
        # 旧库：有 containers/components/version，但没有 idempotent_requests
        raw.executescript(
            """
            CREATE TABLE containers (
                id      TEXT PRIMARY KEY,
                version INTEGER NOT NULL DEFAULT 0,
                created INTEGER NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE components (
                container_id TEXT NOT NULL REFERENCES containers(id) ON DELETE CASCADE,
                source_id    TEXT NOT NULL,
                amount       TEXT NOT NULL,
                PRIMARY KEY (container_id, source_id)
            );
            INSERT INTO containers (id, version) VALUES ('OLD', 3);
            INSERT INTO components (container_id, source_id, amount)
                VALUES ('OLD', 'OLD', '5.500000000');
            """
        )
        raw.commit()
    finally:
        raw.close()

    monkeypatch.setattr(settings, "database_path", str(legacy_path))

    from app.db import init_db

    init_db()  # 迁移：补建幂等表
    init_db()  # 必须幂等

    conn = connect()
    try:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "idempotent_requests" in tables
        cols = {
            r[1]
            for r in conn.execute("PRAGMA table_info(idempotent_requests)").fetchall()
        }
        assert {
            "idempotency_key",
            "request_fingerprint",
            "response_json",
            "status_code",
            "created",
        } <= cols

        # 旧数据完好，且旧容器可参与带幂等键的转移
        service.register_empty_container(conn, "NEW")
        fp = service.fingerprint_request(
            {"transfers": [{"source": "OLD", "destination": "NEW", "volume": "1"}]}
        )
        result, replayed = service.apply_transfers_idempotent(
            conn, [("OLD", "NEW", Decimal("1"))], None, "legacy-key", fp
        )
        assert replayed is False
        assert result["versions"] == {"OLD": 4, "NEW": 1}
    finally:
        conn.close()
