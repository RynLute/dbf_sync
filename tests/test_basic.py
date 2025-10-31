import os
import sqlite3
from pathlib import Path
from dbf_sync import (
    decode_fs, combine_datetime, validate_record, prepare_insert_data, create_table_sql, DBClient
)

def test_decode_fs():
    fs = 0b10110110
    result = decode_fs(fs)
    assert result["SHIFT"] == 2
    assert result["SHIFT_END"] is True
    assert result["SENSOR1_ALARM"] is True
    assert result["SENSOR2_ALARM"] is False
    assert result["TIME_SYNC"] is True
    assert result["READ_ATTEMPT"] is True

def test_combine_datetime():
    ts = combine_datetime("20251031", "1435")
    assert ts.startswith("2025-10-31 14:35")

def test_validate_record_and_prepare():
    rec = {
        "DATE": "20251031",
        "TIME": "1435",
        "COUNT": "100",
        "KOD": "5",
        "KEY": "1",
        "AVR": "1",
        "DNA": "0",
        "PIT": "1",
        "KTIME": "0",
        "AVRTIME": "2",
        "PITTIME": "3",
        "FS": 12,
    }
    res = validate_record(rec, 42)
    assert res["COUNTER_ID"] == 42
    assert "TS" in res
    cols, vals = prepare_insert_data([res])
    assert "TS" in cols
    assert len(vals) == 1

def test_create_table_sqlite(tmp_path):
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(db_file)
    conn.execute(create_table_sql("sqlite"))
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='reports'").fetchall()
    assert tables

def test_dbclient_sqlite(tmp_path):
    cfg = {"db": {"type": "sqlite", "sqlite_path": str(tmp_path / "test.db")}}
    db = DBClient(cfg)
    db.connect()
    db.ensure_schema()
    assert db.conn is not None
    cols = ["COUNTER_ID", "TS", "COUNT"]
    vals = [(1, "2025-10-31 14:00", 123)]
    n = db.save_batch(cols, vals)
    assert n == 1