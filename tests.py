import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

# Импорт функций из dbf_sync.py (предполагаем, что файл доступен)
from dbf_sync import (
    decode_fs, extract_counter_id, combine_datetime, validate_record,
    prepare_insert_data, create_table_sql, process_file, main,
    DBClient, DBF_SPEC, BOOL_FIELDS
)

# Тестовые данные
SAMPLE_RECORD = {
    "DATE": "20251031",
    "TIME": "1234",
    "COUNT": 100,
    "KOD": 1,
    "KEY": 2,
    "AVR": 1,
    "DNA": 0,
    "PIT": 1,
    "KTIME": 0,
    "AVRTIME": 10,
    "PITTIME": 20,
    "FS": 15  # Бинарно: 00001111 -> SHIFT=3, SHIFT_END=1, etc.
}

@pytest.fixture
def mock_db_client():
    client = DBClient({"db": {"type": "sqlite"}})
    client.conn = MagicMock()
    return client

def test_decode_fs():
    fs = 15  # 00001111 в бинарном
    expected = {
        "SHIFT": 3,  # 11
        "SHIFT_END": True,  # (15 >> 2) & 1 = 1
        "SENSOR1_ALARM": False,  # (15 >> 4) & 1 = 0
        "SENSOR2_ALARM": True,  # (15 >> 5) & 1 = 1 (wait, recalculate: 15 is 1111, >>2=11 (3), &1=1; >>4=0, &1=0; >>5=0, &1=0; wait, fix)
        # Correct: 15 = 0b1111 -> SHIFT=0b11=3, SHIFT_END=0b1 (bit2=1? 0b1111: bit0=1,1=1,2=1,3=1 -> bit2=1 yes, SHIFT_END=1; bit4=0, etc.
        # Actually: fs & 0b11 = 3, >>2 &1 =1 (SHIFT_END), >>4&1=0, >>5&1=0, >>6&1=0, >>7&1=0
        "SENSOR1_ALARM": False,
        "SENSOR2_ALARM": False,
        "TIME_SYNC": False,
        "READ_ATTEMPT": False,
    }
    assert decode_fs(fs) == expected

    # Edge case: fs=0
    assert decode_fs(0) == {k: 0 if k == "SHIFT" else False for k in expected}

    # Max fs=255
    assert decode_fs(255) == {
        "SHIFT": 3,
        "SHIFT_END": True,
        "SENSOR1_ALARM": True,
        "SENSOR2_ALARM": True,
        "TIME_SYNC": True,
        "READ_ATTEMPT": True,
    }

def test_extract_counter_id():
    assert extract_counter_id("1234abcd.dbf") == 1234
    assert extract_counter_id("invalid.dbf") is None
    assert extract_counter_id("0001test.dbf") == 1

def test_combine_datetime():
    assert combine_datetime("20251031", "1234") == "2025-10-31 12:34"
    assert combine_datetime("20251031", "00:00") == "2025-10-31 00:00"
    assert combine_datetime("invalid", "1234") is None
    assert combine_datetime("", "1234") is None
    assert combine_datetime("20251031", "") == "2025-10-31 00:00"
    assert combine_datetime("20251031", "abc") == "2025-10-31 00:00"  # Invalid time -> default 00:00

def test_validate_record():
    counter_id = 1234
    validated = validate_record(SAMPLE_RECORD, counter_id)
    assert validated["COUNTER_ID"] == 1234
    assert validated["TS"] == "2025-10-31 12:34"
    assert validated["COUNT"] == 100
    assert validated["AVR"] == True  # 1 -> bool(True)
    assert validated["DNA"] == False
    # FS decoded fields
    assert validated["SHIFT"] == 3
    assert validated["SHIFT_END"] == True

    # Empty fields
    empty_rec = {k: None for k in SAMPLE_RECORD}
    validated_empty = validate_record(empty_rec, counter_id)
    assert validated_empty["TS"] is None
    for fld in BOOL_FIELDS:
        assert validated_empty[fld] is None

def test_prepare_insert_data():
    records = [validate_record(SAMPLE_RECORD, 1234)]
    cols, vals = prepare_insert_data(records)
    expected_cols = ["COUNTER_ID", "TS", "COUNT", "KOD", "KEY", "AVR", "DNA", "PIT", "KTIME", "AVRTIME", "PITTIME", "FS", "SHIFT", "SHIFT_END", "SENSOR1_ALARM", "SENSOR2_ALARM", "TIME_SYNC", "READ_ATTEMPT"]
    assert cols == expected_cols
    assert len(vals) == 1
    assert vals[0][0] == 1234  # COUNTER_ID
    assert vals[0][1] == "2025-10-31 12:34"  # TS
    assert vals[0][-1] == False  # READ_ATTEMPT

    # Empty list
    assert prepare_insert_data([]) == ([], [])

def test_create_table_sql():
    sqlite_sql = create_table_sql("sqlite")
    assert "CREATE TABLE IF NOT EXISTS reports" in sqlite_sql
    assert "INTEGER PRIMARY KEY AUTOINCREMENT" in sqlite_sql
    assert "avr INTEGER" in sqlite_sql  # INTEGER for bool in sqlite

    pg_sql = create_table_sql("postgres")
    assert "SERIAL PRIMARY KEY" in pg_sql
    assert "avr BOOLEAN" in pg_sql  # BOOLEAN for postgres

@patch('dbf_sync.read_new_records')
@patch('dbf_sync.logger')
def test_process_file(mock_logger, mock_read, mock_db: mock_db_client):
    mock_read.return_value = [SAMPLE_RECORD]
    f = Path("1234test.dbf")
    total = process_file(f, mock_db, batch=1000, dry_run=False)
    assert total == 1
    mock_db.save_batch.assert_called_once()

    # Dry run
    total_dry = process_file(f, mock_db, batch=1000, dry_run=True)
    assert total_dry == 1
    mock_db.save_batch.assert_not_called()

    # Invalid counter_id
    invalid_f = Path("invalid.dbf")
    total_invalid = process_file(invalid_f, mock_db, batch=1000)
    assert total_invalid == 0

@patch('dbf_sync.argparse.ArgumentParser.parse_args')
@patch('dbf_sync.load_config')
@patch('dbf_sync.setup_logging')
@patch('dbf_sync.process_file')
@patch('dbf_sync.DBClient')
@patch('dbf_sync.Path')
@patch('dbf_sync.datetime')
def test_main(mock_datetime, mock_path, mock_dbclient, mock_process, mock_logging, mock_config, mock_parse):
    mock_parse.return_value = MagicMock(config="config.yaml", init=False, dry_run=False)
    mock_config.return_value = {"source": {"folder": ".", "pattern": "????xxxx.dbf"}, "batch_size": 1000}
    mock_datetime.now.return_value.year = 2025
    mock_path.return_value.glob.return_value = [Path("12342025.dbf")]
    mock_process.return_value = 1
    mock_db = MagicMock()
    mock_dbclient.return_value = mock_db

    exit_code = main()
    assert exit_code == 0
    mock_process.assert_called_once()
    mock_db.ensure_schema.assert_not_called()  # Not init

    # Init mode
    mock_parse.return_value.init = True
    exit_code_init = main()
    assert exit_code_init == 0
    mock_db.ensure_schema.assert_called_once()