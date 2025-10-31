import os
import re
import sqlite3
import yaml
import json
import logging
import argparse
import subprocess
import psycopg2
import signal
import sys

from psycopg2.extras import execute_batch
from datetime import datetime, date, time
from dbfread import DBF


# ---------- LOGGING ----------
def setup_logging(log_file: str) -> None:
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


# ---------- Ping ----------
def ping_host(host: str) -> bool:
    param = "-n" if os.name == "nt" else "-c"
    result = subprocess.run(["ping", param, "1", host], stdout=subprocess.DEVNULL)
    return result.returncode == 0


# ---------- READING THE CONFIG ----------
def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config


# ---------- STATE ----------
def load_state(state_file: str) -> dict:
    """
    Loads the status from a JSON file with error handling.
    """
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
                else:
                    logging.warning(f"State file {state_file} is empty. Returning empty dict.")
                    return {}
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding state file {state_file}: {e}. Returning empty dict.")
            return {}
        except Exception as e:
            logging.error(f"Unexpected error loading state file {state_file}: {e}. Returning empty dict.")
            return {}
    logging.info(f"State file {state_file} does not exist. Returning empty dict.")
    return {}


def save_state(state_file: str, state: dict) -> None:
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


# ---------- FS Decode ----------
def decode_fs(fs: int | None, db_type: str = None) -> dict:
    if fs is None:
        fs = 0

    flags = {
        "SHIFT": fs & 0b00000011,
        "SHIFT_END": 1 if fs & 0b00000100 else 0,
        "SENSOR1": 1 if fs & 0b00010000 else 0,
        "SENSOR2": 1 if fs & 0b00100000 else 0,
        "TIME_SYNC": 1 if fs & 0b01000000 else 0,
        "READ_ATTEMPT": 1 if fs & 0b10000000 else 0
    }

    if db_type == "postgres":
        bool_fields = ["SHIFT_END", "SENSOR1", "SENSOR2", "TIME_SYNC", "READ_ATTEMPT"]
        for field in bool_fields:
            flags[field] = bool(flags[field])

    return flags


# ---------- EXTRACTING THE COUNTER NUMBER ----------
def extract_counter_id(filename: str) -> int | None:
    """
    Extracts the first 4 digits from the file name (????xxxx.dbf).
    Returns int or None if failed..
    """
    match = re.match(r'^(\d{4})', filename)
    if match:
        return int(match.group(1))
    logging.warning(f"Couldn't extract COUNTER_ID from file name: {filename}")
    return None


# ---------- CREATING TABLES AND INDEXES ----------
def create_table_sql(db_type: str) -> str:
    """
    Returns an SQL query to create a table with the PRIMARY KEY and the INSERTED_AT field.
    """
    id_field = (
        "ID INTEGER PRIMARY KEY AUTOINCREMENT" if db_type == "sqlite"
        else "ID SERIAL PRIMARY KEY"
    )

    if db_type == "sqlite":
        date_time_fields = """
            DATE TEXT,
            TIME TEXT,
        """
    elif db_type == "postgres":
        date_time_fields = """
            DATE DATE,
            TIME TIME,
        """
    else:
        raise ValueError("Unsupported database type")

    common_fields = f"""
        {id_field},
        COUNTER_ID INTEGER,
        {date_time_fields}
        COUNT INTEGER,
        KOD INTEGER,
        KEY INTEGER,
        AVR INTEGER,
        DNA INTEGER,
        PIT INTEGER,
        KTIME INTEGER,
        AVRTIME INTEGER,
        PITTIME INTEGER,
        SHIFT INTEGER
    """

    if db_type == "sqlite":
        flag_fields = """
            SHIFT_END INTEGER,
            SENSOR1 INTEGER,
            SENSOR2 INTEGER,
            TIME_SYNC INTEGER,
            READ_ATTEMPT INTEGER,
            INSERTED_AT TEXT DEFAULT CURRENT_TIMESTAMP
        """
    elif db_type == "postgres":
        flag_fields = """
            SHIFT_END BOOLEAN,
            SENSOR1 BOOLEAN,
            SENSOR2 BOOLEAN,
            TIME_SYNC BOOLEAN,
            READ_ATTEMPT BOOLEAN,
            INSERTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """

    return f"""
    CREATE TABLE IF NOT EXISTS reports (
        {common_fields},
        {flag_fields}
    )
    """


def create_indexes(conn, db_type: str) -> None:
    """
    Creates optimized indexes, including UNIQUE ones to prevent duplicates..
    """
    indexes = [
        # A unique index to prevent duplicates in key fields.
        "CREATE UNIQUE INDEX IF NOT EXISTS uniq_reports_counter_datetime ON reports(COUNTER_ID, DATE, TIME)",
        # Indexes for filtering
        "CREATE INDEX IF NOT EXISTS idx_reports_counter_id ON reports(COUNTER_ID)",
        "CREATE INDEX IF NOT EXISTS idx_reports_date ON reports(DATE)",
        "CREATE INDEX IF NOT EXISTS idx_reports_time ON reports(TIME)",
        "CREATE INDEX IF NOT EXISTS idx_reports_kod ON reports(KOD)",
        "CREATE INDEX IF NOT EXISTS idx_reports_key ON reports(KEY)",
        "CREATE INDEX IF NOT EXISTS idx_reports_shift ON reports(SHIFT)",
        # Composite indexes for typical queries
        "CREATE INDEX IF NOT EXISTS idx_reports_counter_date ON reports(COUNTER_ID, DATE)",
        "CREATE INDEX IF NOT EXISTS idx_reports_counter_datetime ON reports(COUNTER_ID, DATE, TIME)",
        "CREATE INDEX IF NOT EXISTS idx_reports_inserted_at ON reports(INSERTED_AT)",
    ]

    try:
        if db_type == "sqlite":
            for idx_sql in indexes:
                conn.execute(idx_sql)
            conn.commit()
        else:
            with conn.cursor() as cur:
                for idx_sql in indexes:
                    cur.execute(idx_sql)
            conn.commit()
        logging.info("Indexes have been created successfully.")
    except Exception as e:
        logging.warning(f"Error when creating indexes: {e}")


# ---------- CONNECTING TO THE DATABASE ----------
def connect_db(cfg: dict) -> tuple:
    db_type = cfg["db"]["type"].lower()
    sql = create_table_sql(db_type)

    if db_type == "sqlite":
        conn = sqlite3.connect(cfg["db"]["name"])
        conn.execute(sql)
        conn.commit()
        create_indexes(conn, db_type)
        return conn, db_type
    elif db_type == "postgres":
        conn = psycopg2.connect(
            host=cfg["db"]["host"],
            port=cfg["db"]["port"],
            dbname=cfg["db"]["name"],
            user=cfg["db"]["user"],
            password=cfg["db"]["password"]
        )
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        create_indexes(conn, db_type)
        return conn, db_type
    else:
        raise ValueError("Unsupported database type (use 'sqlite' or 'postgres')")


# ---------- FILE SEARCH ----------
def find_dbf_files(folder: str, pattern: str, year: int | str) -> list:
    if "xxxx" in pattern:
        pattern = pattern.replace("xxxx", str(year))
    regex = re.compile(pattern.replace("?", ".").replace("*", ".*"), re.IGNORECASE)
    return sorted([f for f in os.listdir(folder) if regex.fullmatch(f)])


# ---------- VALIDATION AND RECORD CONVERSION ----------
def validate_and_convert_record(record: dict, db_type: str) -> bool:
    """
    Verifies the validity of the record and converts the DATE/TIME for PostgreSQL.
    """
    required_fields = ["DATE", "TIME"]
    for field in required_fields:
        value = record.get(field)
        if value is None:
            logging.warning(f"Invalid record: missing {field} - {record}")
            return False

    # DATE processing (dbfread returns datetime.date)
    if isinstance(record["DATE"], date):
        date_obj = record["DATE"]
        if db_type == "sqlite":
            record["DATE"] = date_obj.strftime("%Y-%m-%d")
        # For postgres, we leave it as date
    elif isinstance(record["DATE"], str) and record["DATE"].strip():
        try:
            date_obj = datetime.strptime(record["DATE"], "%Y-%m-%d").date()
            record["DATE"] = date_obj if db_type == "postgres" else record["DATE"]
        except ValueError:
            logging.warning(f"Invalid date format: {record['DATE']} in record: {record}")
            return False
    else:
        logging.warning(f"Invalid DATE type or empty: {record['DATE']} in record: {record}")
        return False

    # TIME processing (string 'HH:MM')
    time_str = record["TIME"]
    if isinstance(time_str, str) and time_str.strip():
        try:
            # Format 'HH:MM' (from the log)
            parsed_time = datetime.strptime(time_str, "%H:%M")
            if db_type == "postgres":
                record["TIME"] = parsed_time.time()
            elif db_type == "sqlite":
                record["TIME"] = time_str  # Leaving the line
        except ValueError:
            logging.warning(f"Invalid time format: {time_str} in record: {record}")
            return False
    else:
        logging.warning(f"Invalid TIME type or empty: {record['TIME']} in record: {record}")
        return False

    return True


# ---------- READING NEW RECORDS ----------
def read_new_records(file_path: str, start_record: int, encoding: str, db_type: str, counter_id: int) -> list:
    num_records, _, _ = get_dbf_meta(file_path)
    if start_record >= num_records:
        return []

    records = []
    invalid_count = 0

    # Lazy DBF iterator without full load in memory
    table = DBF(file_path, encoding=encoding, ignore_missing_memofile=True, load=False)

    # Skipping the first start_record entries
    for i, r in enumerate(table):
        if i < start_record:
            continue
        if not validate_and_convert_record(r, db_type):
            invalid_count += 1
            continue
        # Adding the COUNTER_ID
        r["COUNTER_ID"] = counter_id
        # Decoding FS
        fs_decoded = decode_fs(r.get("FS", 0), db_type)
        r.update(fs_decoded)
        records.append(r)

    if invalid_count > 0:
        logging.warning(f"{invalid_count} invalid records skipped in {file_path}")

    return records


# ---------- READING METADATA ----------
def get_dbf_meta(file_path: str) -> tuple:
    with open(file_path, "rb") as f:
        header = f.read(32)
        num_records = int.from_bytes(header[4:8], byteorder="little")
        header_len = int.from_bytes(header[8:10], byteorder="little")
        record_len = int.from_bytes(header[10:12], byteorder="little")
    return num_records, header_len, record_len


# ---------- DATA PREPARATION ----------
def prepare_insert_data(records: list, allowed_cols: list) -> tuple:
    cols = [c for c in records[0].keys() if c.upper() in allowed_cols]
    if not cols:
        logging.warning("There are no matching columns to insert — skipping the file.")
        return None, None
    values = [[r.get(c) for c in cols] for r in records]
    return cols, values


# ---------- SAVE TO BD ----------
def save_to_db(conn, db_type: str, table_name: str, records: list) -> int:
    if not records:
        return 0

    allowed_cols = [
        "COUNTER_ID", "DATE", "TIME", "COUNT", "KOD", "KEY", "AVR", "DNA", "PIT",
        "KTIME", "AVRTIME", "PITTIME", "SHIFT", "SHIFT_END",
        "SENSOR1", "SENSOR2", "TIME_SYNC", "READ_ATTEMPT"
    ]  # Without ID and INSERTED_AT, they are auto

    cols, values = prepare_insert_data(records, allowed_cols)
    if not cols:
        return 0

    placeholders = ",".join(["?" if db_type == "sqlite" else "%s"] * len(cols))

    if db_type == "sqlite":
        sql = f"INSERT OR IGNORE INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})"
    else:  # postgres
        sql = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders}) ON CONFLICT (COUNTER_ID, DATE, TIME) DO NOTHING"

    try:
        if db_type == "sqlite":
            conn.executemany(sql, values)
        else:
            bool_fields = ["SHIFT_END", "SENSOR1", "SENSOR2", "TIME_SYNC", "READ_ATTEMPT"]
            processed_values = []
            for row in values:
                processed_row = []
                for i, val in enumerate(row):
                    col_name = cols[i]
                    if col_name in bool_fields and val is not None:
                        processed_row.append(bool(val))
                    else:
                        processed_row.append(val)
                processed_values.append(processed_row)

            with conn.cursor() as cur:
                execute_batch(cur, sql, processed_values)

        conn.commit()
        # To count the inserted ones (ignoring duplicates)
        inserted = conn.total_changes if db_type == "sqlite" else len(
            records)  # In postgres, execute_batch does not return, but for simplicity it does
        return inserted
    except Exception as e:
        logging.exception(f"Error when saving to the database: {e}")
        return 0


# ---------- CHUNKS FOR LARGE FILES ----------
def chunks(lst: list, n: int):
    """Splits the list into chunks of n elements."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ---------- BASIC LOGIC ----------
def main() -> None:
    parser = argparse.ArgumentParser(description="DBF sync utility")
    parser.add_argument("--init", action="store_true", help="Full initial import")
    parser.add_argument("--update", action="store_true", help="Incremental update")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    cfg = load_config()
    setup_logging(cfg.get("log_file", "dbf_sync.log"))

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logging.info("=== DBF Sync Started ===")

    # Graceful shutdown
    def handle_exit(signum, frame):
        logging.warning("A termination signal has been received, and connections are being closed...")
        if 'conn' in locals() and conn is not None:
            conn.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    db_type = cfg["db"]["type"].lower()

    if db_type == "postgres":
        if not ping_host(cfg["db"]["host"]):
            logging.error(f"Host {cfg['db']['host']} is unreachable")
            return

    folder = cfg["source"]["folder"]
    if not os.path.isdir(folder) or not os.access(folder, os.R_OK):
        logging.error(f"Source folder {folder} not found or not readable.")
        return

    pattern = cfg["source"]["pattern"]
    encoding = cfg["source"]["encoding"]
    year = cfg["source"].get("year", "auto")
    if year == "auto":
        year = datetime.now().year

    state_file = cfg["state_file"]
    state = load_state(state_file)

    files = find_dbf_files(folder, pattern, year)
    if not files:
        logging.warning("No DBF files found.")
        return

    conn, db_type = connect_db(cfg)

    try:
        for file in files:
            path = os.path.join(folder, file)
            counter_id = extract_counter_id(file)
            if counter_id is None:
                logging.error(f"Skipping the {file} file: the COUNTER_ID could not be determined")
                continue

            start_index = 0 if args.init else state.get(file, 0)

            records = read_new_records(path, start_index, encoding, db_type, counter_id)
            logging.info(f"{file} (COUNTER_ID={counter_id}): found {len(records)} new records (from {start_index})")

            if args.dry_run:
                logging.info(f"{file}: dry-run mode — skipped saving.")
                continue

            # Batch insertion for large files
            inserted = 0
            batch_size = 5000
            for chunk in chunks(records, batch_size):
                inserted += save_to_db(conn, db_type, "reports", chunk)

            state[file] = start_index + len(
                records)  # We update the state based on what we read, because duplicates may be skipped.
            save_state(state_file, state)
            logging.info(f"{file}: inserted {inserted} records (duplicates skipped)")

    finally:
        conn.close()
        logging.info("=== DBF Sync Completed ===")


if __name__ == "__main__":
    main()