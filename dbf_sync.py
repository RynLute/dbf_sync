#!/usr/bin/env python3
"""
Переработанный dbf_sync.py (v2)
- Дата и время теперь объединены в одно поле TIMESTAMP (datetime) — подход к Time Series.
- Все остальные правки сохранены из предыдущей версии: устойчивость, безопасность, производительность.
- Поддержка SQLite и PostgreSQL.

"""

from __future__ import annotations
import argparse, json, logging, os, re, sqlite3, subprocess, sys, time
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import yaml
except Exception as e:
    yaml = None

try:
    from dbfread import DBF
except Exception:
    DBF = None

DEFAULT_CONFIG = {
    "source": {"folder": "./dbf", "pattern": "????xxxx.dbf"},
    "db": {"type": "sqlite", "sqlite_path": "./db.sqlite"},
    "state_file": "./state.json",
    "log_file": "./dbf_sync.log",
    "batch_size": 1000,
}

DBF_SPEC = [
    ("DATE", "D", 8),
    ("TIME", "C", 5),
    ("COUNT", "N", 5),
    ("KOD", "N", 3),
    ("KEY", "N", 2),
    ("AVR", "N", 1),
    ("DNA", "N", 1),
    ("PIT", "N", 1),
    ("KTIME", "N", 1),
    ("AVRTIME", "N", 2),
    ("PITTIME", "N", 2),
    ("FS", "N", 3),
]

ALLOWED_COLS = [n for n, *_ in DBF_SPEC]
BOOL_FIELDS = {"AVR", "DNA", "PIT", "KTIME"}

logger = logging.getLogger("dbf_sync")

def setup_logging(log_file: str):
    handlers = [logging.StreamHandler()]
    try:
        fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
        handlers.append(fh)
    except Exception:
        pass
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=handlers)

def load_config(path: str) -> dict:
    if yaml is None:
        raise RuntimeError("pyyaml не установлен")
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    out = DEFAULT_CONFIG.copy()
    for k, v in cfg.items():
        if isinstance(v, dict) and k in out and isinstance(out[k], dict):
            out[k].update(v)
        else:
            out[k] = v
    return out

def decode_fs(fs: int) -> Dict[str, Any]:
    return {
        "SHIFT": fs & 0b11,
        "SHIFT_END": bool((fs >> 2) & 1),
        "SENSOR1_ALARM": bool((fs >> 4) & 1),
        "SENSOR2_ALARM": bool((fs >> 5) & 1),
        "TIME_SYNC": bool((fs >> 6) & 1),
        "READ_ATTEMPT": bool((fs >> 7) & 1),
    }

def extract_counter_id(filename: str) -> Optional[int]:
    m = re.match(r"^(\d{4})", Path(filename).stem)
    return int(m.group(1)) if m else None

def read_new_records(file_path: Path, encoding="cp1251") -> List[Dict[str, Any]]:
    if DBF is None:
        raise RuntimeError("dbfread не установлен")
    table = DBF(str(file_path), encoding=encoding, load=True)
    out = []
    for rec in table:
        r = {name: rec.get(name) for name, *_ in DBF_SPEC}
        fs = int(r.get("FS") or 0)
        r.update(decode_fs(fs))
        out.append(r)
    return out

def combine_datetime(date_str: str, time_str: str) -> Optional[str]:
    if not date_str:
        return None
    s = str(date_str).strip()
    if not re.match(r"^\d{8}$", s):
        return None
    y, m, d = s[0:4], s[4:6], s[6:8]
    t = str(time_str or "00:00").strip()
    if re.match(r"^\d{4}$", t):
        t = f"{t[0:2]}:{t[2:4]}"
    if not re.match(r"^\d{1,2}:\d{2}$", t):
        t = "00:00"
    try:
        return datetime.strptime(f"{y}-{m}-{d} {t}", "%Y-%m-%d %H:%M").isoformat(sep=" ")
    except Exception:
        return None

def validate_record(rec: Dict[str, Any], counter_id: Optional[int]) -> Dict[str, Any]:
    r: Dict[str, Any] = {"COUNTER_ID": counter_id}
    r["TS"] = combine_datetime(rec.get("DATE"), rec.get("TIME"))
    for fld in ("COUNT", "KOD", "KEY", "AVRTIME", "PITTIME", "FS"):
        v = rec.get(fld)
        r[fld] = int(v) if v not in (None, "") else None
    for fld in BOOL_FIELDS:
        v = rec.get(fld)
        r[fld] = bool(int(v)) if v not in (None, "") else None
    fs = int(rec.get("FS") or 0)
    r.update(decode_fs(fs))
    return r

def prepare_insert_data(records: List[Dict[str, Any]]) -> Tuple[List[str], List[Tuple]]:
    if not records:
        return [], []
    cols = ["COUNTER_ID", "TS", "COUNT", "KOD", "KEY", "AVR", "DNA", "PIT", "KTIME", "AVRTIME", "PITTIME", "FS", "SHIFT", "SHIFT_END", "SENSOR1_ALARM", "SENSOR2_ALARM", "TIME_SYNC", "READ_ATTEMPT"]
    vals = [tuple(r.get(c) for c in cols) for r in records]
    return cols, vals

@dataclass
class DBClient:
    cfg: dict
    conn: Any = None

    def connect(self):
        dbcfg = self.cfg.get("db", {})
        t = dbcfg.get("type", "sqlite")
        if t == "sqlite":
            path = Path(dbcfg.get("sqlite_path", "./db.sqlite"))
            path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(str(path))
        else:
            import psycopg2
            self.conn = psycopg2.connect(
                host=dbcfg.get("host"), port=dbcfg.get("port", 5432), dbname=dbcfg.get("dbname"), user=dbcfg.get("user"), password=dbcfg.get("password")
            )

    def ensure_schema(self):
        dbtype = self.cfg.get("db", {}).get("type", "sqlite")
        sql = create_table_sql(dbtype)
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def save_batch(self, cols: List[str], values: List[Tuple]) -> int:
        if not values:
            return 0
        dbtype = self.cfg.get("db", {}).get("type", "sqlite")
        if dbtype == "sqlite":
            placeholders = ",".join(["?" for _ in cols])
            sql = f"INSERT INTO reports ({','.join(cols)}) VALUES ({placeholders});"
            cur = self.conn.cursor()
            cur.executemany(sql, values)
            self.conn.commit()
            return cur.rowcount
        else:
            import psycopg2.extras as extras
            sql = f"INSERT INTO reports ({','.join(cols)}) VALUES %s ON CONFLICT (counter_id, ts) DO NOTHING"
            cur = self.conn.cursor()
            extras.execute_values(cur, sql, values, page_size=1000)
            self.conn.commit()
            return len(values)

def create_table_sql(db_type: str) -> str:
    if db_type == "sqlite":
        return ("CREATE TABLE IF NOT EXISTS reports ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "counter_id INTEGER,"
                "ts TEXT,"
                "count INTEGER, kod INTEGER, key INTEGER, avr INTEGER, dna INTEGER, pit INTEGER, ktime INTEGER,"
                "avrtime INTEGER, pittime INTEGER, fs INTEGER, shift INTEGER, shift_end INTEGER,"
                "sensor1_alarm INTEGER, sensor2_alarm INTEGER, time_sync INTEGER, read_attempt INTEGER,"
                "UNIQUE(counter_id, ts));")
    else:
        return ("CREATE TABLE IF NOT EXISTS reports ("
                "id SERIAL PRIMARY KEY,"
                "counter_id INTEGER,"
                "ts TIMESTAMP,"
                "count INTEGER, kod INTEGER, key INTEGER, avr BOOLEAN, dna BOOLEAN, pit BOOLEAN, ktime BOOLEAN,"
                "avrtime INTEGER, pittime INTEGER, fs INTEGER, shift INTEGER, shift_end BOOLEAN,"
                "sensor1_alarm BOOLEAN, sensor2_alarm BOOLEAN, time_sync BOOLEAN, read_attempt BOOLEAN,"
                "UNIQUE(counter_id, ts));")

def chunked(it: Iterable, size: int):
    it = iter(it)
    while True:
        part = list(islice(it, size))
        if not part:
            break
        yield part

def process_file(f: Path, db: DBClient, batch: int, dry_run=False) -> int:
    cid = extract_counter_id(f.name)
    if cid is None:
        logger.warning("Пропуск файла %s — нет counter_id", f)
        return 0
    recs = read_new_records(f)
    converted = [validate_record(r, cid) for r in recs]
    cols, vals = prepare_insert_data(converted)
    total = 0
    for chunk in chunked(vals, batch):
        if dry_run:
            total += len(chunk)
        else:
            total += db.save_batch(cols, chunk)
    logger.info("%s: вставлено %d", f, total)
    return total

def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--init", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)
    cfg = load_config(args.config)
    setup_logging(cfg.get("log_file", "./dbf_sync.log"))
    db = DBClient(cfg)
    db.connect()
    if args.init:
        db.ensure_schema()
        logger.info("Schema created")
        return 0
    src = Path(cfg["source"]["folder"])
    patt = cfg["source"]["pattern"]
    year = datetime.now().year
    files = [f for f in src.glob(patt.replace("xxxx", str(year)))]
    total_all = 0
    for f in files:
        total_all += process_file(f, db, cfg.get("batch_size", 1000), dry_run=args.dry_run)
    logger.info("Всего вставлено: %d", total_all)
    return 0

if __name__ == "__main__":
    sys.exit(main())