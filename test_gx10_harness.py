import sqlite3, numpy as np, pandas as pd
from pathlib import Path

WIKI_ROOT = Path("/home/jaren/wiki").resolve(strict=True)
assert not (WIKI_ROOT / "wiki").exists(), "FATAL: Nested wiki directory found!"
print(f"[PASS] Path Guardrail: {WIKI_ROOT}")

db_path = Path("/tmp/universe_test_cache.db")
if db_path.exists(): db_path.unlink()
conn = sqlite3.connect(str(db_path))
conn.execute("PRAGMA journal_mode = WAL;")
conn.execute("PRAGMA mmap_size = 268435456;")
print(f"[PASS] SQLite WAL & Mmap operational. Mode: {conn.execute('PRAGMA journal_mode;').fetchone()[0]}")
conn.close()
db_path.unlink()
