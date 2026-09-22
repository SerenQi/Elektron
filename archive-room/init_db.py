#!/usr/bin/env python3
"""Initialize empty databases for the archive room (context.db + memory.db).
Safe to run repeatedly; existing tables are left untouched.
Usage: DATA_DIR=./data python init_db.py
"""
import os, sqlite3, json

DATA = os.environ.get('DATA_DIR', './data')
os.makedirs(DATA, exist_ok=True)

ctx = sqlite3.connect(os.environ.get('CONTEXT_DB', DATA + '/context.db'))
ctx.executescript("""
CREATE VIRTUAL TABLE IF NOT EXISTS raw USING fts5(ts, source, role, text);
CREATE TABLE IF NOT EXISTS digest(day TEXT PRIMARY KEY, total INT, hers INT, mine INT, first_ts TEXT, last_ts TEXT);
CREATE TABLE IF NOT EXISTS digest_narrative(day TEXT PRIMARY KEY, content TEXT, built_at TEXT);
CREATE TABLE IF NOT EXISTS digest_week(week_end TEXT PRIMARY KEY, content TEXT, built_at TEXT);
""")
ctx.commit(); ctx.close()

mem = sqlite3.connect(os.environ.get('MEMORY_DB', DATA + '/memory.db'))
mem.executescript("""
CREATE TABLE IF NOT EXISTS memories(
  id INTEGER PRIMARY KEY, content TEXT, category TEXT DEFAULT 'events',
  tags TEXT DEFAULT '', importance INT DEFAULT 5, pinned INT DEFAULT 0,
  created_at TEXT, recalled_count INT DEFAULT 0, superseded_by INTEGER);
CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(content, content=memories, content_rowid=id);
CREATE TABLE IF NOT EXISTS memory_edges(
  id INTEGER PRIMARY KEY, source_id INT, target_id INT,
  relation TEXT, context TEXT, created_at TEXT);
""")
mem.commit(); mem.close()

for f, default in (('ledger.json', []), ('threads.json', [])):
    p = os.path.join(DATA, f)
    if not os.path.exists(p):
        json.dump(default, open(p, 'w'))

os.makedirs(os.path.join(DATA, 'identity', 'candidates'), exist_ok=True)
os.makedirs(os.path.join(DATA, 'identity', 'committed'), exist_ok=True)
os.makedirs(os.path.join(DATA, 'diary'), exist_ok=True)
os.makedirs(os.path.join(DATA, 'diary-heart'), exist_ok=True)
print('archive-room data initialized at', DATA)
