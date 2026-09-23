"""Maintain the derived Chinese search index in the raw import transaction."""


def ensure_index(conn):
    conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS raw_tri USING fts5(ts, source, role, text, tokenize='trigram')")
    conn.execute('CREATE TABLE IF NOT EXISTS recall_index_state(name TEXT PRIMARY KEY)')
    # Repair the previously one-off snapshot, including any gaps, without
    # changing raw row IDs or adding duplicate source records.
    if not conn.execute("SELECT 1 FROM recall_index_state WHERE name='raw_tri_v1'").fetchone():
        conn.execute("""INSERT INTO raw_tri(rowid, ts, source, role, text)
            SELECT r.rowid, r.ts, r.source, r.role, r.text FROM raw AS r
            LEFT JOIN raw_tri AS t ON t.rowid = r.rowid WHERE t.rowid IS NULL""")
        conn.execute("INSERT INTO recall_index_state VALUES('raw_tri_v1')")
    else:
        conn.execute("""INSERT INTO raw_tri(rowid, ts, source, role, text)
            SELECT rowid, ts, source, role, text FROM raw
            WHERE rowid > (SELECT coalesce(max(rowid), 0) FROM raw_tri)""")


def insert_raw(conn, ts, source, role, text):
    cur = conn.execute('INSERT INTO raw VALUES(?,?,?,?)', (ts, source, role, text))
    conn.execute('INSERT INTO raw_tri(rowid,ts,source,role,text) VALUES(?,?,?,?,?)',
                 (cur.lastrowid, ts, source, role, text))
