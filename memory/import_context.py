#!/usr/bin/env python3
# 原文层导入：全量对话原文 → context.db（FTS5）
# 来源：gateway chat-history.jsonl + CC transcripts (~/.claude/projects/**/*.jsonl)
# 增量：state 表记录 (file, lines_done, bytes_done, mtime)，重跑只补新行。
#   mtime 没变的文件整个跳过，变了的 seek 到上次停的字节，不重读全量。
import os, json, sqlite3, glob, sys
from context_index import ensure_index, insert_raw

DB = os.environ.get('CONTEXT_DB', './data/context.db')
CHAT = os.environ.get('CHAT_LOG', './data/chat-history.jsonl')
PROJ = os.path.expanduser('~/.claude/projects')

def db():
    c = sqlite3.connect(DB, timeout=30)
    c.execute('CREATE VIRTUAL TABLE IF NOT EXISTS raw USING fts5(ts, source, role, text)')
    ensure_index(c)
    c.execute('CREATE TABLE IF NOT EXISTS import_state(file TEXT PRIMARY KEY, lines_done INTEGER)')
    cols = {r[1] for r in c.execute('PRAGMA table_info(import_state)')}
    if 'bytes_done' not in cols:
        c.execute('ALTER TABLE import_state ADD COLUMN bytes_done INTEGER DEFAULT 0')
    if 'mtime' not in cols:
        c.execute('ALTER TABLE import_state ADD COLUMN mtime REAL DEFAULT 0')
    return c

def state(c, f):
    r = c.execute('SELECT lines_done, bytes_done, mtime FROM import_state WHERE file=?', (f,)).fetchone()
    return (r[0] or 0, r[1] or 0, r[2] or 0.0) if r else (0, 0, 0.0)

def mark(c, f, lines, offset, mtime):
    c.execute('''INSERT INTO import_state(file, lines_done, bytes_done, mtime) VALUES(?,?,?,?)
                 ON CONFLICT(file) DO UPDATE SET lines_done=?, bytes_done=?, mtime=?''',
              (f, lines, offset, mtime, lines, offset, mtime))

def seek_to(fh, lines0, bytes0):
    """回到上次停下的地方。有字节偏移直接 seek，只有旧的行号状态就读一遍补上。"""
    if bytes0:
        fh.seek(bytes0)
        return lines0
    n = 0
    while n < lines0:
        if not fh.readline():
            break
        n += 1
    return n

def clean(t):
    t = (t or '').strip()
    if not t or t.startswith(('<local-command', '<command-name', 'Caveat:', '<task-notification', '[Request interrupted')):
        return ''
    if t.startswith('<system-reminder') or 'tool_use_id' in t[:80]:
        return ''
    return t

def import_chat(c):
    try:
        mt = os.path.getmtime(CHAT)
    except OSError:
        return 0
    lines0, bytes0, mt0 = state(c, CHAT)
    if mt0 and mt <= mt0:
        return 0
    added = 0
    with open(CHAT, encoding='utf-8') as fh:
        n = seek_to(fh, lines0, bytes0)
        good = fh.tell()      # 偏移只推进到「最后一条完整记录之后」
        while True:
            line = fh.readline()
            if not line:
                break
            if not line.endswith("\n"):
                # 最后一行还没写完（写入方正在追加）。不推进 good，
                # 下一轮从这一行开头重读——否则这条会被永久跳过。
                break
            n += 1
            good = fh.tell()
            try:
                e = json.loads(line)
            except Exception:
                continue
            t = clean(e.get('text'))
            if t:
                insert_raw(c, e.get('ts', ''), 'chat', e.get('role', ''), t)
                added += 1
            th = clean(e.get('thinking') or '')
            if th:
                insert_raw(c, e.get('ts', ''), 'chat', 'thinking', th)
        offset = good
    mark(c, CHAT, n, offset, mt)
    return added

def import_transcript(c, fp):
    try:
        mt = os.path.getmtime(fp)
    except OSError:
        return 0
    lines0, bytes0, mt0 = state(c, fp)
    if mt0 and mt <= mt0:      # 这个文件从上次导入后一个字节都没动过
        return 0
    try:
        fh = open(fp, encoding='utf-8')
    except Exception:
        return 0
    added = 0
    try:
        n = seek_to(fh, lines0, bytes0)
        good = fh.tell()      # 偏移只推进到「最后一条完整记录之后」
        while True:
            line = fh.readline()
            if not line:
                break
            if not line.endswith("\n"):
                # 最后一行还没写完（写入方正在追加）。不推进 good，
                # 下一轮从这一行开头重读——否则这条会被永久跳过。
                break
            n += 1
            good = fh.tell()
            try:
                e = json.loads(line)
            except Exception:
                continue
            typ = e.get('type')
            if typ not in ('user', 'assistant') or e.get('isMeta'):
                continue
            m = e.get('message', {})
            content = m.get('content')
            ts = e.get('timestamp', '')
            if ts.endswith('Z'):  # UTC -> 北京
                import datetime as _dt
                try:
                    ts = (_dt.datetime.fromisoformat(ts.replace('Z', '+00:00')) + _dt.timedelta(hours=8)).strftime('%Y-%m-%dT%H:%M:%S')
                except Exception:
                    pass
            texts = []
            if isinstance(content, str):
                texts = [content]
            elif isinstance(content, list):
                for b in content:
                    if isinstance(b, dict) and b.get('type') == 'text':
                        texts.append(b.get('text', ''))
            for t in texts:
                t = clean(t)
                if t:
                    insert_raw(c, ts, os.path.basename(fp)[:12], m.get('role', typ), t)
                    added += 1
        offset = good
    finally:
        fh.close()
    mark(c, fp, n, offset, mt)
    return added

if __name__ == '__main__':
    c = db()
    total = import_chat(c)
    touched = 0
    files = glob.glob(os.path.join(PROJ, '*', '*.jsonl'))
    for fp in sorted(files):
        got = import_transcript(c, fp)
        if got:
            touched += 1
        total += got
        c.commit()
    c.commit()
    cnt = c.execute('SELECT count(*) FROM raw').fetchone()[0]
    print(f'本次新增 {total} 条（来自 {touched} 个变动文件），库内总计 {cnt} 条原文')
