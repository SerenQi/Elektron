#!/usr/bin/env python3
# 原文层导入：全量对话原文 → context.db（FTS5）
# 来源：gateway chat-history.jsonl + CC transcripts (~/.claude/projects/**/*.jsonl)
# 增量：state 表记录 (file, lines_done)，重跑只补新行。
import os, json, sqlite3, glob, sys

import os
DB = os.environ.get('CONTEXT_DB', './data/context.db')
CHAT = os.environ.get('CHAT_LOG', './data/chat-history.jsonl')
PROJ = os.path.expanduser('~/.claude/projects')

def db():
    c = sqlite3.connect(DB)
    c.execute('CREATE VIRTUAL TABLE IF NOT EXISTS raw USING fts5(ts, source, role, text)')
    c.execute('CREATE TABLE IF NOT EXISTS import_state(file TEXT PRIMARY KEY, lines_done INTEGER)')
    return c

def done(c, f):
    r = c.execute('SELECT lines_done FROM import_state WHERE file=?', (f,)).fetchone()
    return r[0] if r else 0

def mark(c, f, n):
    c.execute('INSERT INTO import_state(file,lines_done) VALUES(?,?) ON CONFLICT(file) DO UPDATE SET lines_done=?', (f, n, n))

def clean(t):
    t = (t or '').strip()
    if not t or t.startswith(('<local-command', '<command-name', 'Caveat:', '<task-notification', '[Request interrupted')):
        return ''
    if t.startswith('<system-reminder') or 'tool_use_id' in t[:80]:
        return ''
    return t

def import_chat(c):
    n0 = done(c, CHAT); n = 0; added = 0
    for line in open(CHAT, encoding='utf-8'):
        n += 1
        if n <= n0:
            continue
        try:
            e = json.loads(line)
        except Exception:
            continue
        t = clean(e.get('text'))
        if t:
            c.execute('INSERT INTO raw VALUES(?,?,?,?)', (e.get('ts', ''), 'chat', e.get('role', ''), t))
            added += 1
        th = clean(e.get('thinking') or '')
        if th:
            c.execute('INSERT INTO raw VALUES(?,?,?,?)', (e.get('ts', ''), 'chat', 'thinking', th))
    mark(c, CHAT, n)
    return added

def import_transcript(c, fp):
    n0 = done(c, fp); n = 0; added = 0
    try:
        fh = open(fp, encoding='utf-8')
    except Exception:
        return 0
    for line in fh:
        n += 1
        if n <= n0:
            continue
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
        if ts.endswith('Z'):  # UTC -> 北京（StarHub水位线铁律：时区免疫）
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
                c.execute('INSERT INTO raw VALUES(?,?,?,?)', (ts, os.path.basename(fp)[:12], m.get('role', typ), t))
                added += 1
    mark(c, fp, n)
    return added

if __name__ == '__main__':
    c = db()
    total = import_chat(c)
    files = glob.glob(os.path.join(PROJ, '*', '*.jsonl'))
    for fp in sorted(files):
        total += import_transcript(c, fp)
        c.commit()
    c.commit()
    cnt = c.execute('SELECT count(*) FROM raw').fetchone()[0]
    print(f'本次新增 {total} 条，库内总计 {cnt} 条原文')
