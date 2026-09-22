#!/usr/bin/env python3
"""Archive Room · thin read-only API
只读两个库：context.db（原文层）+ memory.db（记忆层/边）。
不动 Elektron 本体代码。端口 8021，仅监听 127.0.0.1，由 nginx 挂到家门禁内。
"""
import json, sqlite3, re
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import os
DATA = os.environ.get('DATA_DIR', './data')
CTX_DB = os.environ.get('CONTEXT_DB', DATA + '/context.db')
MEM_DB = os.environ.get('MEMORY_DB', DATA + '/memory.db')
LEDGER = os.environ.get('LEDGER_FILE', DATA + '/ledger.json')
THREADS = os.environ.get('THREADS_FILE', DATA + '/threads.json')

def q(db_path, sql, args=()):
    db = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in db.execute(sql, args).fetchall()]
    finally:
        db.close()

def fts_quote(term: str) -> str:
    # 每个词加双引号防 FTS 语法注入，多词 AND
    words = [w for w in re.split(r'\s+', term.strip()) if w]
    return ' '.join(f'"{w.replace(chr(34), "")}"' for w in words)

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        p = u.path
        g = parse_qs(u.query)
        get = lambda k, d=None: (g.get(k, [d])[0])
        try:
            if p == '/api/stats':
                raw = q(CTX_DB, "SELECT COUNT(*) n FROM raw")[0]['n']
                mem = q(MEM_DB, "SELECT COUNT(*) n FROM memories WHERE superseded_by IS NULL")[0]['n']
                cats = q(MEM_DB, "SELECT category, COUNT(*) n FROM memories WHERE superseded_by IS NULL GROUP BY category")
                edges = q(MEM_DB, "SELECT COUNT(*) n FROM memory_edges")[0]['n']
                first = q(CTX_DB, "SELECT MIN(ts) t FROM raw")[0]['t']
                last = q(CTX_DB, "SELECT MAX(ts) t FROM raw")[0]['t']
                try: ledger = json.load(open(LEDGER))
                except Exception: ledger = []
                try: threads = json.load(open(THREADS))
                except Exception: threads = []
                return self.send_json({'raw': raw, 'memories': mem, 'edges': edges,
                    'categories': cats, 'raw_first': first, 'raw_last': last,
                    'ledger': len(ledger), 'threads': len(threads)})

            if p == '/api/raw/stars':
                # 全量星表：紧凑数组 [epoch_min, role01]，按时间排序
                rows = q(CTX_DB, "SELECT ts, role FROM raw ORDER BY ts ASC")
                import datetime
                out = []
                for r in rows:
                    ts = r['ts']
                    try:
                        t = datetime.datetime.fromisoformat(ts.replace('Z', '+00:00'))
                        em = int(t.timestamp() // 60)
                    except Exception:
                        em = 0
                    out.append([em, 1 if r['role'] == 'user' else 0])
                return self.send_json({'n': len(out), 'stars': out})

            if p == '/api/raw/at':
                # 按 epoch-minute 拿该分钟内的原文（星点点击用）
                em = get('em')
                if not em: return self.send_json({'error': 'em required'}, 400)
                import datetime
                t0 = datetime.datetime.fromtimestamp(int(em)*60).astimezone()
                # 匹配该分钟的 ts 前缀（本地 ISO）
                rows = q(CTX_DB, "SELECT ts, role, text FROM raw ORDER BY ts ASC")
                # 效率妥协：直接二分——改为 SQL 范围查
                lo = t0.isoformat()[:16]
                rows = q(CTX_DB, "SELECT ts, role, text FROM raw WHERE substr(ts,1,16) = ? LIMIT 10", (lo,))
                return self.send_json({'rows': rows})

            if p == '/api/raw/search':
                term = get('q', '').strip()
                limit = min(int(get('limit', '50')), 200)
                role = get('role')
                if not term: return self.send_json({'hits': []})
                sql = "SELECT ts, source, role, text FROM raw WHERE raw MATCH ?"
                args = [fts_quote(term)]
                if role in ('user', 'assistant'):
                    sql += " AND role = ?"; args.append(role)
                sql += " ORDER BY ts DESC LIMIT ?"; args.append(limit)
                return self.send_json({'hits': q(CTX_DB, sql, args)})

            if p == '/api/raw/context':
                # 某时间点前后各 N 条，看现场
                ts = get('ts'); n = min(int(get('n', '6')), 20)
                if not ts: return self.send_json({'error': 'ts required'}, 400)
                before = q(CTX_DB, "SELECT ts,role,text FROM raw WHERE ts <= ? ORDER BY ts DESC LIMIT ?", (ts, n+1))
                after = q(CTX_DB, "SELECT ts,role,text FROM raw WHERE ts > ? ORDER BY ts ASC LIMIT ?", (ts, n))
                return self.send_json({'context': list(reversed(before)) + after})

            if p == '/api/mem/list':
                limit = min(int(get('limit', '200')), 500)
                cat = get('category')
                sql = "SELECT id,content,category,tags,importance,pinned,created_at,recalled_count FROM memories WHERE superseded_by IS NULL"
                args = []
                if cat: sql += " AND category=?"; args.append(cat)
                sql += " ORDER BY created_at DESC LIMIT ?"; args.append(limit)
                return self.send_json({'memories': q(MEM_DB, sql, args)})

            if p == '/api/mem/search':
                term = get('q', '').strip()
                if not term: return self.send_json({'memories': []})
                rows = q(MEM_DB, """SELECT m.id,m.content,m.category,m.tags,m.importance,m.pinned,m.created_at
                    FROM memories_fts f JOIN memories m ON m.id=f.rowid
                    WHERE memories_fts MATCH ? AND m.superseded_by IS NULL LIMIT 100""", (fts_quote(term),))
                return self.send_json({'memories': rows})

            if p == '/api/mem/edges':
                mid = get('id')
                if mid:
                    rows = q(MEM_DB, """SELECT e.*, m1.content s_content, m2.content t_content
                        FROM memory_edges e JOIN memories m1 ON m1.id=e.source_id JOIN memories m2 ON m2.id=e.target_id
                        WHERE e.source_id=? OR e.target_id=?""", (mid, mid))
                else:
                    rows = q(MEM_DB, "SELECT * FROM memory_edges ORDER BY created_at DESC LIMIT 500")
                return self.send_json({'edges': rows})

            if p == '/api/mem/graph':
                nodes = q(MEM_DB, "SELECT id, substr(content,1,60) label, category, importance, created_at FROM memories WHERE superseded_by IS NULL")
                edges = q(MEM_DB, "SELECT id, source_id, target_id, relation, context FROM memory_edges")
                return self.send_json({'nodes': nodes, 'edges': edges})

            if p == '/api/digest':
                days = q(CTX_DB, "SELECT day,total,hers,mine,first_ts,last_ts FROM digest ORDER BY day ASC")
                narrs = q(CTX_DB, "SELECT day,content FROM digest_narrative ORDER BY day DESC")
                weeks = q(CTX_DB, "SELECT week_end,content FROM digest_week ORDER BY week_end DESC")
                import re as _re
                def _peel(txt, key):
                    try: return json.loads(txt).get(key, txt)
                    except Exception:
                        m = _re.search(r'"'+key+r'"\s*:\s*"(.*)', txt, _re.S)
                        if m:
                            body = m.group(1)
                            body = _re.sub(r'"\s*\}?\s*$', '', body)
                            return body.replace('\\n', '\n').replace('\\"', '"')
                        return txt
                for n in narrs:
                    n['content'] = _peel(n['content'], 'narrative')
                for w in weeks:
                    w['content'] = _peel(w['content'], 'week_narrative')
                return self.send_json({'days': days, 'narratives': narrs, 'weeks': weeks})

            if p == '/api/inner':
                # 内心系统聚合：驱力 + 梦 + 天气残留 + 节律（全只读）
                import time as _t
                out = {}
                try:
                    rows = q(os.environ.get('DESIRE_DB', DATA + '/desire.db'),
                             "SELECT drives_json, last_ts, tick_count FROM drive_state LIMIT 1")
                    if rows:
                        out['drives'] = json.loads(rows[0]['drives_json'])
                        out['drives_ts'] = rows[0]['last_ts']
                        out['ticks'] = rows[0]['tick_count']
                    g = q(os.environ.get('DESIRE_DB', DATA + '/desire.db'),
                          "SELECT layer, protest_ticks FROM grief_state LIMIT 1")
                    if g: out['grief'] = g[0]
                except Exception as e:
                    out['drives_error'] = str(e)
                try:
                    out['dream'] = json.load(open(DATA + '/latest_dream.json'))
                except Exception: pass
                try:
                    w = json.load(open(DATA + '/weather_residue.json'))
                    out['weather'] = {'warmth': w.get('warmth_residue'), 'shadow': w.get('shadow_residue'),
                                      'updated_at': w.get('updated_at')}
                except Exception: pass
                return self.send_json(out)

            if p == '/api/identity':
                import glob as _g
                cand, comm = [], []
                for f in sorted(_g.glob(os.environ.get('IDENTITY_DIR', DATA + '/identity') + '/candidates/*.json')):
                    try: cand.append(json.load(open(f)))
                    except Exception: pass
                for f in sorted(_g.glob(os.environ.get('IDENTITY_DIR', DATA + '/identity') + '/committed/*.json')):
                    try: comm.append(json.load(open(f)))
                    except Exception: pass
                return self.send_json({'candidates': cand, 'committed': comm})

            if p == '/api/diary/list':
                import glob as _g, os as _os
                out = []
                for src, base in (('main', os.environ.get('DIARY_MAIN', DATA + '/diary')),
                                  ('heart', os.environ.get('DIARY_HEART', DATA + '/diary-heart'))):
                    for f in sorted(_g.glob(base + '/*.md'), reverse=True):
                        out.append({'src': src, 'name': _os.path.basename(f),
                                    'size': _os.path.getsize(f)})
                return self.send_json({'diaries': out})

            if p == '/api/diary/get':
                import os as _os, re as _re
                src = get('src'); name = get('f', '')
                base = {'main': os.environ.get('DIARY_MAIN', DATA + '/diary'),
                        'heart': os.environ.get('DIARY_HEART', DATA + '/diary-heart')}.get(src)
                if not base or not _re.fullmatch(r'[\w.\-]+\.md', name):
                    return self.send_json({'error': 'bad path'}, 400)
                fp = _os.path.join(base, name)
                if not _os.path.isfile(fp): return self.send_json({'error': 'not found'}, 404)
                return self.send_json({'name': name, 'text': open(fp).read()})

            if p == '/api/admin/status':
                # 运维状态面板（纯只读聚合）
                import subprocess, os as _os, glob as _g, time as _t
                out = {}
                # systemd 服务/定时器
                units = [u for u in os.environ.get('WATCH_UNITS', '').split(',') if u]
                st = {}
                for u in units:
                    try:
                        r = subprocess.run(['systemctl', 'is-active', u],
                                           capture_output=True, text=True, timeout=5)
                        st[u] = r.stdout.strip()
                    except Exception:
                        st[u] = 'unknown'
                out['units'] = st
                tm = {}
                for u in [u for u in os.environ.get('WATCH_TIMERS', '').split(',') if u]:
                    try:
                        r = subprocess.run(['systemctl', 'show', u, '-p',
                                            'LastTriggerUSec', '-p', 'NextElapseUSecRealtime'],
                                           capture_output=True, text=True, timeout=5)
                        kv = dict(l.split('=', 1) for l in r.stdout.strip().splitlines() if '=' in l)
                        tm[u] = {'last': kv.get('LastTriggerUSec', ''), 'next': kv.get('NextElapseUSecRealtime', '')}
                    except Exception:
                        tm[u] = {}
                out['timers'] = tm
                # 本地备份链
                bs = sorted(_g.glob(os.environ.get('BACKUP_GLOB', './backups/*.tar.gz*')), key=_os.path.getmtime)
                if bs:
                    f = bs[-1]
                    out['backup'] = {'latest': _os.path.basename(f), 'size': _os.path.getsize(f),
                                     'mtime': int(_os.path.getmtime(f)), 'count': len(bs)}
                # digest 管线新鲜度
                try:
                    out['digest_day'] = q(CTX_DB, "SELECT MAX(day) d FROM digest_narrative")[0]['d']
                    out['digest_week'] = q(CTX_DB, "SELECT MAX(week_end) w FROM digest_week")[0]['w']
                except Exception:
                    pass
                # 库体积
                out['db'] = {'context': _os.path.getsize(CTX_DB), 'memory': _os.path.getsize(MEM_DB)}
                # 冲动队列
                try:
                    iq = json.load(open(os.environ.get('IMPULSE_QUEUE', DATA + '/impulse-queue.json')))
                    out['impulse_pending'] = sum(1 for x in iq if not x.get('done'))
                except Exception:
                    pass
                # DeepSeek 余额（key 不出门，只回余额数字；缓存 5 分钟）
                global _DS_CACHE
                try:
                    _DS_CACHE
                except NameError:
                    _DS_CACHE = {'t': 0, 'v': None}
                if _t.time() - _DS_CACHE['t'] > 300:
                    try:
                        import urllib.request as _ur
                        k = os.environ.get('DIGEST_API_KEY')
                        if k:
                            rq = _ur.Request('https://api.deepseek.com/user/balance',
                                             headers={'Authorization': 'Bearer ' + k})
                            b = json.loads(_ur.urlopen(rq, timeout=8).read())
                            infos = b.get('balance_infos') or [{}]
                            bi = next((x for x in infos if float(x.get('total_balance') or 0) > 0), infos[0])
                            _DS_CACHE = {'t': _t.time(), 'v': {
                                'available': b.get('is_available'),
                                'balance': bi.get('total_balance'), 'currency': bi.get('currency')}}
                    except Exception as e:
                        _DS_CACHE = {'t': _t.time(), 'v': {'error': str(e)[:80]}}
                out['deepseek'] = _DS_CACHE['v']
                return self.send_json(out)

            if p == '/api/nightwatch':
                # 守夜月相：全部心跳到岗签名"·"的时间戳
                rows = q(CTX_DB, "SELECT ts FROM raw WHERE role='assistant' AND trim(text)='·' ORDER BY ts ASC")
                return self.send_json({'n': len(rows), 'ts': [r['ts'] for r in rows]})

            if p == '/api/wander':
                m = q(MEM_DB, "SELECT id,content,category,importance,created_at FROM memories WHERE superseded_by IS NULL ORDER BY RANDOM() LIMIT 1")
                r = q(CTX_DB, "SELECT ts,role,text FROM raw WHERE length(text) BETWEEN 12 AND 300 ORDER BY RANDOM() LIMIT 1")
                return self.send_json({'memory': m[0] if m else None, 'raw': r[0] if r else None})

            if p == '/api/ledger':
                try: led = json.load(open(LEDGER))
                except Exception: led = []
                return self.send_json({'ledger': led})
            if p == '/api/threads':
                try: th = json.load(open(THREADS))
                except Exception: th = []
                return self.send_json({'threads': th})

            return self.send_json({'error': 'not found'}, 404)
        except Exception as e:
            return self.send_json({'error': str(e)}, 500)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', '8021'))
    host = os.environ.get('HOST', '127.0.0.1')
    print(f'archive-room api on {host}:{port}')
    HTTPServer((host, port), H).serve_forever()
