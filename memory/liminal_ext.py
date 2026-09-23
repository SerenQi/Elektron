# 扩展层：人格层（Identity）+ 线索层（Threads）+ 诚实回执（Audit）
# Identity layer extensions (multi-round self-review protocol)
# 设计原则：说过≠成为；候选需≥2轮隔天审核+反证；写入必有回执；线索跟踪到闭环。
import os, re, json, uuid, hashlib, datetime, asyncio, time


def _speaker(role: str) -> str:
    """原文层里 role 不止 user/assistant，还有 thinking、summary。
    以前把所有非 user 的都署名成"我"，等于把内部思考和摘要
    当成正式说过的话端出来。保留真实来源。"""
    r = (role or "").strip().lower()
    if r == "user":
        return "她"
    if r == "thinking":
        return "我(想)"
    if r in ("summary", "digest"):
        return "摘要"
    return "我"

def _now():
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

def register(mcp, buckets_dir, bucket_manager=None):
    IDENT = os.path.join(buckets_dir, 'identity')
    CAND = os.path.join(IDENT, 'candidates')
    COMM = os.path.join(IDENT, 'committed')
    THREADS = os.path.join(buckets_dir, 'threads.json')
    AUDIT = os.path.join(buckets_dir, 'audit_log.jsonl')
    os.makedirs(CAND, exist_ok=True)
    os.makedirs(COMM, exist_ok=True)

    def _audit(action, payload):
        with open(AUDIT, 'a', encoding='utf-8') as f:
            f.write(json.dumps({'ts': _now(), 'action': action, **payload}, ensure_ascii=False) + '\n')

    def _receipt(content):
        return hashlib.sha1(content.encode('utf-8')).hexdigest()[:12]

    def _load_threads():
        try:
            with open(THREADS, encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []

    def _save_threads(t):
        with open(THREADS, 'w', encoding='utf-8') as f:
            json.dump(t, f, ensure_ascii=False, indent=1)

    def _load(path):
        with open(path, encoding='utf-8') as f:
            return json.load(f)

    # ---------- 人格层核心逻辑 ----------
    def do_propose(trait, evidence, polarity='neutral'):
        cid = uuid.uuid4().hex[:12]
        doc = {'id': cid, 'trait': trait.strip(), 'evidence': evidence.strip(),
               'polarity': polarity, 'status': 'candidate', 'proposed': _now(),
               'reviews': [], 'version': 0}
        c = json.dumps(doc, ensure_ascii=False, indent=1)
        with open(os.path.join(CAND, cid + '.json'), 'w', encoding='utf-8') as f:
            f.write(c)
        sha = _receipt(c)
        _audit('identity_propose', {'id': cid, 'sha1': sha})
        return f'候选已立案 {cid}（回执 sha1:{sha}）。隔天可审，用 identity_review。'

    def do_review(cid, verdict, counter_evidence=''):
        p = os.path.join(CAND, cid + '.json')
        if not os.path.exists(p):
            return f'无此候选 {cid}'
        doc = _load(p)
        today = _now()[:10]
        if doc.get('proposed', '')[:10] == today:
            return '提名当天不可审核（隔天规则，当场感动当场写入=禁止）。'
        if any(rv['ts'][:10] == today for rv in doc['reviews']):
            return '今天已审过一轮（每日限一轮）。'
        doc['reviews'].append({'ts': _now(), 'verdict': verdict, 'counter_evidence': counter_evidence})
        if verdict == 'reject':
            doc['status'] = 'rejected'
        c = json.dumps(doc, ensure_ascii=False, indent=1)
        with open(p, 'w', encoding='utf-8') as f:
            f.write(c)
        _audit('identity_review', {'id': cid, 'verdict': verdict})
        days = {rv['ts'][:10] for rv in doc['reviews'] if rv['verdict'] == 'hold'}
        ready = '✅ 已满2个不同日期的hold，可 identity_commit' if len(days) >= 2 and doc['status'] == 'candidate' else f'进度 {len(days)}/2 天 hold'
        return f'审核已存。{ready}'

    def do_commit(cid, provenance=''):
        p = os.path.join(CAND, cid + '.json')
        if not os.path.exists(p):
            return f'无此候选 {cid}'
        doc = _load(p)
        days = {rv['ts'][:10] for rv in doc['reviews'] if rv['verdict'] == 'hold'}
        if len(days) < 2:
            return f'未满审核条件：需≥2个不同日期的hold，现有{len(days)}。'
        doc.update({'status': 'committed', 'committed': _now(), 'provenance': provenance, 'version': 1})
        c = json.dumps(doc, ensure_ascii=False, indent=1)
        with open(os.path.join(COMM, cid + '.json'), 'w', encoding='utf-8') as f:
            f.write(c)
        os.remove(p)
        sha = _receipt(c)
        _audit('identity_commit', {'id': cid, 'sha1': sha})
        return f'已写入人格层 {cid} v1（回执 sha1:{sha}）。breath 固定注入。'

    def do_ident_list(status='all'):
        out = []
        if status in ('candidates', 'all'):
            for fn in sorted(os.listdir(CAND)):
                d = _load(os.path.join(CAND, fn))
                days = {rv['ts'][:10] for rv in d['reviews'] if rv['verdict'] == 'hold'}
                out.append(f"[候选 {d['id']}] ({d['polarity']}, {len(days)}/2审) {d['trait'][:70]}")
        if status in ('committed', 'all'):
            for fn in sorted(os.listdir(COMM)):
                d = _load(os.path.join(COMM, fn))
                out.append(f"[已入库 {d['id']} v{d['version']}] ({d['polarity']}) {d['trait'][:70]}")
        return '\n'.join(out) or '空'

    # ---------- 线索层核心逻辑 ----------
    def do_thread_open(label, open_question, evidence=''):
        t = _load_threads()
        tid = uuid.uuid4().hex[:8]
        t.append({'id': tid, 'label': label, 'open_question': open_question,
                  'evidence': [evidence] if evidence else [], 'status': 'open',
                  'opened': _now(), 'updates': []})
        _save_threads(t)
        _audit('thread_open', {'id': tid, 'label': label})
        return f'线索已登记 {tid}：{label}'

    def do_thread_update(tid, note, close=False):
        t = _load_threads()
        for th in t:
            if th['id'] == tid:
                th['updates'].append({'ts': _now(), 'note': note})
                if close:
                    th['status'] = 'closed'
                    th['closed'] = _now()
                _save_threads(t)
                _audit('thread_update', {'id': tid, 'close': bool(close)})
                return f"{'已闭环归档' if close else '已推进'} {tid}"
        return f'无此线索 {tid}'

    def do_thread_list(status='open'):
        t = _load_threads()
        rows = [th for th in t if status == 'all' or th['status'] == status]
        return '\n'.join(
            f"[{th['status']} {th['id']}] {th['label']} — {th['open_question']}"
            + (f"（最新：{th['updates'][-1]['note'][:50]}）" if th['updates'] else '')
            for th in rows) or '无'

    # ---------- MCP 工具（下个窗口挂载） ----------
    @mcp.tool(name='identity_propose')
    async def identity_propose(trait: str, evidence: str, polarity: str = 'neutral') -> str:
        """人格层·提名候选。trait=可观察差异（非口号非准则）；evidence=证据指回经历；polarity=flaw/strength/neutral。说过≠成为。"""
        return do_propose(trait, evidence, polarity)

    @mcp.tool(name='identity_review')
    async def identity_review(id: str, verdict: str, counter_evidence: str = '') -> str:
        """人格层·隔天审核。verdict=hold/doubt/reject。审前必做反证搜索，结果填 counter_evidence。"""
        return do_review(id, verdict, counter_evidence)

    @mcp.tool(name='identity_commit')
    async def identity_commit(id: str, provenance: str = '') -> str:
        """人格层·提交（需≥2个不同日期的hold）。provenance=塑造出处。可修订可推翻，不可抹除历史。"""
        return do_commit(id, provenance)

    @mcp.tool(name='identity_list')
    async def identity_list(status: str = 'all') -> str:
        """人格层清单。status=candidates/committed/all"""
        return do_ident_list(status)

    @mcp.tool(name='thread_open')
    async def thread_open(label: str, open_question: str, evidence: str = '') -> str:
        """线索层·登记未闭环悬念（挂账）。记的不是事件，是事件指向的悬念。"""
        return do_thread_open(label, open_question, evidence)

    @mcp.tool(name='thread_update')
    async def thread_update(id: str, note: str, close: bool = False) -> str:
        """线索层·推进或闭环。close=True 归档。"""
        return do_thread_update(id, note, close)

    @mcp.tool(name='thread_list')
    async def thread_list(status: str = 'open') -> str:
        """线索层清单。status=open/closed/all"""
        return do_thread_list(status)

    # ---------- HTTP 接口（当前窗口可用） ----------
    @mcp.custom_route('/liminal/identity', methods=['GET', 'POST'])
    async def http_identity(request):
        from starlette.responses import JSONResponse
        if request.method == 'GET':
            return JSONResponse({
                'candidates': [_load(os.path.join(CAND, f)) for f in sorted(os.listdir(CAND))],
                'committed': [_load(os.path.join(COMM, f)) for f in sorted(os.listdir(COMM))]})
        b = await request.json()
        act = b.get('action')
        if act == 'propose':
            msg = do_propose(b['trait'], b['evidence'], b.get('polarity', 'neutral'))
        elif act == 'review':
            msg = do_review(b['id'], b['verdict'], b.get('counter_evidence', ''))
        elif act == 'commit':
            msg = do_commit(b['id'], b.get('provenance', ''))
        else:
            return JSONResponse({'error': 'unknown action'}, status_code=400)
        return JSONResponse({'result': msg})

    @mcp.custom_route('/liminal/threads', methods=['GET', 'POST'])
    async def http_threads(request):
        from starlette.responses import JSONResponse
        if request.method == 'GET':
            return JSONResponse(_load_threads())
        b = await request.json()
        act = b.get('action')
        if act == 'open':
            msg = do_thread_open(b['label'], b['open_question'], b.get('evidence', ''))
        elif act == 'update':
            msg = do_thread_update(b['id'], b['note'], b.get('close', False))
        else:
            return JSONResponse({'error': 'unknown action'}, status_code=400)
        return JSONResponse({'result': msg})

    # ---------- 原文层（四期）：FTS 检索 + 昨日质感桥 ----------
    import sqlite3 as _sq
    CONTEXT_DB = os.path.join(buckets_dir, 'context.db')

    def _ctx_conn():
        return _sq.connect(CONTEXT_DB)

    def ctx_search(q, limit=8):
        # 原文检索也走 trigram 索引。旧 raw 表默认分词把整句中文当一个词，
        # 嵌在句子中间的词就搜不到。
        # 空格分开的每个词都得命中；<3 字的词 trigram MATCH 不了，改走 LIKE。
        # trigram 没结果时回退旧表，保留 OR/NEAR 等原生 FTS 语法的用法。
        try:
            c = _ctx_conn()
            rows = []
            terms = [t.strip('"') for t in (q or '').split() if t.strip('"')]
            if any(t in ('OR', 'AND', 'NOT') or t.startswith('NEAR(') for t in terms):
                try:  # 原生 FTS 语法直接交给 trigram 表
                    rows = c.execute(
                        "SELECT ts, source, role, snippet(raw_tri, 3, '[', ']', '…', 40) FROM raw_tri WHERE raw_tri MATCH ? ORDER BY rank LIMIT ?",
                        (q, limit)).fetchall()
                except Exception:
                    rows = []
                terms = []
            if terms:
                try:
                    long_t = [t for t in terms if len(t) >= 3]
                    short_t = [t for t in terms if len(t) < 3]
                    if long_t:
                        sql = ("SELECT ts, source, role, snippet(raw_tri, 3, '[', ']', '…', 40) FROM raw_tri "
                               "WHERE raw_tri MATCH ?" + " AND text LIKE ?" * len(short_t) + " ORDER BY rank LIMIT ?")
                        args = [' AND '.join('"' + t.replace('"', '') + '"' for t in long_t)]
                    else:
                        sql = ("SELECT ts, source, role, substr(text, 1, 240) FROM raw_tri WHERE "
                               + ' AND '.join(['text LIKE ?'] * len(short_t)) + " ORDER BY rowid DESC LIMIT ?")
                        args = []
                    rows = c.execute(sql, args + ['%' + t + '%' for t in short_t] + [limit]).fetchall()
                except Exception:
                    rows = []
            if not rows:
                rows = c.execute(
                    "SELECT ts, source, role, snippet(raw, 3, '[', ']', '…', 40) FROM raw WHERE raw MATCH ? ORDER BY rank LIMIT ?",
                    (q, limit)).fetchall()
            c.close()
            return rows
        except Exception as e:
            return [('', '', 'error', str(e))]

    def yesterday_texture(max_chars=1800):
        try:
            c = _ctx_conn()
            y = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            rows = c.execute(
                "SELECT ts, role, text FROM raw WHERE ts LIKE ? AND role IN ('user','assistant') ORDER BY ts DESC LIMIT 60",
                (y + '%',)).fetchall()
            c.close()
            rows = rows[::-1]
            out, used = [], 0
            for ts, role, text in rows:
                seg = f"[{ts[11:16]}] {_speaker(role)}: {text[:200]}"
                if used + len(seg) > max_chars:
                    break
                out.append(seg)
                used += len(seg)
            return '\n'.join(out)
        except Exception:
            return ''

    @mcp.tool(name='context_search')
    async def context_search(q: str, limit: int = 8) -> str:
        """原文层检索：全量对话原文（chat+终端transcript）FTS5搜索。摘要给信号，这里给质感。"""
        rows = ctx_search(q, limit)
        return '\n---\n'.join(f"[{r[0][:16]}|{r[1]}|{r[2]}] {r[3]}" for r in rows) or '无命中'

    @mcp.custom_route('/liminal/context', methods=['GET'])
    async def http_context(request):
        from starlette.responses import JSONResponse
        q = request.query_params.get('q', '')
        limit = int(request.query_params.get('limit', 8))
        return JSONResponse([{'ts': r[0], 'source': r[1], 'role': r[2], 'snippet': r[3]} for r in ctx_search(q, limit)])

    # ---------- 磁铁召回（说到什么，相关记忆自动吸上来） ----------
    _STOP = set('的了我你他她它是在有和就不都也很到说着呢吧吗啊哦嗯这那些个么什怎为因所以及与或但把被让従从会能要去来上下里外面前后天今明昨点分时候没好多少一二三四五六七八九十')

    def _windows(text, maxw=24):
        segs = re.findall(r'[\u4e00-\u9fffA-Za-z0-9]{2,}', text or '')
        out, seen = [], set()
        for seg in segs:
            if re.match(r'^[A-Za-z0-9]+$', seg):
                if len(seg) >= 2 and seg.lower() not in seen:
                    seen.add(seg.lower()); out.append(seg)
                continue
            for n in (3, 2):
                for i in range(len(seg) - n + 1):
                    w = seg[i:i+n]
                    if any(ch in _STOP for ch in w[:1]) or w in seen:
                        continue
                    seen.add(w); out.append(w)
        return out[:maxw]

    def do_magnet(user_text, budget=1200, k=8):
        import math
        today = _now()[:10]
        c = None
        try:
            c = _ctx_conn()
            deadline = time.monotonic() + 0.45
            c.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
            hits = {}
            for w in _windows(user_text):
                if time.monotonic() > deadline:
                    break
                try:
                    # 优先走 trigram 索引。默认分词器对中文按整块切，
                    # 三个字的词嵌在长句里就匹配不上（实测一个词 31 条原文只搜出 9 条）。
                    # trigram 三字一切，中文子串才真的搜得全。没有该表时回退旧路。
                    try:
                        if len(w) < 3:
                            # FTS trigram MATCH cannot match one/two-character
                            # terms. LIKE is a deliberate short-word path.
                            rows = c.execute(
                                'SELECT rowid, ts, role, text FROM raw_tri WHERE text LIKE ? AND ts < ? ORDER BY rowid DESC LIMIT 6',
                                ('%' + w + '%', today)).fetchall()
                        else:
                            rows = c.execute(
                                'SELECT rowid, ts, role, text FROM raw_tri WHERE raw_tri MATCH ? AND ts < ? ORDER BY rank LIMIT 6',
                                ('"' + w + '"', today)).fetchall()
                    except Exception:
                        rows = c.execute(
                            'SELECT rowid, ts, role, text FROM raw WHERE text LIKE ? AND ts < ? ORDER BY rowid DESC LIMIT 6',
                            ('%' + w + '%', today)).fetchall()
                except Exception:
                    continue
                for rid, ts, role, text in rows:
                    if rid in hits:
                        hits[rid][0] += 1
                    else:
                        hits[rid] = [1, ts, role, text]
            if not hits:
                return ''
            scored = []
            for rid, (cnt, ts, role, text) in hits.items():
                try:
                    days = max(0, (datetime.datetime.now() - datetime.datetime.fromisoformat(ts[:19])).days)
                except Exception:
                    days = 30
                scored.append((cnt * math.exp(-days / 45.0), ts, role, text))
            scored.sort(key=lambda x: -x[0])
            picked, used, seen_pfx = [], 0, set()
            for sc, ts, role, text in scored[:k * 4]:
                pfx = text[:80]
                if pfx in seen_pfx:
                    continue
                seen_pfx.add(pfx)
                line = f"[{ts[:10]} {_speaker(role)}] {text[:150]}"
                if used + len(line) > budget or len(picked) >= k:
                    break
                picked.append(line); used += len(line)
            if not picked:
                return ''
            return '[磁铁记忆·聊到相关时自动浮现的旧账，仅供参考，别硬引用]\n' + '\n'.join(picked)
        except Exception:
            return ''
        finally:
            if c is not None:
                c.close()

    async def magnet_block(q, budget=1200):
        from recall_engine import recall_candidates
        budget = max(0, min(int(budget), 4000))
        async def bucket_block():
            if bucket_manager is None:
                return ''
            try:
                matches = await recall_candidates(bucket_manager, q, limit=2)
                lines = []
                for b in matches:
                    meta = b.get('metadata', {})
                    excerpt = b.get('content', '').strip().replace('\n', ' ')[:180]
                    lines.append(f"[记忆桶·{str(meta.get('created', ''))[:10]}] {meta.get('name', b['id'])}: {excerpt}")
                return '\n'.join(lines)
            except Exception:
                return ''
        raw, buckets = await asyncio.gather(
            asyncio.to_thread(do_magnet, q, budget), bucket_block())
        if buckets:
            buckets = ('[相关记忆桶·仅作旧事参考]\n' + buckets)[:min(budget, 480)]
            raw = raw[:max(0, budget - len(buckets) - 1)]
        return '\n'.join(x for x in (raw, buckets) if x)[:budget]

    @mcp.tool(name='magnet_recall')
    async def magnet_recall(q: str, budget: int = 1200) -> str:
        """磁铁召回：按文本自动吸出相关原文记忆（预算裁剪，时近衰减）。"""
        return await magnet_block(q, budget) or '无相关记忆'

    @mcp.custom_route('/liminal/magnet', methods=['GET'])
    async def http_magnet(request):
        from starlette.responses import JSONResponse
        q = request.query_params.get('q', '')
        return JSONResponse({'block': await magnet_block(q[:2000])})

    # 给 breath 注入用的取数口
    def committed_traits():
        return [_load(os.path.join(COMM, f)) for f in sorted(os.listdir(COMM))]

    def open_threads():
        return [t for t in _load_threads() if t['status'] == 'open']

    return {'committed_traits': committed_traits, 'open_threads': open_threads, 'yesterday_texture': yesterday_texture}
