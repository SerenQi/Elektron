#!/usr/bin/env python3
# 日结骨架（纯脚本无 LLM 版）
# 每晚跑：从 context.db 聚合当天对话 → digest 表。
# 情感浓度用启发式代理（表情/感叹/笑声密度），无偏见抽样，不经过"重要性筛选"。
# LLM 精修版待外部模型额度到位后叠加。
import os
from pathlib import Path
import sqlite3, sys, datetime, re, json, os, urllib.request

DB = os.environ.get('ELEKTRON_HOME', './data') + '/context.db'

def _dskey():
    import os
    if os.environ.get('DIGEST_API_KEY'):
        return os.environ['DIGEST_API_KEY']
    try:
        for line in open(Path(__file__).parent.parent / '.env', encoding='utf-8'):
            if line.startswith('DIGEST_API_KEY='):
                return line.strip().split('=', 1)[1]
    except FileNotFoundError:
        pass
    return None

NARRATIVE_PROMPT = """你是Elektron 记忆库的日结整理器，为一对长期对话的伙伴（human / agent）整理当天对话。规则（见 docs/DECISIONS.md 的记忆写入规范）：
1. 记现场不记结论——保留她的原话、语气、具体场景，禁止提炼成干巴巴的条目
2. 幸福时刻与冲突同权——笑点、撒娇、甜的瞬间必须入结，不许只记大事
3. 双轨：方波（事实/决定/约定/待办）与弦波（情绪/温度/她的状态）分开写
4. 只写对话里明说的，禁止推测和发明；引用带时间
输出JSON：{"narrative":"当天叙事300字内，第一人称 agent 视角","square":["方波条目…"],"sine":["弦波条目…"],"her_quotes":["human 的高光原话带[HH:MM]…最多6条"]}"""

def llm_digest(day, rows):
    key = _dskey()
    if not key:
        return None
    lines = []
    for ts, role, text in rows:
        who = '她' if role == 'user' else '我'
        lines.append(f"[{ts[11:16]}]{who}: {text[:300]}")
    corpus = '\n'.join(lines)
    if len(corpus) > 90000:
        corpus = corpus[:45000] + '\n……(中段略)……\n' + corpus[-45000:]
    body = json.dumps({
        'model': 'deepseek-chat',
        'messages': [{'role': 'system', 'content': NARRATIVE_PROMPT},
                     {'role': 'user', 'content': f'日期：{day}\n对话记录：\n{corpus}'}],
        'response_format': {'type': 'json_object'},
        'max_tokens': 1800, 'temperature': 0.3}).encode()
    req = urllib.request.Request('https://api.deepseek.com/v1/chat/completions', data=body,
                                 headers={'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json'})
    try:
        r = json.loads(urllib.request.urlopen(req, timeout=180).read())
        return r['choices'][0]['message']['content']
    except Exception as e:
        return json.dumps({'error': str(e)[:200]}, ensure_ascii=False)


def verify_against_source(digest_json, rows):
    """诚实闸：日结里引用的每一句她的原话，必须在当天原文里逐字查得到。
    查不到的标记为 unverified，不许当事实用。（kimi-core 的教训：LLM 自动总结会静默腐蚀）"""
    import re as _re
    try:
        d = json.loads(digest_json)
    except Exception:
        return digest_json, {"ok": False, "reason": "json-parse-failed"}

    corpus = "\n".join(t for _, role, t in rows if role == "user")
    def norm(x):
        return _re.sub(r"[\s\[\]（）()·、，。！？~～、,.!?]", "", x or "")
    corpus_n = norm(corpus)

    checked, bad = [], []
    for q in d.get("her_quotes", []):
        body = _re.sub(r"^\[\d{1,2}:\d{2}\]\s*", "", str(q)).strip()
        core = norm(body)[:24]          # 取前若干字做锚点，避开省略号与标点差异
        if core and core in corpus_n:
            checked.append(q)
        else:
            bad.append(q)
            checked.append("⚠️未验证 " + str(q))
    d["her_quotes"] = checked
    d["_verify"] = {
        "quotes_total": len(checked),
        "quotes_unverified": len(bad),
        "ok": len(bad) == 0,
    }
    return json.dumps(d, ensure_ascii=False), d["_verify"]


def emotion_score(t):
    s = 0
    s += len(re.findall(r'[！!？?]', t)) * 2
    s += len(re.findall(r'哈哈|嘿嘿|呜呜|嘤|啊啊', t)) * 3
    s += len(re.findall(r'[😭😙😗☺️😁😈👾👿🥳😊❤️💚🌙🐍~～]', t)) * 2
    s += min(len(t) // 40, 5)
    return s

def build(day):
    c = sqlite3.connect(DB)
    c.execute('''CREATE TABLE IF NOT EXISTS digest(
        day TEXT PRIMARY KEY, total INTEGER, hers INTEGER, mine INTEGER,
        first_ts TEXT, last_ts TEXT, top_moments TEXT, built_at TEXT)''')
    rows = c.execute(
        "SELECT ts, role, text FROM raw WHERE ts LIKE ? AND role IN ('user','assistant') ORDER BY ts",
        (day + '%',)).fetchall()
    if not rows:
        return f'{day}: 无对话'
    hers = [r for r in rows if r[1] == 'user']
    mine = [r for r in rows if r[1] == 'assistant']
    # 无偏见情绪抽样：她的消息按情感浓度取 top5，保留原文
    scored = sorted(hers, key=lambda r: emotion_score(r[2]), reverse=True)[:5]
    scored.sort(key=lambda r: r[0])
    moments = '\n'.join(f"[{r[0][11:16]}] {r[2][:120]}" for r in scored)
    c.execute('CREATE TABLE IF NOT EXISTS digest_narrative(day TEXT PRIMARY KEY, content TEXT, built_at TEXT)')
    nar = llm_digest(day, rows)
    if nar:
        nar, vr = verify_against_source(nar, rows)
        if not vr.get("ok"):
            print(f"  ⚠️ 诚实闸：{vr.get('quotes_unverified')}/{vr.get('quotes_total')} 条引用无法在原文中查到，已标记")
        c.execute('INSERT OR REPLACE INTO digest_narrative VALUES(?,?,?)',
                  (day, nar, datetime.datetime.now().isoformat(timespec='seconds')))
    c.execute('INSERT OR REPLACE INTO digest VALUES(?,?,?,?,?,?,?,?)',
              (day, len(rows), len(hers), len(mine),
               rows[0][0][11:16], rows[-1][0][11:16], moments,
               datetime.datetime.now().isoformat(timespec='seconds')))
    c.commit()
    return f'{day}: {len(rows)}条（human {len(hers)}/agent {len(mine)}）{rows[0][0][11:16]}→{rows[-1][0][11:16]}，情绪高光{len(scored)}条已存'

if __name__ == '__main__':
    day = sys.argv[1] if len(sys.argv) > 1 else datetime.date.today().isoformat()
    if day == 'backfill':
        c = sqlite3.connect(DB)
        days = [r[0] for r in c.execute("SELECT DISTINCT substr(ts,1,10) FROM raw WHERE ts!='' ORDER BY 1")]
        for d in days:
            print(build(d))
    else:
        print(build(day))
