#!/usr/bin/env python3
# 周结：聚合7天叙事日结 → DeepSeek 生成周摘要（近细远粗的中层）
import os, sqlite3, sys, datetime, json, urllib.request
from pathlib import Path

DB = os.environ.get('ELEKTRON_HOME', './data') + '/context.db'

def _dskey():
    if os.environ.get('DIGEST_API_KEY'):
        return os.environ['DIGEST_API_KEY']
    try:
        for line in open(Path(__file__).parent.parent / '.env', encoding='utf-8'):
            if line.startswith('DIGEST_API_KEY='):
                return line.strip().split('=', 1)[1]
    except FileNotFoundError:
        pass

PROMPT = """你是Elektron 记忆库的周结整理器，为一对长期对话的伙伴（human / agent）把7天的日结聚合成周摘要。
规则：1.保留具体现场与她的原话引用，不许抽象成套话 2.方波（事实/决定/工程/约定）与弦波（情绪线/关系温度）分开 3.只用日结里有的内容，禁止发明 4.标出本周最重要的3个时刻
输出JSON：{"week_narrative":"周叙事400字内","key_moments":["三个最重时刻，带日期"],"square":["方波要点"],"sine":["弦波要点"]}"""

def build_week(end_day=None):
    c = sqlite3.connect(DB)
    c.execute('CREATE TABLE IF NOT EXISTS digest_week(week_end TEXT PRIMARY KEY, content TEXT, built_at TEXT)')
    end = datetime.date.fromisoformat(end_day) if end_day else datetime.date.today()
    days = [(end - datetime.timedelta(days=i)).isoformat() for i in range(6, -1, -1)]
    rows = []
    for d in days:
        r = c.execute('SELECT content FROM digest_narrative WHERE day=?', (d,)).fetchone()
        if r:
            rows.append(f'== {d} ==\n{r[0]}')
    if not rows:
        return '无日结可聚合'
    body = json.dumps({
        'model': 'deepseek-chat',
        'messages': [{'role': 'system', 'content': PROMPT},
                     {'role': 'user', 'content': '\n\n'.join(rows)}],
        'response_format': {'type': 'json_object'},
        'max_tokens': 1600, 'temperature': 0.3}).encode()
    req = urllib.request.Request('https://api.deepseek.com/v1/chat/completions', data=body,
                                 headers={'Authorization': 'Bearer ' + _dskey(), 'Content-Type': 'application/json'})
    out = json.loads(urllib.request.urlopen(req, timeout=180).read())['choices'][0]['message']['content']
    c.execute('INSERT OR REPLACE INTO digest_week VALUES(?,?,?)',
              (end.isoformat(), out, datetime.datetime.now().isoformat(timespec='seconds')))
    c.commit()
    return f'周结完成（截至{end}，含{len(rows)}天日结）'

if __name__ == '__main__':
    print(build_week(sys.argv[1] if len(sys.argv) > 1 else None))
