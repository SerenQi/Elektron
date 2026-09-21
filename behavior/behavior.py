#!/usr/bin/env python3
"""情绪唤醒桥。

情绪只有唤醒权，没有行动决策权。这里读取 desire 与潮汐，在情绪越过阈值或
发生明显变化时写入一条中性的 emotion_wake，并即时唤醒 Claude。具体做什么、
是否暂时不做，由醒来的 Claude 结合记忆和现实边界自行判断。
"""
import os, json, os, signal, sqlite3, subprocess, tempfile, time
from datetime import datetime
from pathlib import Path

from impulse_queue import ImpulseQueue

# 路径一律走环境变量，克隆下来改 .env 就能跑
HOME_DIR = Path(os.environ.get('ELEKTRON_HOME', './data'))
AGENT_DIR = Path(os.environ.get('AGENT_HOME', './agent'))
QUEUE = str(HOME_DIR / 'dynamic' / 'impulse-queue.json')
HEARTBEAT_PID = AGENT_DIR / '.pid-heartbeat'
HEARTBEAT_DIR = AGENT_DIR
HISTORY_FILE = Path(__file__).parent / 'emotion-history.json'
TRIGGER_STATE_FILE = Path(__file__).parent / 'emotion-trigger-state.json'
WAKE_KIND = 'emotion_wake'
ABSOLUTE_MIN_GAP_H = 0.5
RELATIVE_RISE = 0.08
HISTORY_RETENTION_H = 30
DAY_BASELINE_H = 24
DAY_BASELINE_TOLERANCE_H = 3
WARM_BASELINE_MIN_H = 0.5
queue_store = ImpulseQueue(QUEUE)

def outcome_age_hours(item, now):
    try:
        return (now - datetime.fromisoformat(item.get('outcome_at')).timestamp()) / 3600
    except (TypeError, ValueError):
        return None

def desire_drives():
    db = sqlite3.connect(f'file:{HOME_DIR}/desire.db?mode=ro', uri=True)
    row = db.execute("SELECT drives_json FROM drive_state LIMIT 1").fetchone()
    db.close()
    return json.loads(row[0]) if row else {}

def _envfile():
    cfg = {}
    try:
        for line in open(Path(__file__).parent / '.env', encoding='utf-8'):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1); cfg[k] = v
    except FileNotFoundError:
        pass
    return cfg

def tide_drives():
    cfg = _envfile()
    url, host, auth = cfg.get('TIDE_API_URL'), cfg.get('TIDE_API_HOST'), cfg.get('TIDE_API_AUTH')
    if not url:
        return {}
    try:
        cmd = ['curl','-s','-m','8','-k']
        if auth: cmd += ['-u', auth]
        if host: cmd += ['-H', 'Host: ' + host]
        cmd.append(url)
        out = subprocess.run(cmd, capture_output=True, text=True).stdout
        return json.loads(out).get('drives', {})
    except Exception:
        return {}

SIGNAL_THRESHOLDS = [
    # (维度, 来源, 唤醒阈值)。这里只决定是否值得醒来，不映射任何行为。
    ('curiosity',  'any',    0.50),
    ('boredom',    'tide',   0.60),
    ('share',      'tide',   0.60),
    ('stress',     'desire', 0.50),
    ('grieve',     'tide',   0.55),
    ('social',     'any',    0.55),
    ('reflection', 'any',    0.60),
]

def _number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _snapshot(values):
    return {key: round(_number(value), 3) for key, value in values.items()
            if isinstance(value, (int, float))}


def _sample_values(desire, tide):
    values = {f'desire.{key}': value for key, value in _snapshot(desire).items()}
    values.update({f'tide.{key}': value for key, value in _snapshot(tide).items()})
    return values


def _load_json(path, default):
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return data
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def _write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as out:
            json.dump(data, out, ensure_ascii=False, indent=1)
            out.write('\n')
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _record_history(now, values):
    history = _load_json(HISTORY_FILE, [])
    if not isinstance(history, list):
        history = []
    cutoff = now - HISTORY_RETENTION_H * 3600
    history = [row for row in history
               if isinstance(row, dict) and _number(row.get('ts')) >= cutoff]
    history.append({'ts': now, 'values': values})
    _write_json(HISTORY_FILE, history)
    return history[:-1]


def _baseline(history, now):
    eligible = [row for row in history
                if now - _number(row.get('ts')) >= WARM_BASELINE_MIN_H * 3600]
    if not eligible:
        return None, None, None
    target = now - DAY_BASELINE_H * 3600
    nearest = min(eligible, key=lambda row: abs(_number(row.get('ts')) - target))
    distance_h = abs(_number(nearest.get('ts')) - target) / 3600
    if distance_h <= DAY_BASELINE_TOLERANCE_H:
        kind = 'same_time_yesterday'
    else:
        nearest = min(eligible, key=lambda row: _number(row.get('ts')))
        kind = 'warmup_oldest'
    age_h = (now - _number(nearest.get('ts'))) / 3600
    return nearest.get('values') or {}, round(age_h, 2), kind


def _absolute_candidates(desire, tide):
    found = {}
    for key, source, threshold in SIGNAL_THRESHOLDS:
        desire_value = _number(desire.get(key))
        tide_value = _number(tide.get(key))
        value = tide_value if source == 'tide' else desire_value if source == 'desire' \
            else max(desire_value, tide_value)
        if value >= threshold:
            candidate_key = f'absolute:{source}:{key}'
            found[candidate_key] = {
                'id': candidate_key,
                'name': key,
                'source': source,
                'trigger': 'absolute_threshold',
                'value': round(value, 3),
                'threshold': threshold,
                'desire_value': round(desire_value, 3),
                'tide_value': round(tide_value, 3),
            }
    return found


def _relative_candidates(values, baseline_values, baseline_age_h, baseline_kind):
    found = {}
    if not baseline_values:
        return found
    for full_name, value in values.items():
        if full_name not in baseline_values:
            continue
        before = _number(baseline_values.get(full_name))
        rise = _number(value) - before
        if rise < RELATIVE_RISE:
            continue
        source, name = full_name.split('.', 1)
        candidate_key = f'relative:{full_name}'
        found[candidate_key] = {
            'id': candidate_key,
            'name': name,
            'source': source,
            'trigger': 'relative_rise',
            'value': round(_number(value), 3),
            'baseline': round(before, 3),
            'delta': round(rise, 3),
            'threshold': RELATIVE_RISE,
            'baseline_age_hours': baseline_age_h,
            'baseline_kind': baseline_kind,
        }
    return found


def _heartbeat_identity(pid):
    try:
        cmdline = Path(f'/proc/{pid}/cmdline').read_bytes().rstrip(b'\0').split(b'\0')
        cwd = Path(os.readlink(f'/proc/{pid}/cwd'))
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError) as exc:
        return False, f'{type(exc).__name__}: {exc}'
    command_ok = (len(cmdline) >= 3 and
                  Path(os.fsdecode(cmdline[0])).name.startswith('python3') and
                  os.fsdecode(cmdline[-1]) == 'packages/imprint_heartbeat/agent.py')
    cwd_ok = cwd == HEARTBEAT_DIR
    if not command_ok or not cwd_ok:
        readable = ' '.join(os.fsdecode(part) for part in cmdline)
        return False, f'identity mismatch: cmd={readable!r}, cwd={str(cwd)!r}'
    return True, f'pid={pid}, cmd=heartbeat agent, cwd={cwd}'


def _wake_heartbeat():
    try:
        pid = int(HEARTBEAT_PID.read_text(encoding='utf-8').strip())
        valid, detail = _heartbeat_identity(pid)
        if not valid:
            return False, detail
        os.kill(pid, signal.SIGUSR1)
        return True, f'SIGUSR1->{pid}'
    except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError) as exc:
        return False, f'{type(exc).__name__}: {exc}'


def main():
    desire = desire_drives()
    tide = tide_drives()
    now = time.time()
    values = _sample_values(desire, tide)
    previous_history = _record_history(now, values)
    baseline_values, baseline_age_h, baseline_kind = _baseline(previous_history, now)
    candidates = _absolute_candidates(desire, tide)
    candidates.update(_relative_candidates(values, baseline_values,
                                            baseline_age_h, baseline_kind))
    state = _load_json(TRIGGER_STATE_FILE, {'initialized': False, 'latched': {}})
    if not isinstance(state, dict):
        state = {'initialized': False, 'latched': {}}
    latched = state.get('latched') if isinstance(state.get('latched'), dict) else {}
    active_keys = set(candidates)
    latched = {key: value for key, value in latched.items() if key in active_keys}

    if not state.get('initialized'):
        state = {'initialized': True, 'latched': {
            key: {'value': candidate.get('value'), 'at': now}
            for key, candidate in candidates.items()
        }}
        _write_json(TRIGGER_STATE_FILE, state)
        queue_store.prune()
        print(f'wake: none | reason: baseline initialized | latched: {len(candidates)}')
        return

    new_keys = [key for key in candidates if key not in latched]
    signals = [candidates[key] for key in new_keys]
    state['latched'] = latched
    _write_json(TRIGGER_STATE_FILE, state)

    if not signals:
        print(f'wake: none | reason: no new emotion crossing | active: {len(candidates)}')
        queue_store.prune()
        return

    queue = queue_store.snapshot()  # 同时迁移旧格式、回收过期 lease
    active = any(item.get('kind') == WAKE_KIND and
                 item.get('status') in {'pending', 'claimed'} for item in queue)
    if active:
        print('wake: none | reason: emotion wake already pending')
        return

    recent = queue_store.recent_outcome(WAKE_KIND)
    if recent and recent.get('status') == 'deferred' and float(recent.get('retry_at') or 0) > now:
        print('wake: none | reason: previous wake is deferred')
        return
    if recent:
        age_h = outcome_age_hours(recent, now)
        if age_h is not None and age_h < ABSOLUTE_MIN_GAP_H:
            print(f'wake: none | reason: minimum gap ({age_h:.2f}h)')
            return
        if age_h is not None and recent.get('status') == 'failed' and age_h < 1:
            print(f'wake: none | reason: failed wake cooldown ({age_h:.2f}h)')
            return

    item = queue_store.enqueue({
        'kind': WAKE_KIND,
        'drive': 'emotion',
        'value': max(item['value'] for item in signals),
        'desc': '情绪跨过阈值或相对基线明显上涨。醒来感受它，结合记忆与现实自行决定是否行动；情绪不指定行动。',
        'signals': signals,
        'emotion_signature': {item['id']: item['value'] for item in signals},
        'emotion_snapshot': {
            'desire': _snapshot(desire),
            'tide': _snapshot(tide),
        },
        'baseline': {
            'kind': baseline_kind,
            'age_hours': baseline_age_h,
        },
        'born_at': datetime.now().astimezone().isoformat(timespec='seconds'),
    })
    for key in new_keys:
        latched[key] = {'value': candidates[key].get('value'), 'at': now}
    state['latched'] = latched
    _write_json(TRIGGER_STATE_FILE, state)
    sent, detail = _wake_heartbeat()
    queue_store.prune()
    print(f"wake: added {item['id']} | signal: {sent} ({detail}) | "
          f"emotions: {','.join(signal['name'] for signal in signals)}")

if __name__ == '__main__':
    main()
