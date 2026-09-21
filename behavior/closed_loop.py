#!/usr/bin/env python3
"""Bridge Elektron memory, drive events, and completed behavior impulses.

The first run establishes checkpoints only. This deliberately avoids replaying
the existing memory archive or old drive ledger into the live emotional state.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import urllib.request
from pathlib import Path

BASE = Path(os.environ.get("ELEKTRON_HOME", "./data"))
STATE = Path(__file__).parent / "closed-loop-state.json"
QUEUE = BASE / "dynamic" / "impulse-queue.json"
DB = BASE / "desire.db"
ELEKTRON_URL = os.environ.get("ELEKTRON_URL", "http://127.0.0.1:8002").rstrip("/")
ELEKTRON_SRC = os.environ.get("ELEKTRON_SRC", str(Path(__file__).parent.parent / "memory"))
MAX_MEMORY_PER_RUN = 3

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, ELEKTRON_SRC)
from impulse_queue import ImpulseQueue  # noqa: E402
from bucket_manager import BucketManager  # noqa: E402


def load_state():
    try:
        state = json.loads(STATE.read_text(encoding="utf-8"))
        return state if isinstance(state, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=STATE.name + ".", dir=STATE.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            json.dump(state, out, ensure_ascii=False, indent=2)
            out.write("\n")
        os.replace(tmp, STATE)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def request_json(path, body=None, timeout=20):
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        ELEKTRON_URL + path,
        data=data,
        headers={"Content-Type": "application/json"} if data else {},
        method="POST" if data else "GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def analyzer_entries():
    rows = request_json("/api/analyzer/entries?since=2026-06-25T00:00:00Z")
    return rows if isinstance(rows, list) else []


def ledger_rows(after_id=0):
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute(
            "SELECT id,ts,source,event_label,primary_drive,intensity,confidence,agency,"
            "suppressed,reason,brain_json,evidence_json FROM drive_event_ledger "
            "WHERE id>? ORDER BY id", (int(after_id or 0),)
        )]
    finally:
        conn.close()


def qualifies(row):
    return (
        not int(row.get("suppressed") or 0)
        and float(row.get("intensity") or 0) >= 0.5
        and float(row.get("confidence") or 0) >= 0.65
        and bool(str(row.get("primary_drive") or "").strip())
    )


def manager():
    return BucketManager({
        "buckets_dir": str(BASE),
        "matching": {"fuzzy_threshold": 50, "max_results": 5},
        "wikilink": {"enabled": False},
        "scoring_weights": {},
    })


async def create_emotion_memory(row):
    drive = str(row.get("primary_drive") or "")
    label = str(row.get("event_label") or drive)
    intensity = float(row.get("intensity") or 0)
    content = (
        f"一次强度较高的情绪事件被系统留下：{label}。\n\n"
        f"主要驱动：{drive}；强度：{intensity:.2f}；"
        f"置信度：{float(row.get('confidence') or 0):.2f}。\n"
        "这是一条当时的 feel 记录，不自动等同于已经想明白的 insight。"
    )
    valence = 0.28 if drive in {"stress", "possessiveness", "fatigue"} else 0.5
    return await manager().create(
        content=content,
        tags=["closed-loop", "emotion"],
        importance=6,
        domain=["self", "emotion"],
        valence=valence,
        arousal=min(1.0, max(0.3, intensity)),
        bucket_type="feel",
        name=f"情绪回声-{label}"[:40],
        drive_tags={drive: round(intensity, 3)},
        extra_meta={
            "source": "drive_event_ledger",
            "source_event_id": f"drive-{row['id']}",
            "source_event_ts": row.get("ts"),
            "event_confidence": round(float(row.get("confidence") or 0), 3),
        },
    )


async def create_behavior_memory(item):
    note = str(item.get("note") or "完成")
    content = (
        f"一次由情绪驱动的行动已经完成：{item.get('desc') or item.get('kind')}\n\n"
        f"结果：{note}"
    )
    return await manager().create(
        content=content,
        tags=["closed-loop", "behavior", str(item.get("kind") or "impulse")],
        importance=5,
        domain=["self", "behavior"],
        valence=0.58,
        arousal=0.4,
        bucket_type="feel",
        name=f"行动回声-{item.get('kind') or 'impulse'}",
        drive_tags={str(item.get("drive") or "reflection"): float(item.get("value") or 0)},
        extra_meta={
            "source": "behavior_impulse",
            "source_impulse_id": item.get("id"),
            "behavior_status": item.get("status"),
        },
    )


async def run():
    state = load_state()
    entries = analyzer_entries()
    rows = ledger_rows(0 if not state.get("initialized") else state.get("last_ledger_id", 0))
    queue = ImpulseQueue(str(QUEUE))

    if not state.get("initialized"):
        state.update(
            initialized=True,
            processed_memory_ids=[str(e.get("id")) for e in entries if e.get("id")],
            last_ledger_id=max([int(r["id"]) for r in rows], default=0),
        )
        save_state(state)
        print(f"initialized: memories={len(entries)} ledger={state['last_ledger_id']}")
        return

    seen = set(state.get("processed_memory_ids") or [])
    fresh = [e for e in reversed(entries) if str(e.get("id") or "") not in seen]
    memory_applied = 0
    for entry in fresh[:MAX_MEMORY_PER_RUN]:
        result = request_json("/api/analyzer/dp-memory", {"entry": entry, "post_feed": True}, timeout=30)
        if result.get("ok"):
            seen.add(str(entry["id"]))
            memory_applied += 1

    emotion_memories = 0
    for row in rows:
        if qualifies(row):
            await create_emotion_memory(row)
            emotion_memories += 1
        state["last_ledger_id"] = max(int(state.get("last_ledger_id") or 0), int(row["id"]))

    behavior_memories = 0
    for item in queue.snapshot():
        if item.get("status") != "done" or item.get("result_memory_id"):
            continue
        bucket_id = await create_behavior_memory(item)
        queue.attach_memory(item["id"], bucket_id)
        behavior_memories += 1

    state["processed_memory_ids"] = list(seen)[-2000:]
    save_state(state)
    queue.prune()
    print(f"memory->emotion={memory_applied} emotion->memory={emotion_memories} behavior->memory={behavior_memories}")


if __name__ == "__main__":
    asyncio.run(run())

