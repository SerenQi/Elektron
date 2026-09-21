#!/usr/bin/env python3
"""Small, crash-safe state machine for emotion-driven impulses."""
from __future__ import annotations

import fcntl
import json
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


ACTIVE = {"pending", "claimed"}
FINAL = {"done", "ignored"}


class ImpulseQueue:
    def __init__(self, path: str):
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")

    @contextmanager
    def _locked(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            items = self._read()
            self._recover(items)
            yield items
            self._write(items)
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _read(self):
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _write(self, items):
        fd, tmp = tempfile.mkstemp(prefix=self.path.name + ".", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as out:
                json.dump(items, out, ensure_ascii=False, indent=1)
                out.write("\n")
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    @staticmethod
    def _normalize(item):
        item.setdefault("id", "imp-" + uuid.uuid4().hex[:12])
        if "status" not in item:
            item["status"] = "done" if item.get("done") else "pending"
        item["done"] = item["status"] in FINAL
        item.setdefault("attempts", 0)
        item.setdefault("note", "")
        item.setdefault("claimed_at", None)
        item.setdefault("lease_until", None)
        item.setdefault("retry_at", None)
        item.setdefault("completed_at", None)
        item.setdefault("outcome_at", item.get("completed_at"))
        item.setdefault("result_memory_id", None)
        return item

    def _recover(self, items, now=None):
        now = now or time.time()
        for item in items:
            self._normalize(item)
            if item["status"] == "claimed" and float(item.get("lease_until") or 0) <= now:
                item.update(status="pending", claimed_at=None, lease_until=None,
                            note="领取租约过期，自动退回队列")
            if item["status"] == "deferred" and float(item.get("retry_at") or 0) <= now:
                item.update(status="pending", retry_at=None)

    def snapshot(self):
        with self._locked() as items:
            return [dict(item) for item in items]

    def enqueue(self, payload):
        with self._locked() as items:
            item = self._normalize(dict(payload))
            items.append(item)
            return dict(item)

    def has_active_kind(self, kind):
        with self._locked() as items:
            return any(i.get("kind") == kind and i.get("status") in ACTIVE for i in items)

    def recent_outcome(self, kind):
        with self._locked() as items:
            matches = [i for i in items if i.get("kind") == kind and i.get("status") not in ACTIVE]
            return dict(matches[-1]) if matches else None

    def claim(self, lease_seconds=600):
        now = time.time()
        with self._locked() as items:
            for item in items:
                if item.get("status") != "pending":
                    continue
                item.update(status="claimed", claimed_at=now,
                            lease_until=now + lease_seconds,
                            attempts=int(item.get("attempts") or 0) + 1,
                            done=False)
                return dict(item)
        return None

    def finish(self, impulse_id, status, note="", retry_seconds=3600):
        if status not in {"done", "deferred", "failed", "ignored"}:
            raise ValueError("invalid impulse status")
        now = time.time()
        outcome_at = datetime.now().astimezone().isoformat(timespec="seconds")
        with self._locked() as items:
            for item in items:
                if item.get("id") != impulse_id:
                    continue
                item.update(status=status, note=str(note or "")[:800],
                            claimed_at=None, lease_until=None,
                            done=status in FINAL, outcome_at=outcome_at)
                if status in FINAL:
                    item["completed_at"] = outcome_at
                    item["retry_at"] = None
                elif status == "deferred":
                    item["retry_at"] = now + max(300, retry_seconds)
                else:
                    item["retry_at"] = None
                return dict(item)
        return None

    def attach_memory(self, impulse_id, bucket_id):
        with self._locked() as items:
            for item in items:
                if item.get("id") == impulse_id:
                    item["result_memory_id"] = bucket_id
                    return True
        return False

    def prune(self, keep_seconds=7 * 86400):
        cutoff = time.time() - keep_seconds
        with self._locked() as items:
            kept = []
            for item in items:
                stamp = item.get("completed_at")
                try:
                    ts = datetime.fromisoformat(stamp).timestamp() if stamp else time.time()
                except ValueError:
                    ts = time.time()
                if item.get("status") in FINAL and ts < cutoff and item.get("result_memory_id"):
                    continue
                kept.append(item)
            items[:] = kept
