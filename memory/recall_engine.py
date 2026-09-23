"""Shared, read-only keyword + local-vector retrieval for chat entry points."""
import asyncio
import re
import time
import logging
from rapidfuzz import fuzz
import jieba

# Warm during service startup, not inside the gateway's 1.5-second request.
jieba.initialize()


class RecallHistory:
    def __init__(self, ttl=1800, max_sessions=128):
        self.ttl = ttl
        self.max_sessions = max_sessions
        self.sessions = {}

    def _prune(self):
        now = time.monotonic()
        for key, items in list(self.sessions.items()):
            live = {bid: at for bid, at in items.items() if now - at < self.ttl}
            if live:
                self.sessions[key] = live
            else:
                self.sessions.pop(key, None)

    def seen(self, session, bid):
        self._prune()
        return session is not None and bid in self.sessions.get(session, {})

    def mark(self, session, bid):
        if session is None:
            return
        self._prune()
        if session not in self.sessions and len(self.sessions) >= self.max_sessions:
            oldest = min(self.sessions, key=lambda k: max(self.sessions[k].values()))
            self.sessions.pop(oldest, None)
        self.sessions.setdefault(session, {})[bid] = time.monotonic()


async def recall_candidates(manager, message, limit=8, threshold=55, vector_timeout=0.95):
    text = (message or '').strip()[:2000]
    if not text:
        return []
    buckets = await manager.list_all(include_archive=False)
    terms = list(dict.fromkeys(
        [w.strip() for w in jieba.cut(text) if len(w.strip()) >= 2]
        + [t.strip() for t in re.split(r'[\s,，、。！？!?.]+', text) if len(t.strip()) >= 2]
    ))
    similarities = {}
    engine = getattr(manager, 'embedding_engine', None)
    if engine and engine.enabled:
        try:
            pairs = await asyncio.wait_for(engine.search_similar(text, top_k=50), vector_timeout)
            similarities = dict(pairs)
        except Exception:
            # A slow/unavailable embedding service must not suppress lexical hits.
            pass
    results = []
    for bucket in buckets:
        meta = bucket.get('metadata', {})
        if meta.get('digested'):
            continue
        haystack = '\n'.join([str(meta.get('name', '')),
            ' '.join(str(x) for x in meta.get('domain', [])),
            ' '.join(str(x) for x in meta.get('tags', [])),
            bucket.get('content', '')]).lower()
        lexical = max((fuzz.partial_ratio(t.lower(), haystack) for t in terms), default=0)
        semantic = max(0.0, min(1.0, similarities.get(bucket['id'], 0))) * 100
        relevance = max(lexical, semantic)
        # Qualify on relevance BEFORE applying resolved ranking penalty.
        if relevance < threshold:
            continue
        score = relevance * (0.6 if meta.get('resolved') else 1.0)
        results.append({**bucket, 'score': score, 'semantic_score': semantic})
    results.sort(key=lambda b: (-b['score'], b['id']))
    logging.getLogger('ombre_brain.recall').info(
        'Hybrid recall: vector_candidates=%d qualified=%d', len(similarities), len(results))
    return results[:limit] if limit is not None else results
