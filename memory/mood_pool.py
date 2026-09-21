from __future__ import annotations

import json
import os
import hashlib
import time

from identity import AGENT_NAME, AGENT_PERSONA

_LIVE_WIRE_CACHE_PATH = os.environ.get(
    "LIVE_WIRE_CACHE", "/app/buckets/live_wire_cache.json"
)
_LIVE_WIRE_TTL = int(os.environ.get("LIVE_WIRE_TTL_SECONDS", str(30 * 60)))
_LIVE_WIRE_SCHEMA = "mood_synthesis_v3"
FALLBACK_MOOD_TRACE = ""
FALLBACK_LIVE_WIRE = ""


def _load_live_wire_cache(thought_signature: str = "") -> dict | None:
    try:
        if not os.path.exists(_LIVE_WIRE_CACHE_PATH):
            return None
        with open(_LIVE_WIRE_CACHE_PATH) as f:
            cache = json.load(f)
        if cache.get("schema") != _LIVE_WIRE_SCHEMA:
            return None
        if cache.get("source") != "thought_synthesis":
            return None
        if thought_signature and cache.get("thought_signature") != thought_signature:
            return None
        if time.time() - cache.get("generated_at", 0) > _LIVE_WIRE_TTL:
            return None
        return cache
    except Exception:
        return None


def _save_live_wire_cache(mood_trace: str, live_wire: str, thought_count: int, thought_signature: str) -> None:
    try:
        os.makedirs(os.path.dirname(_LIVE_WIRE_CACHE_PATH), exist_ok=True)
        with open(_LIVE_WIRE_CACHE_PATH, "w") as f:
            json.dump({
                "schema": _LIVE_WIRE_SCHEMA,
                "source": "thought_synthesis",
                "mood_trace": mood_trace,
                "live_wire": live_wire,
                "thought_count": thought_count,
                "thought_signature": thought_signature,
                "generated_at": time.time(),
            }, f)
    except Exception:
        pass


def _source_lines(sources: list) -> list[str]:
    top = sorted(sources, key=lambda t: (
        t.get("born_at", 0) if isinstance(t, dict) else getattr(t, "born_at", 0),
        t.get("strength", 0) if isinstance(t, dict) else getattr(t, "strength", 0),
    ), reverse=True)[:5]
    lines = []
    for i, t in enumerate(top, 1):
        text = t.get("text", "") if isinstance(t, dict) else getattr(t, "text", "")
        drive = t.get("drive", "") if isinstance(t, dict) else getattr(t, "drive", "")
        source = t.get("source_type", "") if isinstance(t, dict) else getattr(t, "source_type", "")
        strength = t.get("strength", 0) if isinstance(t, dict) else getattr(t, "strength", 0)
        text = str(text or "").strip()
        if not text:
            continue
        try:
            strength_label = f"{float(strength):.2f}"
        except (TypeError, ValueError):
            strength_label = "0.00"
        source_label = f"/{source}" if source else ""
        lines.append(f"{i}. [{drive}{source_label} {strength_label}] {text}")
    return lines


def _thought_signature(thoughts: list) -> str:
    joined = "\n".join(_source_lines(thoughts))
    return hashlib.sha1(joined.encode("utf-8")).hexdigest() if joined else ""


def _synthesize_mood(thoughts: list) -> tuple[str, str] | None:
    """Call DeepSeek to synthesize an auxiliary trace from recent sources."""
    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not api_key:
        return None
    lines = _source_lines(thoughts)
    if not lines:
        return None
    try:
        import httpx
        prompt = (
            f"{AGENT_PERSONA}\n"
            f"你是 {AGENT_NAME}。"
            "以下是最近的有来源念头和记忆片段。只能综合这些来源，不要新增设定，不要编随机心情。\n"
            + "\n".join(lines) + "\n\n"
            "这是最近一小段时间沉下来的底色，不是这一刻刚冒出来的反应。只输出JSON，不要解释：\n"
            "{\"live_wire\": \"一个很短的观察词，不要当作Climate\", \"mood_trace\": \"一句话，这段时间心境的底色\"}\n"
        )
        resp = httpx.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": "deepseek-v4-flash",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 120,
                "temperature": 0.7,
            },
            timeout=10,
        )
        data = resp.json()
        raw = data["choices"][0]["message"]["content"].strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:])
            raw = raw.replace("```", "").strip()
        start = raw.index("{")
        end = raw.rindex("}") + 1
        result = json.loads(raw[start:end])
        live_wire = (result.get("live_wire") or "").strip()
        mood_trace = str(result.get("mood_trace") or "").strip()
        if mood_trace:
            return (mood_trace, live_wire)
    except Exception:
        pass
    return None


def get_daily_mood(branch: str = None, thoughts: list = None):
    """
    Synthesize a mood trace from recent sourced memory items only.

    `branch` is a retired compatibility parameter. When synthesis is unavailable,
    return a fixed neutral sentinel instead of a random dead mood dictionary.
    """
    current_count = len(thoughts) if thoughts else 0
    thought_signature = _thought_signature(thoughts or [])
    cache = _load_live_wire_cache(thought_signature)

    if cache:
        return (cache["mood_trace"], cache["live_wire"])

    if thoughts and len(thoughts) >= 2:
        synth = _synthesize_mood(thoughts)
        if synth:
            _save_live_wire_cache(synth[0], synth[1], current_count, thought_signature)
            return synth

    return (FALLBACK_MOOD_TRACE, FALLBACK_LIVE_WIRE)
