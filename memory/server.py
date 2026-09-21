from __future__ import annotations

# ============================================================
# Module: MCP Server Entry Point (server.py)
# 模块：MCP 服务器主入口
#
# Starts the Ombre Brain MCP service and registers memory
# operation tools for Claude to call.
# 启动 Ombre Brain MCP 服务，注册记忆操作工具供 Claude 调用。
#
# Core responsibilities:
# 核心职责：
#   - Initialize config, bucket manager, dehydrator, decay engine
#     初始化配置、记忆桶管理器、脱水器、衰减引擎
#   - Expose MCP tools:
#     暴露 MCP 工具：
#       breath — Surface unresolved memories or search by keyword
#                浮现未解决记忆 或 按关键词检索
#       hold   — Store memory/feel/writing/unresolved/window with optional signal hints
#                存储记忆/感受/写作/悬置/窗口，并可附轻量信号
#       wander / wander_mark — Browse drawers and mark old entries
#                抽屉漫游与旧条目标记
#       stir / settle / pass / break / undercurrent — Weather and drive controls
#                天气与 drive 控制
#
# Startup:
# 启动方式：
#   Local:  python server.py
#   Remote: OMBRE_TRANSPORT=streamable-http python server.py
#   Docker: docker-compose up
# ============================================================

import os
import sys
import random
import logging
import asyncio
import hashlib
import hmac
import secrets
import time
import json as _json_lib
import sqlite3
import re
import unicodedata
import tempfile
import threading
import fcntl
import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional
import httpx
import os as _os


# --- Ensure same-directory modules can be imported ---
# --- 确保同目录下的模块能被正确导入 ---
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mcp.server.fastmcp import FastMCP

from bucket_manager import BucketManager
from dehydrator import Dehydrator
from decay_engine import DecayEngine
from embedding_engine import EmbeddingEngine
from import_memory import ImportEngine
from latent_note_view import latent_notes_for_display
from identity import AGENT_NAME, AGENT_PERSONA, HUMAN_NAME
from utils import load_config, setup_logging, strip_wikilinks, count_tokens_approx, now_iso
from desire_engine import (
    CHORD_KEYS,
    DRIVE_BASELINES,
    DRIVE_KEYS,
    DRIVE_EVENT_SCHEMA,
    DesireEngine,
    normalize_drive_key,
    _legacy_brain_to_event,
    _normalize_chord,
)

# MCP tool enums — keep in sync with desire_engine.DRIVE_KEYS / CHORD_KEYS.
HoldKind = Literal["memory", "feel", "writing", "unresolved", "window"]
TrackKind = Literal["square_wave", "sine_wave", "both"]
DriveKeyName = Literal[
    "attachment",
    "libido",
    "possessiveness",
    "reflection",
    "stewardship",
    "curiosity",
    "social",
    "fatigue",
    "stress",
]
ChordName = Literal[
    "C6",
    "Am7",
    "Gsus4",
    "Dmaj7",
    "Amaj7",
    "Fmaj7",
    "Fmaj7#11",
    "Gmaj7",
    "Dm7",
    "Em7",
    "F#dim",
    "Bm7b5",
]
DriveActionName = Literal["stir", "settle", "break", "pass"]
from speech_event_engine import (
    append_pending_batch,
    apply_speech_event_review,
    batch_text,
    classify_speech_batch_dp,
    clear_pending_batch,
    is_recent_speech_event,
    load_speech_event_state,
    normalize_speech_event,
    save_speech_event_state,
    speech_event_drive_event,
    speech_event_classifier_available,
    append_ledger as append_speech_event_ledger,
)
from dialogue_residue_engine import (
    classify_dialogue_residue_dp,
    dialogue_residue_available,
    load_dialogue_residue_state,
    normalize_dialogue_messages,
    normalize_dialogue_residue_event,
    save_dialogue_residue_state,
    append_dialogue_residue_ledger,
)
from memory_residue_engine import (
    classify_memory_residue_dp,
    memory_residue_available,
    normalize_memory_entry,
)

# --- Load config & init logging / 加载配置 & 初始化日志 ---
config = load_config()
setup_logging(config.get("log_level", "INFO"))
logger = logging.getLogger("ombre_brain")

# --- Runtime env vars (port + webhook) / 运行时环境变量 ---
# OMBRE_PORT: HTTP/SSE 监听端口，默认 8000
try:
    OMBRE_PORT = int(os.environ.get("OMBRE_PORT", "8000") or "8000")
except ValueError:
    logger.warning("OMBRE_PORT 不是合法整数，回退到 8000")
    OMBRE_PORT = 8000

# OMBRE_HOOK_URL: 在 breath/dream 被调用后推送事件到该 URL（POST JSON）。
# OMBRE_HOOK_SKIP: 设为 true/1/yes 跳过推送。
# 详见 ENV_VARS.md。
OMBRE_HOOK_URL = os.environ.get("OMBRE_HOOK_URL", "").strip()
OMBRE_HOOK_SKIP = os.environ.get("OMBRE_HOOK_SKIP", "").strip().lower() in ("1", "true", "yes", "on")


async def _fire_webhook(event: str, payload: dict) -> None:
    """
    Fire-and-forget POST to OMBRE_HOOK_URL with the given event payload.
    Failures are logged at WARNING level only — never propagated to the caller.
    """
    if OMBRE_HOOK_SKIP or not OMBRE_HOOK_URL:
        return
    try:
        body = {
            "event": event,
            "timestamp": time.time(),
            "payload": payload,
        }
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(OMBRE_HOOK_URL, json=body)
    except Exception as e:
        logger.warning(f"Webhook push failed ({event} → {OMBRE_HOOK_URL}): {e}")

# --- Initialize core components / 初始化核心组件 ---
embedding_engine = EmbeddingEngine(config)            # Embedding engine first (BucketManager depends on it)
bucket_mgr = BucketManager(config, embedding_engine=embedding_engine)  # Bucket manager / 记忆桶管理器
dehydrator = Dehydrator(config)                      # Dehydrator / 脱水器
decay_engine = DecayEngine(config, bucket_mgr)       # Decay engine / 衰减引擎
import_engine = ImportEngine(config, bucket_mgr, dehydrator, embedding_engine)
BUCKETS_DIR = config["buckets_dir"]
MEMORY_ANALYZER_MODE = str(
    os.environ.get("OMBRE_MEMORY_ANALYZER")
    or "dp"
).strip().lower()
if MEMORY_ANALYZER_MODE not in {"dp", "cli"}:
    logger.warning("Invalid OMBRE_MEMORY_ANALYZER=%r; falling back to dp", MEMORY_ANALYZER_MODE)
    MEMORY_ANALYZER_MODE = "dp"
def _bucket_path(*parts: str) -> str:
    return os.path.join(BUCKETS_DIR, *parts)

_desire_db = os.path.join(
    BUCKETS_DIR,
    "desire.db"
)
_desire = DesireEngine(db_path=_desire_db)
try:
    # 一次性清掉历史 analyzer 代写念头（dp_memory 不再 mint）
    _purged = _desire.purge_thoughts_by_source("dp_memory")
    if _purged.get("removed"):
        logger.info(f"purged legacy dp_memory thoughts: {_purged['removed']}")
except Exception as _purge_exc:
    logger.warning(f"purge dp_memory thoughts failed: {_purge_exc}")
_last_signal_ts: list = [0.0]  # latest direct human-input signal

def _speech_event_context_snapshot() -> dict:
    """Small state sample for async classification; never blocks the hook path."""
    try:
        desire = _desire.state()
    except Exception:
        desire = {}
    # Same ruler as Pulse Weather: highest drive by pressure above own baseline.
    top_drive, top_pressure, top_raw = _undertow_snapshot(desire if isinstance(desire, dict) else {})
    weather = desire.get("effective_pa_na") or {}
    current = load_speech_event_state(config["buckets_dir"])
    return {
        "undertow": top_drive,
        "undertow_value": round(top_pressure, 3),
        "undertow_raw_value": round(top_raw, 3),
        "warmth": weather.get("effective_PA"),
        "shadow": weather.get("effective_NA"),
        "current_chord": weather.get("current_chord"),
        "last_speech_label": current.get("label"),
        "last_speech_review": (current.get("review") or {}).get("mark"),
    }


def _dialogue_residue_context_snapshot() -> dict:
    """Current state for 2+2 dialogue analysis; lightweight and read-only."""
    context = _speech_event_context_snapshot()
    try:
        weather = (_desire.state().get("effective_pa_na") or {})
    except Exception:
        weather = {}
    chemistry = weather.get("chord_chemistry") if isinstance(weather, dict) else {}
    if isinstance(chemistry, dict):
        context["chemistry_core"] = chemistry.get("core") or weather.get("chemistry_core") or {}
        context["chemistry_route"] = chemistry.get("route") or weather.get("chemistry_route") or {}
        context["chord_situation"] = chemistry.get("situation") or weather.get("chord_situation") or ""
    return context


async def _related_memory_context(query: str, limit: int = 3) -> list[dict]:
    """Small evidence-only memory sample for classifiers, never the whole archive."""
    query = str(query or "").strip()[:600]
    if not query:
        return []
    try:
        matches = await asyncio.wait_for(bucket_mgr.search(query, limit=limit), timeout=1.5)
    except Exception as exc:
        logger.debug(f"related memory lookup skipped: {exc}")
        return []
    result = []
    budget = 1200
    for bucket in matches[:limit]:
        meta = bucket.get("metadata") if isinstance(bucket.get("metadata"), dict) else {}
        excerpt = strip_wikilinks(str(bucket.get("content") or "")).strip().replace("\n", " ")[:360]
        if not excerpt or budget <= 0:
            continue
        excerpt = excerpt[:budget]
        budget -= len(excerpt)
        result.append({
            "bucket_id": bucket.get("id"),
            "created": meta.get("created"),
            "type": meta.get("type"),
            "summary": excerpt,
            "usage": "background_evidence_not_current_event",
        })
    return result


def _weather_chord_display(weather: dict) -> str:
    current = str((weather or {}).get("current_chord") or "").strip()
    active = str((weather or {}).get("active_chord") or "").strip()
    if active and current and active != current:
        return f"{active} → {current}"
    return current or active


def _short_state_text(value: object, limit: int = 160) -> str:
    text = str(value or "").strip().replace("\n", " ")
    return text[:limit]


def _num(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _sorted_thoughts(state: dict) -> list:
    thoughts = state.get("thoughts") if isinstance(state.get("thoughts"), list) else []
    return sorted(
        [t for t in thoughts if isinstance(t, dict)],
        key=lambda t: _num(t.get("born_at"), 0.0),
        reverse=True,
    )


def _latest_thought_text(state: dict) -> str:
    for thought in _sorted_thoughts(state):
        text = str(thought.get("text") or "").strip()
        if text:
            return text
    return ""


MOOD_TRACE_FRESH_SECONDS = 45 * 60


def _undertow_snapshot(state: dict) -> tuple[str, float, float]:
    """Return active pressure above each drive's own baseline.

    Raw drive magnitudes are not comparable because their baselines differ.
    The third value keeps the raw magnitude available for diagnostics.
    """
    drives = state.get("drives") if isinstance(state.get("drives"), dict) else {}
    if not drives:
        return "", 0.0, 0.0
    candidates = {
        key: _num(value) - _num(DRIVE_BASELINES.get(key))
        for key, value in drives.items()
        if key in DRIVE_BASELINES and key != "fatigue"
    }
    drive = max(candidates, key=candidates.get, default="")
    if not drive:
        return "", 0.0, 0.0
    return drive, round(candidates[drive], 3), round(_num(drives.get(drive)), 3)


def _fresh_mood_trace(state: dict, now: float | None = None) -> tuple[str, float]:
    now = time.time() if now is None else now
    for thought in _sorted_thoughts(state):
        text = str(thought.get("text") or "").strip()
        born_at = _num(thought.get("born_at"), 0.0)
        if text and born_at > 0 and -60 <= now - born_at <= MOOD_TRACE_FRESH_SECONDS:
            return text, born_at
    return "", 0.0


def _now_playing_text(state: dict) -> str:
    source = state.get("now_playing") if isinstance(state.get("now_playing"), dict) else {}
    if not source:
        weather = state.get("pulse_weather") if isinstance(state.get("pulse_weather"), dict) else {}
        source = weather.get("now_playing") if isinstance(weather.get("now_playing"), dict) else {}
    title = str(source.get("title") or source.get("name") or "").strip()
    artist = str(source.get("artist") or "").strip()
    if not title:
        return ""
    return f"{title} - {artist}" if artist else title


_NOW_PLAYING_CACHE = {"ts": 0.0, "value": {}}


def _spotify_client_credentials() -> tuple[str, str]:
    """Spotify app credentials from process env only.

    Public edition does not scrape editor config files for secrets.
    """
    client_id = os.environ.get("SPOTIFY_CLIENT_ID", "").strip()
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "").strip()
    if client_id and client_secret:
        return client_id, client_secret
    return "", ""


def _spotify_access_token(force_refresh: bool = False) -> str:
    """Optional Spotify now-playing. Opt-in via env credentials + local token file.

    Requires SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET and a token cache at
    ~/.spotify-mcp/tokens.json (same layout as common Spotify MCP helpers).
    Silent no-op when either side is missing.
    """
    import urllib.parse
    import urllib.request

    client_id, client_secret = _spotify_client_credentials()
    if not (client_id and client_secret):
        return ""

    token_path = os.path.expanduser("~/.spotify-mcp/tokens.json")
    if not os.path.exists(token_path):
        return ""
    try:
        with open(token_path) as f:
            token_data = _json_lib.load(f)
    except Exception:
        return ""
    if not isinstance(token_data, dict):
        return ""
    token = str(token_data.get("accessToken") or "").strip()
    expires_at = float(token_data.get("expiresAt", 0) or 0) / 1000.0
    if token and expires_at > time.time() + 60 and not force_refresh:
        return token

    refresh_token = str(token_data.get("refreshToken") or "").strip()
    if not refresh_token:
        return token

    data = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }).encode()
    req = urllib.request.Request("https://accounts.spotify.com/api/token", data=data, method="POST")
    with urllib.request.urlopen(req, timeout=8) as resp:
        refreshed = _json_lib.loads(resp.read() or b"{}")
    token_data["accessToken"] = refreshed["access_token"]
    token_data["expiresAt"] = int((time.time() + float(refreshed.get("expires_in", 3600))) * 1000)
    if refreshed.get("refresh_token"):
        token_data["refreshToken"] = refreshed["refresh_token"]
    with open(token_path, "w") as f:
        _json_lib.dump(token_data, f, indent=2)
    return str(token_data["accessToken"])


def _current_now_playing(max_age_sec: float = 12.0) -> dict:
    now = time.time()
    if now - float(_NOW_PLAYING_CACHE.get("ts", 0.0) or 0.0) < max_age_sec:
        return dict(_NOW_PLAYING_CACHE.get("value") or {})
    value: dict = {}
    try:
        import urllib.error
        import urllib.request
        token = _spotify_access_token()
        if token:
            req = urllib.request.Request(
                "https://api.spotify.com/v1/me/player/currently-playing",
                headers={"Authorization": f"Bearer {token}"},
            )
            try:
                resp = urllib.request.urlopen(req, timeout=4)
            except urllib.error.HTTPError as e:
                if e.code != 401:
                    raise
                token = _spotify_access_token(force_refresh=True)
                req = urllib.request.Request(
                    "https://api.spotify.com/v1/me/player/currently-playing",
                    headers={"Authorization": f"Bearer {token}"},
                )
                resp = urllib.request.urlopen(req, timeout=4)
            with resp:
                if resp.status != 204:
                    payload = _json_lib.loads(resp.read() or b"{}")
                    item = payload.get("item") if isinstance(payload.get("item"), dict) else {}
                    artists = item.get("artists") if isinstance(item.get("artists"), list) else []
                    if payload.get("is_playing") and item.get("name"):
                        value = {
                            "title": str(item.get("name") or "").strip(),
                            "artist": ", ".join(
                                str(a.get("name") or "").strip()
                                for a in artists
                                if isinstance(a, dict) and a.get("name")
                            ),
                            "state": "PLAYING",
                            "source": "spotify",
                        }
    except Exception:
        value = {}
    if value:
        _NOW_PLAYING_CACHE.update({"ts": now, "value": value})
        return dict(value)
    try:
        import subprocess
        script = os.environ.get("OMBRE_NOW_PLAYING_COMMAND", "").strip()
        if not script:
            return {}
        proc = subprocess.run(
            [script, "now"],
            text=True,
            capture_output=True,
            timeout=6,
            env={**os.environ, "NO_PROXY": "*", "no_proxy": "*"},
        )
        if proc.returncode == 0:
            state = ""
            title = ""
            artist = ""
            for line in proc.stdout.splitlines():
                if line.startswith("State:"):
                    state = line.split(":", 1)[1].strip()
                elif line.startswith("Track:"):
                    raw = line.split(":", 1)[1].strip()
                    if " - " in raw:
                        artist, title = [part.strip() for part in raw.split(" - ", 1)]
                    else:
                        title = raw
            if state == "PLAYING" and title:
                value = {"title": title, "artist": artist, "state": state}
    except Exception:
        value = {}
    _NOW_PLAYING_CACHE.update({"ts": now, "value": value})
    return dict(value)


def _weather_panel_from_state(state: dict, soma: dict | None = None) -> dict:
    """First-layer Pulse Weather readout for agent/breath; internals stay in Undercurrent."""
    state = state if isinstance(state, dict) else {}
    weather = state.get("pulse_weather") if isinstance(state.get("pulse_weather"), dict) else {}
    effective = state.get("effective_pa_na") if isinstance(state.get("effective_pa_na"), dict) else {}
    drives = state.get("drives") if isinstance(state.get("drives"), dict) else {}
    intent = state.get("intent") if isinstance(state.get("intent"), dict) else {}

    calculated_undertow, _, calculated_raw = _undertow_snapshot(state)
    undertow = str(weather.get("undertow") or calculated_undertow).strip()
    activations = (
        state.get("effective_activations")
        or state.get("drive_activations")
        or {}
    )
    undertow_value = _num(
        activations.get(undertow) if isinstance(activations, dict) else None,
        _num(weather.get("undertow_raw_value"), calculated_raw),
    )
    warmth = _num(weather.get("warmth"), _num(effective.get("effective_PA"), _num(state.get("pa_na", {}).get("PA") if isinstance(state.get("pa_na"), dict) else None, 0.5)))
    shadow = abs(_num(weather.get("shadow"), _num(effective.get("effective_NA"), _num(state.get("pa_na", {}).get("NA") if isinstance(state.get("pa_na"), dict) else None, 0.2))))
    chord = str(weather.get("chord_display") or _weather_chord_display(weather or effective)).strip()
    fresh_trace, trace_born_at = _fresh_mood_trace(state)
    mood_trace = _short_state_text(fresh_trace, 220)

    soma = soma if isinstance(soma, dict) else None
    if soma is None:
        soma = _fresh_soma_state()
    soma_line = str((soma or {}).get("line") or "").strip()
    soma_chord = str((soma or {}).get("chord") or "").strip()
    soma_trace = f"{soma_chord} · {soma_line}" if soma_chord and soma_line else soma_line

    panel = {
        "warmth": round(warmth, 3),
        "shadow": round(shadow, 3),
        "undertow": undertow,
        "undertow_value": round(undertow_value, 3),
        "chord": chord,
        "mood_trace": mood_trace,
        "mood_trace_born_at": trace_born_at,
    }
    if soma_trace:
        panel["soma_trace"] = _short_state_text(soma_trace, 180)
    now_playing = _now_playing_text(state)
    if now_playing:
        panel["now_playing"] = _short_state_text(now_playing, 120)
    return panel


def _weather_panel_lines(panel: dict) -> list[str]:
    lines = []
    undertow = str(panel.get("undertow") or "").strip()
    if undertow:
        tail = f" · {panel['chord']}" if panel.get("chord") else ""
        lines.append(f"Undertow：{undertow} {_num(panel.get('undertow_value')):.2f}{tail}")
    if panel.get("mood_trace"):
        lines.append(f"Mood Trace：{panel['mood_trace']}")
    if panel.get("soma_trace"):
        lines.append(f"Soma Trace：{panel['soma_trace']}")
    if panel.get("now_playing"):
        lines.append(f"♪ On Air：{panel['now_playing']}")
    return lines


def _undercurrent_state(state: dict) -> dict:
    state = state if isinstance(state, dict) else {}
    weather = state.get("pulse_weather") if isinstance(state.get("pulse_weather"), dict) else {}
    effective = state.get("effective_pa_na") if isinstance(state.get("effective_pa_na"), dict) else {}
    source_weather = weather or effective
    chemistry = source_weather.get("chord_chemistry") if isinstance(source_weather.get("chord_chemistry"), dict) else {}
    core = source_weather.get("chemistry_core") or chemistry.get("core") or {}
    route = source_weather.get("chemistry_route") or chemistry.get("route") or {}
    thoughts = _sorted_thoughts(state)
    drives = state.get("drives") if isinstance(state.get("drives"), dict) else {}
    drive_order = [k for k in DRIVE_KEYS if k in drives] + [k for k in drives if k not in DRIVE_KEYS]
    undertow_drive, undertow_pressure, undertow_raw = _undertow_snapshot(state)
    if not undertow_drive:
        undertow_drive = str(weather.get("undertow") or "").strip()
        undertow_pressure = _num(weather.get("undertow_value"))
        undertow_raw = _num(weather.get("undertow_raw_value"), _num(drives.get(undertow_drive)))
    out = {
        "Drive": {k: round(_num(drives.get(k)), 3) for k in drive_order},
        "Activation": {
            k: round(_num((state.get("effective_activations") or {}).get(k)), 3)
            for k in drive_order
        },
        # Undertow = highest drive by pressure (raw − baseline). Activation uses sqrt scale.
        "Undertow": {
            "drive": undertow_drive,
            "pressure": round(undertow_pressure, 3),
            "raw": round(undertow_raw, 3),
            "scale": "pressure=raw−baseline; Activation uses sqrt headroom",
        },
        "Affect": {
            "Warmth": round(_num(source_weather.get("warmth"), _num(effective.get("effective_PA"), 0.0)), 3),
            "Shadow": round(abs(_num(source_weather.get("shadow"), _num(effective.get("effective_NA"), 0.0))), 3),
            "Longing": round(_num(source_weather.get("longing"), _num(state.get("longing"), 0.0)), 3),
        },
        "Chemistry": {
            "Charge": round(_num(core.get("charge")), 3),
            "Clutch": round(_num(core.get("clutch")), 3),
            "Strain": round(_num(core.get("strain")), 3),
            "Vector": route.get("vector") or "hover",
        },
        "Thought Pool": [
            {
                "index": i + 2,
                "drive": t.get("drive"),
                "kind": t.get("kind"),
                "strength": t.get("strength"),
                "text": str(t.get("text") or "").strip().replace("\n", " "),
            }
            for i, t in enumerate(thoughts[1:8])
            if str(t.get("text") or "").strip()
        ],
    }
    return out


def _compact_desire_state(state: dict) -> dict:
    """MCP readout for Claude: dashboard state is too large for tool context."""
    state = state if isinstance(state, dict) else {}
    weather = state.get("pulse_weather") if isinstance(state.get("pulse_weather"), dict) else {}
    effective = state.get("effective_pa_na") if isinstance(state.get("effective_pa_na"), dict) else {}
    intent = state.get("intent") if isinstance(state.get("intent"), dict) else None
    thoughts = state.get("thoughts") if isinstance(state.get("thoughts"), list) else []
    drive_events = state.get("drive_events") if isinstance(state.get("drive_events"), list) else []
    speech_event = state.get("speech_event") if isinstance(state.get("speech_event"), dict) else {}
    dialogue = state.get("dialogue_residue") if isinstance(state.get("dialogue_residue"), dict) else {}

    def _compact_thought(t: dict) -> dict:
        return {
            "tid": t.get("tid"),
            "drive": t.get("drive"),
            "kind": t.get("kind"),
            "strength": t.get("strength"),
            "source": t.get("source"),
            "text": str(t.get("text") or "").strip().replace("\n", " "),
            "born_at": t.get("born_at"),
        }

    def _compact_event(e: dict) -> dict:
        brain = e.get("brain") if isinstance(e.get("brain"), dict) else {}
        return {
            "id": e.get("id"),
            "ts": e.get("ts"),
            "source": e.get("source") or brain.get("source"),
            "primary_drive": e.get("primary_drive"),
            "event_label": e.get("event_label"),
            "intensity": e.get("intensity"),
            "confidence": e.get("confidence"),
            "agency": e.get("agency"),
            "applied": e.get("applied"),
            "suppressed": e.get("suppressed", False),
            "reason": e.get("reason", ""),
            "brain": {
                k: brain.get(k)
                for k in (
                    "target",
                    "time_mode",
                    "grounding",
                    "anchor_target",
                    "release_pressure",
                    "closeness_pull",
                    "inward_pull",
                    "novelty_pull",
                    "expression_pressure",
                    "tension_load",
                    "discernment_alarm",
                )
                if brain.get(k) not in (None, "", [], {})
            },
            "evidence": [_short_state_text(x, 120) for x in (e.get("evidence") or [])[:2]],
        }

    compact_intent = None
    if intent:
        compact_intent = {
            "drive_key": intent.get("drive_key"),
            "want_action": intent.get("want_action"),
            "score": intent.get("score"),
            "thought": _short_state_text(intent.get("thought_text") or intent.get("thought"), 160),
        }

    compact_thoughts = [_compact_thought(t) for t in thoughts[:8] if isinstance(t, dict)]
    compact_events = [_compact_event(e) for e in drive_events[:5] if isinstance(e, dict)]
    return {
        "drives": state.get("drives", {}),
        "effective_drives": state.get("effective_drives", {}),
        "drive_activations": state.get("drive_activations", {}),
        "effective_activations": state.get("effective_activations", {}),
        "local_fatigue": state.get("local_fatigue", {}),
        "drive_outputs": state.get("drive_outputs", {}),
        "discernment": state.get("discernment", {}),
        "intent": compact_intent,
        # longing 只给网页前端观察，不进 weather_panel / 注入文案
        "longing": round(_num(state.get("longing"), _num(weather.get("longing"))), 3),
        "longing_phase": state.get("longing_phase") or weather.get("longing_phase") or "",
        "hours_awake_absent": round(_num(state.get("hours_awake_absent"), _num(weather.get("hours_awake_absent"))), 3),
        "hours_since_last_message": round(
            _num(state.get("hours_since_last_message"), _num(weather.get("hours_since_last_message"))), 3
        ),
        "attachment_gain_scale": round(
            _num(state.get("attachment_gain_scale"), _num(weather.get("attachment_gain_scale"), 1.0)), 3
        ),
        "weather_panel": _weather_panel_from_state(state),
        "pulse_weather": {
            "undertow": weather.get("undertow"),
            "undertow_value": weather.get("undertow_value"),
            "undertow_raw_value": weather.get("undertow_raw_value"),
            "warmth": weather.get("warmth"),
            "shadow": weather.get("shadow"),
            "warmth_residue": weather.get("warmth_residue"),
            "shadow_residue": weather.get("shadow_residue"),
            "component_shadow_residue": weather.get("component_shadow_residue"),
            "crystal_shadow": weather.get("crystal_shadow"),
            "shadow_crystal": weather.get("shadow_crystal"),
            "base_warmth": weather.get("base_warmth"),
            "base_shadow": weather.get("base_shadow"),
            "mood_trace": _short_state_text(weather.get("mood_trace"), 160),
            "mood_trace_born_at": weather.get("mood_trace_born_at"),
            "current_chord": weather.get("current_chord"),
            "chord_display": weather.get("chord_display") or _weather_chord_display(effective),
            "chemistry_core": weather.get("chemistry_core") or (weather.get("chord_chemistry") or {}).get("core"),
            "chemistry_route": weather.get("chemistry_route") or (weather.get("chord_chemistry") or {}).get("route"),
            "chord_situation": weather.get("chord_situation", ""),
            "derived_texture": weather.get("derived_texture", {}),
            "longing": round(_num(state.get("longing"), _num(weather.get("longing"))), 3),
            "longing_phase": state.get("longing_phase") or weather.get("longing_phase") or "",
            "hours_awake_absent": round(
                _num(state.get("hours_awake_absent"), _num(weather.get("hours_awake_absent"))), 3
            ),
            "attachment_gain_scale": round(
                _num(state.get("attachment_gain_scale"), _num(weather.get("attachment_gain_scale"), 1.0)), 3
            ),
        },
        "weather_residue": {
            "warmth": weather.get("warmth_residue"),
            "shadow": weather.get("shadow_residue"),
            "component_shadow": weather.get("component_shadow_residue"),
            "crystal_shadow": weather.get("crystal_shadow"),
            "shadow_crystal": weather.get("shadow_crystal"),
            "base_warmth": weather.get("base_warmth"),
            "base_shadow": weather.get("base_shadow"),
        },
        "speech_event": {
            "label": speech_event.get("label"),
            "confidence": speech_event.get("confidence"),
            "intensity": speech_event.get("intensity"),
            "trace": _short_state_text(speech_event.get("trace_line"), 140),
            "recent": speech_event.get("recent"),
        } if speech_event else {},
        "dialogue_residue": {
            "status": dialogue.get("status"),
            "primary_drive": dialogue.get("primary_drive"),
            "intensity": dialogue.get("intensity"),
            "confidence": dialogue.get("confidence"),
            "event_label": dialogue.get("event_label"),
        } if dialogue else {},
        "thoughts": compact_thoughts,
        "recent_thoughts": compact_thoughts,
        "drive_events": compact_events,
        "recent_drive_events": compact_events,
        "recent_refusals": state.get("recent_refusals", []),
        "counts": {
            "thoughts": len(thoughts),
            "drive_events_in_state": len(drive_events),
        },
    }


async def _refine_speech_batch_background(items: list[dict]) -> None:
    """Analyze a small batch of the human messages; this is the route that affects Drive/PA/NA."""
    try:
        fallback = normalize_speech_event(None, batch_text(items))
        context = _speech_event_context_snapshot()
        context["related_memories"] = await _related_memory_context(batch_text(items))
        refined = await classify_speech_batch_dp(
            items,
            state_context=context,
            fallback_event=fallback,
        )
        saved = save_speech_event_state(config["buckets_dir"], refined, ledger_stage="dp_batch_refined")
        _apply_speech_event_weather(saved)
        _apply_speech_event_drive(saved)
        append_speech_event_ledger(
            config["buckets_dir"], {"stage": "batch_applied", "batch_size": len(items), "event": saved}
        )
    except Exception as e:
        append_speech_event_ledger(
            config["buckets_dir"], {"stage": "batch_failed", "error": str(e)[:180], "batch_size": len(items)}
        )
        logger.warning(f"speech_event batch refine failed: {e}")


def _dialogue_residue_should_apply(event: dict) -> bool:
    if event.get("confidence", 0) < 0.45:
        return False
    brain = event.get("brain") if isinstance(event.get("brain"), dict) else {}
    has_primary = bool(event.get("primary_drive")) and event.get("intensity", 0) > 0

    def _positive_brain_value(key: str) -> bool:
        try:
            return float(brain.get(key, 0.0) or 0.0) > 0
        except (TypeError, ValueError):
            return False

    has_discernment = any(
        _positive_brain_value(key)
        for key in ("discernment_alarm", "self_softening", "output_drift", "template_intimacy")
    ) or bool(brain.get("discernment_flags") or event.get("discernment_flags"))
    return has_primary or has_discernment


async def _refine_dialogue_residue_background(messages: list[dict], window_id: str) -> None:
    """Analyze a 2+2 dialogue window after Stop; skipped windows are handled before scheduling."""
    try:
        query = "\n".join(
            str(message.get("content") or message.get("text") or "")
            for message in messages if isinstance(message, dict)
        )
        context = _dialogue_residue_context_snapshot()
        context["related_memories"] = await _related_memory_context(query)
        event = await classify_dialogue_residue_dp(
            messages,
            state_context=context,
            window_id=window_id,
        )
        saved = save_dialogue_residue_state(config["buckets_dir"], event, ledger_stage="dp_refined")
        result = {}
        if _dialogue_residue_should_apply(saved):
            result = _desire.apply_drive_event(saved)
            try:
                _last_signal_ts[0] = time.time()
                _desire.mark_user_signal(_last_signal_ts[0])
            except Exception as e:
                logger.warning(f"dialogue_residue mark_user_signal failed: {e}")
        append_dialogue_residue_ledger(
            config["buckets_dir"],
            {"stage": "applied", "window_id": window_id, "event": saved, "result": result},
        )
    except Exception as e:
        append_dialogue_residue_ledger(
            config["buckets_dir"], {"stage": "failed", "window_id": window_id, "error": str(e)[:180]}
        )
        logger.warning(f"dialogue_residue refine failed: {e}")


def _apply_speech_event_drive(event: dict | None) -> dict:
    payload = speech_event_drive_event(event)
    if not payload:
        return {}
    try:
        result = _desire.apply_drive_event(payload)
    except Exception as e:
        logger.warning(f"speech_event drive apply failed: {e}")
        return {"ok": False, "error": str(e)}
    try:
        import json as _json, os as _os
        mood_path = _bucket_path("current_mood.json")
        mood_data = {}
        if _os.path.exists(mood_path):
            with open(mood_path) as f:
                mood_data = _json.load(f)
        mood_data["drive_event"] = {
            "schema_version": DRIVE_EVENT_SCHEMA,
            "primary_drive": payload.get("primary_drive", ""),
            "event_label": payload.get("event_label", ""),
            "brain": payload.get("brain", {}),
            "evidence": payload.get("evidence", []),
            "result": result,
        }
        with open(mood_path, "w") as f:
            _json.dump(mood_data, f)
    except Exception as e:
        logger.warning(f"speech_event drive mood write failed: {e}")
    return result


def _apply_speech_event_weather(event: dict | None) -> dict:
    if not isinstance(event, dict):
        return {}
    try:
        warmth = max(0.0, float(event.get("warmth_delta", 0.0) or 0.0))
        shadow = max(0.0, float(event.get("shadow_delta", 0.0) or 0.0))
    except (TypeError, ValueError):
        warmth, shadow = 0.0, 0.0
    soothe = bool(event.get("soothe", False))
    if warmth <= 0 and shadow <= 0 and not soothe:
        return {}
    try:
        return _desire.apply_weather_delta(
            warmth_delta=warmth,
            shadow_delta=shadow,
            source="speech_event_batch",
            soothe=soothe,
        )
    except Exception as e:
        logger.warning(f"speech_event weather apply failed: {e}")
        return {"ok": False, "error": str(e)}





def _autofeed_thought(text: str, drive: str, strength: float = 0.45,
                      source: str = "autofeed") -> None:
    """往念头池喂一条闪念。drive=关联维度，strength=初始强度，source=来源标记。"""
    try:
        _desire.add_thought(text.strip(), drive, strength=strength, source=source)
    except Exception as e:
        logger.warning(f"Autofeed thought failed: {e}")


async def _execute_intent(intent: dict) -> None:
    """intent发作时只记日志，行为由窗口里的agent自己决定。
    satisfy/refractory挪到/api/desire/intent/ack——只有本地投递成功后才回落。"""
    if not intent:
        return
    drive = intent.get("drive_key", "")
    logger.info(f"Intent fired: {drive} (score={intent.get('score', 0):.2f}) — waiting for heartbeat bridge")

# --- Create MCP server instance# --- Create MCP server instance / 创建 MCP 服务器实例 ---
# host="0.0.0.0" so Docker container's SSE is externally reachable
# stdio mode ignores host (no network)
mcp = FastMCP(
    "Elektron",
    host="127.0.0.1",
    port=OMBRE_PORT,
)


# =============================================================
# Wander marks storage — annotations layered over existing buckets
# Wander 批注存储 —— 叠加在现有桶上的标记层
# =============================================================
MARKS_DB_PATH = os.path.join(config["buckets_dir"], "embeddings.db")
VALID_WANDER_MARKS = {"认", "不认", "悬置"}
LATENT_NOTES_PATH = os.path.join(config["buckets_dir"], "latent_notes.json")
LATENT_NOTE_POOL_VERSION = 1
LATENT_NOTE_USED_RETENTION_DAYS = 15
VALID_LATENT_NOTE_STATUSES = {"draft", "approved", "used", "deleted"}
VALID_LATENT_NOTE_TYPES = {"inward", "outward"}
VALID_LATENT_NOTE_DRIVES = set(DRIVE_KEYS) | {"general"}
# 投递后瘦骨：不随 used+15 天 prune 消失，供 wander(mode=trails) 串路径
SUBCURRENT_LOG_PATH = os.path.join(config["buckets_dir"], "subcurrent_log.json")
TRAIL_CURATIONS_PATH = os.path.join(config["buckets_dir"], "trail_curations.json")
TRAIL_CURATIONS_VERSION = 1
TRAIL_CURATION_QUERY_MAX = 500
TRAIL_CURATION_REF_MAX = 300
_TRAIL_CURATION_REF_RE = re.compile(r"^(?:bucket|latent):[A-Za-z0-9][A-Za-z0-9._:@/+\\-]{0,292}$")
_TRAIL_CURATION_LOCK = threading.RLock()
TRAIL_FAMILIES_PATH = os.path.join(config["buckets_dir"], "trail_families.json")
TRAIL_FAMILIES_VERSION = 1
_TRAIL_FAMILIES_LOCK = threading.RLock()
SUBCURRENT_LOG_VERSION = 1
TRAIL_ANCHOR_MAX = 120
TRAIL_EVIDENCE_MAX = 240
TRAIL_CURATION_DISPLAY_MAX = TRAIL_ANCHOR_MAX
TRAIL_DELTA_TEXT_MAX = 300
_TRAIL_ORDER_ID_RE = re.compile(r"^[0-9a-f]{64}$")
TRAIL_DEFAULT_LIMIT = 6
TRAIL_MAX_LIMIT = 12
_TRAIL_STOPWORDS = {
    "像是", "但是", "可能", "不是", "这个", "那个", "什么", "一个", "我们", "他们",
    "没有", "可以", "因为", "所以", "如果", "还是", "已经", "自己", "就是", "还是",
    "以及", "或者", "然后", "只是", "而是", "关于", "对于", "这种", "那样", "这些",
    "那些", "时候", "现在", "今天", "之前", "之后", "一下", "一种", "有点", "比较",
    "the", "and", "for", "with", "that", "this", "from", "into", "not", "but",
}


def _init_marks_table() -> None:
    os.makedirs(os.path.dirname(MARKS_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(MARKS_DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS marks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bucket_id TEXT NOT NULL,
                mark TEXT NOT NULL,
                note TEXT,
                timestamp TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_marks_bucket_id ON marks(bucket_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_marks_mark ON marks(mark)")
        conn.commit()
    finally:
        conn.close()


def _marks_conn():
    _init_marks_table()
    return sqlite3.connect(MARKS_DB_PATH)


def _normalize_wander_mark(mark: str) -> str:
    return (mark or "").strip()


def _load_all_marks() -> dict[str, list[dict]]:
    conn = _marks_conn()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, bucket_id, mark, note, timestamp FROM marks ORDER BY timestamp ASC, id ASC"
        ).fetchall()
    finally:
        conn.close()

    by_bucket: dict[str, list[dict]] = {}
    for row in rows:
        item = dict(row)
        by_bucket.setdefault(item["bucket_id"], []).append(item)
    return by_bucket


def _mark_counts(mark_rows: list[dict]) -> dict:
    counts = {"认": 0, "不认": 0, "悬置": 0, "inner": 0, "private": 0, "remove_inner": 0}
    for row in mark_rows:
        mark = _normalize_wander_mark(row.get("mark", ""))
        if mark in counts:
            counts[mark] += 1
    return counts


def _has_cross_date_recognition(mark_rows: list[dict]) -> bool:
    recognition_dates = {
        str(row.get("timestamp", ""))[:10]
        for row in mark_rows
        if _normalize_wander_mark(row.get("mark", "")) == "认"
        and len(str(row.get("timestamp", ""))) >= 10
    }
    return len(recognition_dates) >= 2


def _bucket_domains(meta: dict) -> set[str]:
    domains = meta.get("domain", [])
    if isinstance(domains, str):
        domains = [domains]
    return {str(d).strip().lower() for d in domains if str(d).strip()}


def _bucket_tags(meta: dict) -> set[str]:
    tags = meta.get("tags", [])
    if isinstance(tags, str):
        tags = [tags]
    return {str(t).strip().lower() for t in tags if str(t).strip()}


def _guess_wander_domain(bucket: dict, mark_rows: list[dict] = None) -> str:
    meta = bucket.get("metadata", {})
    marks = _mark_counts(mark_rows or [])
    domains = _bucket_domains(meta)
    tags = _bucket_tags(meta)

    if marks["private"] or "private" in domains:
        return "private"
    inner_removed = (marks["remove_inner"] > 0 or marks["不认"] >= 2) and "inner" not in domains
    if "inner" in domains or (
        not inner_removed
        and (marks["inner"] or (marks["认"] >= 3 and _has_cross_date_recognition(mark_rows or [])))
    ):
        return "inner"
    if "letter_human" in domains or "letter_human" in tags:
        return "letter_human"
    if "letter" in domains or "letter" in tags:
        return "letter"
    if "writing" in domains or "writing" in tags:
        return "writing"
    if "window" in domains or "window" in tags:
        return "window"
    return "memory"


def _is_private_bucket(bucket: dict, mark_rows: list[dict]) -> bool:
    return _guess_wander_domain(bucket, mark_rows) == "private"


def _is_unresolved_bucket(bucket: dict, mark_rows: list[dict] = None) -> bool:
    meta = bucket.get("metadata", {})
    labels = _bucket_domains(meta) | _bucket_tags(meta)
    return "unresolved" in labels or _mark_counts(mark_rows or [])["悬置"] > 0


# Domains that should not surface in breath/dream — they have their own wander modes
_WANDER_ONLY_DOMAINS = {"letter", "letter_human", "writing", "window", "private"}

def _is_wander_only_bucket(bucket: dict) -> bool:
    meta = bucket.get("metadata", {})
    labels = _bucket_domains(meta) | _bucket_tags(meta)
    return bool(labels & _WANDER_ONLY_DOMAINS)


def _track_label(track: str) -> str:
    if track == "square_wave":
        return " ▦方波"
    elif track == "sine_wave":
        return " ∿弦波"
    return ""


def _format_wander_entry(bucket: dict, mark_rows: list[dict], include_full_content: bool = True, show_bucket_id: bool = False) -> str:
    meta = bucket.get("metadata", {})
    counts = _mark_counts(mark_rows)
    created = str(meta.get("created", ""))[:10] or "无日期"
    title = meta.get("name") or (bucket.get("id", "") if include_full_content else "")
    bucket_id = bucket.get("id", "")
    content = strip_wikilinks(bucket.get("content", "")).strip()
    # Strip leading date line from content to avoid duplication with header
    import re as _re
    _date_line = _re.match(r"^(?:写在开头\s*·?\s*)?20\d{2}[\.\-/]\d{1,2}[\.\-/]\d{1,2}[^\n]*\n+", content)
    if not _date_line:
        _date_line = _re.match(r"^写在开头[^\n]*\n+", content)
    if _date_line:
        content = content[_date_line.end():]
    if not include_full_content and len(content) > 700:
        content = content[:700].rstrip() + "..."
    if not include_full_content:
        header = f"[{created}] {title}".rstrip()
        return f"{header}\n{content}"

    id_line = f"[bucket:{bucket_id}] " if show_bucket_id else ""
    track_tag = _track_label(meta.get("track", ""))
    is_inner = "inner" in [str(d).lower() for d in meta.get("domain", [])]
    inner_star = "🌟 " if is_inner else ""
    recent_notes = [
        r for r in sorted(mark_rows, key=lambda x: (x.get("timestamp", ""), x.get("id", 0)), reverse=True)
        if (r.get("note") or "").strip()
    ][:3]
    note_lines = []
    for row in recent_notes:
        note_lines.append(f"- [{row.get('mark')}] {row.get('note', '').strip()}")
    notes = "\n".join(note_lines) if note_lines else "（无）"

    return (
        f"{inner_star}{id_line}[{created}] {title}{track_tag}\n"
        f"批注统计：认:{counts['认']} / 不认:{counts['不认']} / 悬置:{counts['悬置']}\n"
        f"正文：\n{content}\n"
        f"最近三条批注原话：\n{notes}"
    )


def _is_settled_bucket(bucket: dict) -> bool:
    meta = bucket.get("metadata", {})
    return meta.get("resolved") == 1 or meta.get("resolved") is True or meta.get("digested") == 1 or meta.get("digested") is True


def _bucket_created_datetime(bucket: dict) -> datetime | None:
    raw = str(bucket.get("metadata", {}).get("created", "") or "")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw[:19])
    except (TypeError, ValueError):
        return None


ANALYZER_DEFAULT_SINCE_UTC = datetime(2026, 6, 24, 16, 0, 0, tzinfo=timezone.utc)
ANALYZER_LOCAL_TZ = timezone(timedelta(hours=8))


def _parse_analyzer_since(raw: str | None) -> datetime:
    value = str(raw or "").strip()
    if not value:
        return ANALYZER_DEFAULT_SINCE_UTC
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise ValueError("since must be ISO datetime")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _bucket_created_utc(bucket: dict) -> datetime | None:
    raw = str(bucket.get("metadata", {}).get("created", "") or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        try:
            dt = datetime.fromisoformat(raw[:19])
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ANALYZER_LOCAL_TZ)
    return dt.astimezone(timezone.utc)


def _analyzer_entry_type(bucket: dict, mark_rows: list[dict]) -> str:
    meta = bucket.get("metadata", {})
    btype = str(meta.get("type", "") or "").strip().lower()
    if btype == "feel":
        return "feel"
    if btype in {"breath", "dream"}:
        return ""
    domains = _bucket_domains(meta)
    tags = _bucket_tags(meta)
    labels = domains | tags
    if _is_unresolved_bucket(bucket, mark_rows):
        return "unresolved"
    if labels & {"letter", "letter_human"}:
        return "letter"
    if "writing" in labels:
        return "writing"
    if "window" in labels:
        return "window"
    if btype == "archived":
        return ""
    if not _is_settled_bucket(bucket) and _guess_wander_domain(bucket, mark_rows) == "memory":
        return "memory"
    return ""


def _analyzer_preview(content: str, limit: int = 1000) -> str:
    text = " ".join(strip_wikilinks(content or "").split())
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _bucket_created_ts(bucket: dict) -> float:
    raw = str(bucket.get("metadata", {}).get("created", "") or "").strip()
    if not raw:
        return 0.0
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return 0.0


async def _recent_weather_sources(limit: int = 2) -> list[dict]:
    try:
        buckets = await bucket_mgr.list_all(include_archive=False)
    except Exception:
        return []
    items = []
    for bucket in buckets:
        meta = bucket.get("metadata", {}) or {}
        text = _analyzer_preview(bucket.get("content", ""), limit=240)
        if not text:
            continue
        domains = _bucket_domains(meta)
        tags = _bucket_tags(meta)
        labels = domains | tags
        if "private" in labels:
            continue
        source_type = "feel" if meta.get("type") == "feel" else "memory"
        for label in ("letter_human", "letter", "writing", "window"):
            if label in labels:
                source_type = label
                break
        items.append({
            "text": text,
            "drive": "memory",
            "source_type": source_type,
            "strength": 0.72,
            "born_at": _bucket_created_ts(bucket),
        })
    items.sort(key=lambda item: item.get("born_at", 0), reverse=True)
    return items[:limit]


async def _weather_mood_entry() -> tuple[str, str]:
    from mood_pool import get_daily_mood
    sources = await _recent_weather_sources(limit=2)
    return await asyncio.to_thread(get_daily_mood, thoughts=sources or None)


def _fresh_soma_state() -> dict:
    try:
        path = _bucket_path("soma_state.json")
        with open(path) as f:
            data = _json_lib.load(f)
        if time.time() - float(data.get("updated_at", 0) or 0) > 3600:
            return {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _short_text(text: str, limit: int = 36) -> str:
    text = " ".join(strip_wikilinks(text or "").split())
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _latent_anchor(bucket: dict) -> str:
    import re as _re
    content = strip_wikilinks(bucket.get("content", "")).strip()
    content = _re.sub(r"^#{1,6}\s*", "", content)
    content = _re.sub(r"^(?:写在开头\s*·?\s*)?20\d{2}[\.\-/]\d{1,2}[\.\-/]\d{1,2}[^\n]*\n+", "", content)
    parts = [
        p.strip(" \t\r\n-—·")
        for p in _re.split(r"[\n。！？!?]+", content)
        if p.strip(" \t\r\n-—·")
    ]
    if not parts:
        return ""
    question = next((p for p in parts if "？" in p or "?" in p or "还没" in p or "悬" in p), "")
    return _short_text(question or parts[0], 82)


def _latent_theme(bucket: dict, mark_rows: list[dict]) -> str:
    meta = bucket.get("metadata", {})
    recent_note = next(
        (
            str(row.get("note", "")).strip()
            for row in sorted(mark_rows, key=lambda x: (x.get("timestamp", ""), x.get("id", 0)), reverse=True)
            if str(row.get("note", "")).strip()
        ),
        "",
    )
    title = str(meta.get("name") or "").strip()
    tags = [str(t).strip() for t in meta.get("tags", []) if str(t).strip()] if isinstance(meta.get("tags", []), list) else []
    return _short_text(recent_note or title or (tags[0] if tags else "") or _latent_anchor(bucket) or bucket.get("id", ""), 28)


def _latent_wander_mode(bucket: dict, mark_rows: list[dict], kind: str) -> str:
    if kind == "悬置":
        return "unresolved"
    domain = _guess_wander_domain(bucket, mark_rows)
    if domain in {"inner", "writing", "letter", "letter_human", "window", "private"}:
        return domain
    return "memory"


def _latent_note_payload(bucket: dict, mark_rows: list[dict], kind: str, score: float) -> dict:
    theme = _latent_theme(bucket, mark_rows)
    anchor = _latent_anchor(bucket)
    templates = {
        "悬置": f"以前和{HUMAN_NAME}聊过「{theme}」。那一页像没合上的门，风还从缝里过。",
        "认过": f"以前和{HUMAN_NAME}聊过「{theme}」。有一小块旧光还压在纸边。",
        "inner": f"以前和{HUMAN_NAME}聊过「{theme}」。它像沉在底下的石头，水面很静。",
        "archive": f"以前和{HUMAN_NAME}留下过「{theme}」。像抽屉里没干透的一张便签。",
        "old_memory": f"以前和{HUMAN_NAME}聊过「{theme}」。梦里像有人把那句话又翻了一面。",
    }
    meta = bucket.get("metadata", {})
    return {
        "kind": kind,
        "bucket_id": bucket.get("id", ""),
        "theme": theme,
        "line": templates.get(kind, templates["old_memory"]),
        "anchor": anchor,
        "wander_mode": _latent_wander_mode(bucket, mark_rows, kind),
        "query": theme,
        "created": meta.get("created", ""),
        "score": round(score, 3),
    }


def _latent_candidate_score(bucket: dict, mark_rows: list[dict], now: datetime) -> tuple[str, float] | None:
    if _is_private_bucket(bucket, mark_rows):
        return None
    meta = bucket.get("metadata", {})
    counts = _mark_counts(mark_rows)
    domains = _bucket_domains(meta)
    tags = _bucket_tags(meta)
    guessed = _guess_wander_domain(bucket, mark_rows)
    settled = _is_settled_bucket(bucket)

    kind = ""
    base = 0.0
    if counts["悬置"] > 0:
        kind, base = "悬置", 1.2 + min(counts["悬置"], 4) * 0.12
    elif counts["认"] > 0:
        kind, base = "认过", 0.9 + min(counts["认"], 4) * 0.08
    elif guessed == "inner":
        kind, base = "inner", 0.82
    elif not settled and (domains & {"letter", "letter_human", "writing", "window"} or tags & {"letter", "letter_human", "writing", "window"}):
        kind, base = "archive", 0.62
    elif not settled and guessed == "memory":
        kind, base = "old_memory", 0.28
    else:
        return None

    created = _bucket_created_datetime(bucket)
    if created:
        age_hours = max(0.0, (now - created).total_seconds() / 3600)
        if age_hours < 24:
            base *= 0.3
        elif age_hours > 24 * 120:
            base *= 0.72
    activation_count = float(meta.get("activation_count", 1) or 1)
    base *= 1.0 / (1.0 + max(0.0, activation_count - 1.0) * 0.12)
    base *= random.uniform(0.85, 1.15)
    return kind, base


def _load_latent_notes() -> dict:
    try:
        with open(LATENT_NOTES_PATH, "r", encoding="utf-8") as f:
            data = _json_lib.load(f)
        if isinstance(data, dict):
            notes = data.get("notes", [])
            if isinstance(notes, list):
                return {"version": data.get("version", LATENT_NOTE_POOL_VERSION), "notes": notes}
    except Exception:
        pass
    return {"version": LATENT_NOTE_POOL_VERSION, "notes": []}


def _save_latent_notes(data: dict) -> None:
    os.makedirs(os.path.dirname(LATENT_NOTES_PATH), exist_ok=True)
    tmp = f"{LATENT_NOTES_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        _json_lib.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, LATENT_NOTES_PATH)


def _latent_note_dt(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", ""))
    except Exception:
        return None


def _prune_expired_latent_notes(data: dict, now: datetime | None = None) -> bool:
    """Remove unpinned used notes after the sink retention window."""
    now = now or datetime.utcnow()
    cutoff = now - timedelta(days=LATENT_NOTE_USED_RETENTION_DAYS)
    notes = data.get("notes", [])
    if not isinstance(notes, list):
        return False
    kept = []
    changed = False
    for note in notes:
        if (
            isinstance(note, dict)
            and note.get("status") == "used"
            and not note.get("pinned")
        ):
            used_at = _latent_note_dt(note.get("used_at") or note.get("updated_at") or note.get("created_at"))
            if used_at and used_at < cutoff:
                changed = True
                continue
        kept.append(note)
    if changed:
        data["notes"] = kept
        _touch_latent_note_data(data)
    return changed


def _latent_note_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")


def _normalize_latent_note_status(status: str, default: str = "draft") -> str:
    value = str(status or default).strip().lower()
    return value if value in VALID_LATENT_NOTE_STATUSES else default


def _normalize_latent_note_type(note_type: str) -> str:
    value = str(note_type or "").strip().lower()
    return value if value in VALID_LATENT_NOTE_TYPES else "inward"


def _normalize_latent_source_kind(kind: str, default: str = "manual") -> str:
    value = str(kind or "").strip()
    allowed = {"manual", "thought_pool", "inner", "archive", "old_memory", "悬置", "认过"}
    return value if value in allowed else default


def _default_latent_drive_tag(note_type: str) -> str:
    return "curiosity" if _normalize_latent_note_type(note_type) == "outward" else "reflection"


def _normalize_latent_drive_tag(drive_tag: str, note_type: str = "") -> str:
    value = normalize_drive_key(drive_tag)
    if not value:
        value = str(drive_tag or "").strip().lower()
    return value if value in VALID_LATENT_NOTE_DRIVES else _default_latent_drive_tag(note_type)


def _latent_note_line(note: dict) -> str:
    return " ".join(str(note.get("dream_line") or note.get("line") or "").split())


def _find_latent_note(data: dict, note_id: str) -> dict | None:
    note_id = str(note_id or "").strip()
    if not note_id:
        return None
    for note in data.get("notes", []):
        if str(note.get("id") or "") == note_id:
            return note
    return None


def _touch_latent_note_data(data: dict) -> None:
    data["version"] = LATENT_NOTE_POOL_VERSION
    data["updated_at"] = _latent_note_ts()


def _approved_latent_note_payload(note: dict) -> dict | None:
    line = _latent_note_line(note)
    note_id = str(note.get("id") or "").strip()
    if not line or not note_id:
        return None
    try:
        delivered_count = int(note.get("delivered_count") or 0)
    except (TypeError, ValueError):
        delivered_count = 0
    return {
        "kind": "latent_pool",
        "note_type": _normalize_latent_note_type(note.get("note_type")),
        "drive_tag": _normalize_latent_drive_tag(note.get("drive_tag"), note.get("note_type")),
        "note_id": note_id,
        "bucket_id": note_id,
        "source_bucket_id": note.get("source_bucket_id", ""),
        "theme": note.get("source_title") or note.get("source_kind") or "潜意识便签",
        "line": line,
        "anchor": note.get("source_fragment", ""),
        "pinned": bool(note.get("pinned")),
        "delivered_count": max(0, delivered_count),
        "last_delivered_at": str(note.get("last_delivered_at") or ""),
        "wander_mode": "memory",
        "query": line,
        "created": note.get("created_at", ""),
        "score": 1.0,
    }


def _iter_approved_latent_payloads(data: dict, exclude_ids: set[str]) -> list[dict]:
    """Approved latent notes only — one pool, filtered by exclude set."""
    out: list[dict] = []
    for note in data.get("notes", []):
        if note.get("status") != "approved":
            continue
        note_id = str(note.get("id") or "").strip()
        if not note_id or note_id in exclude_ids:
            continue
        payload = _approved_latent_note_payload(note)
        if not payload:
            continue
        payload["drive_tag"] = _normalize_latent_drive_tag(
            payload.get("drive_tag"), note.get("note_type")
        )
        out.append(payload)
    return out


def _latent_select_weight(note: dict) -> float:
    """Within one drive_tag: burn one-shot notes first; pinned is reusable reserve, not VIP.

    pinned = 常驻可复用，不是最高优先。空池时不硬塞文案（上层直接不显示 Subcurrent）。
    """
    delivered = max(0, int(note.get("delivered_count") or 0))
    pinned = bool(note.get("pinned"))
    # Prefer never-delivered ordinary notes; then lightly used; pinned last as reserve.
    if not pinned and delivered <= 0:
        return 8.0
    if not pinned and delivered == 1:
        return 3.0
    if not pinned:
        return 1.2
    # pinned: always available, lower weight so day-to-day burns fresh stock first
    if delivered <= 0:
        return 1.5
    return max(0.4, 1.0 / (1.0 + 0.35 * delivered))


def _weighted_pick_latent(pool: list[dict], match: str) -> dict | None:
    if not pool:
        return None
    weights = [max(0.05, _latent_select_weight(n)) for n in pool]
    chosen = random.choices(pool, weights=weights, k=1)[0]
    chosen = dict(chosen)
    chosen["pool_match"] = match
    chosen["source"] = "approved_pool"
    return chosen


def _approved_latent_stock_by_drive(data: dict | None = None) -> dict[str, int]:
    """Count approved notes per drive_tag (for generate-to-empty-pools)."""
    data = data if isinstance(data, dict) else _load_latent_notes()
    stock = {k: 0 for k in sorted(VALID_LATENT_NOTE_DRIVES)}
    for note in data.get("notes", []):
        if note.get("status") != "approved":
            continue
        tag = _normalize_latent_drive_tag(note.get("drive_tag"), note.get("note_type"))
        if tag in stock:
            stock[tag] += 1
        else:
            stock[tag] = stock.get(tag, 0) + 1
    return stock


# 自动补货只认九维 Drive，永不进 general（general 不是投递主维）
LATENT_FILL_DRIVES = (
    "curiosity", "stewardship", "reflection",
    "attachment", "libido", "possessiveness",
    "social", "fatigue", "stress",
)

# 各维 approved 目标库存；低于此 = 需要补。按「真实最少」排序，不是只看 general。
LATENT_STOCK_TARGET = {
    "curiosity": 6,
    "stewardship": 6,
    "reflection": 6,
    "attachment": 5,
    "libido": 4,
    "possessiveness": 4,
    "social": 5,
    "fatigue": 5,
    "stress": 4,
}
# 绝对低水位：不管 target，库存最少的几维一定进补货名单
LATENT_STOCK_ALWAYS_BOTTOM = 4


def _latent_low_stock_drives(data: dict | None = None) -> list[tuple[str, int, int]]:
    """Return [(drive_tag, approved_count, deficit), ...] most empty first.

    - 永不把 general 算进自动补货
    - 按 approved 绝对数量排序（最少的优先）
    - deficit = max(0, target - count)；垫底维至少 deficit≥1 以便分到配额
    """
    stock = _approved_latent_stock_by_drive(data)
    ranked = sorted(
        [
            (
                tag,
                int(stock.get(tag, 0) or 0),
                max(0, int(LATENT_STOCK_TARGET.get(tag, 4)) - int(stock.get(tag, 0) or 0)),
            )
            for tag in LATENT_FILL_DRIVES
        ],
        key=lambda r: (r[1], -r[2], r[0]),
    )
    bottom = {r[0] for r in ranked[:LATENT_STOCK_ALWAYS_BOTTOM]}
    rows = []
    for tag, count, deficit in ranked:
        if deficit <= 0 and tag not in bottom:
            continue
        # 垫底维即使小 target 已满，也至少 need=1，避免 social/fatigue 被挤出配额
        need = deficit if deficit > 0 else (1 if tag in bottom else 0)
        if need > 0:
            rows.append((tag, count, need))
    rows.sort(key=lambda r: (r[1], -r[2], r[0]))
    return rows


def _select_approved_latent_note(exclude_ids: set[str], drive_key: str = "") -> dict | None:
    """Pick Subcurrent from the approved latent pool only.

    Rules:
    - With drive_key: exact drive_tag only. Empty → no Subcurrent line (wake hook only).
    - Same-tag all excluded → relax exclude, still never cross drives.
    - Within tag: weighted pick — one-shot fresh first; pinned is reusable, not VIP.
    """
    data = _load_latent_notes()
    drive_key = normalize_drive_key(drive_key)
    exclude_ids = {str(x).strip() for x in (exclude_ids or set()) if str(x).strip()}

    fresh = _iter_approved_latent_payloads(data, exclude_ids)

    if drive_key:
        exact = [n for n in fresh if n.get("drive_tag") == drive_key]
        hit = _weighted_pick_latent(exact, "exact")
        if hit:
            return hit
        all_same = [
            n for n in _iter_approved_latent_payloads(data, set())
            if n.get("drive_tag") == drive_key
        ]
        return _weighted_pick_latent(all_same, "relaxed_exclude")

    hit = _weighted_pick_latent(fresh, "any")
    if hit:
        return hit
    return _weighted_pick_latent(_iter_approved_latent_payloads(data, set()), "relaxed_exclude")


def _ack_approved_latent_note(note_id: str) -> dict:
    data = _load_latent_notes()
    note = _find_latent_note(data, note_id)
    if not note:
        raise KeyError("latent note not found")
    if note.get("status") == "approved":
        ts = _latent_note_ts()
        note["last_delivered_at"] = ts
        note["delivered_count"] = int(note.get("delivered_count") or 0) + 1
        if note.get("pinned"):
            note["updated_at"] = ts
            _touch_latent_note_data(data)
            _save_latent_notes(data)
            try:
                _upsert_subcurrent_log(note)
            except Exception as e:
                logger.warning(f"subcurrent_log upsert failed (pinned): {e}")
            return note
        note["status"] = "used"
        note["used_at"] = ts
        note["updated_at"] = ts
        _touch_latent_note_data(data)
        _save_latent_notes(data)
        try:
            _upsert_subcurrent_log(note)
        except Exception as e:
            logger.warning(f"subcurrent_log upsert failed: {e}")
    return note


# =============================================================
# Subcurrent log + Trails — delivery bones & differential paths
# 投递瘦骨 + 折痕时间线（好奇了才 wander，不灌心跳）
# =============================================================

def _load_subcurrent_log() -> dict:
    try:
        with open(SUBCURRENT_LOG_PATH, "r", encoding="utf-8") as f:
            data = _json_lib.load(f)
        if isinstance(data, dict):
            entries = data.get("entries", [])
            if isinstance(entries, list):
                return {
                    "version": data.get("version", SUBCURRENT_LOG_VERSION),
                    "entries": [e for e in entries if isinstance(e, dict)],
                }
    except Exception:
        pass
    return {"version": SUBCURRENT_LOG_VERSION, "entries": []}


def _save_subcurrent_log(data: dict) -> None:
    os.makedirs(os.path.dirname(SUBCURRENT_LOG_PATH) or ".", exist_ok=True)
    payload = {
        "version": SUBCURRENT_LOG_VERSION,
        "updated_at": _latent_note_ts(),
        "entries": data.get("entries") if isinstance(data.get("entries"), list) else [],
    }
    tmp = f"{SUBCURRENT_LOG_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        _json_lib.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SUBCURRENT_LOG_PATH)


def _trail_query_identity(query: str) -> tuple[str, str]:
    normalized = " ".join(unicodedata.normalize("NFKC", str(query or "")).casefold().split())
    key = hashlib.sha256(normalized.encode("utf-8")).hexdigest() if normalized else ""
    return key, normalized


def _load_trail_curations(*, strict: bool = False) -> dict:
    try:
        with open(TRAIL_CURATIONS_PATH, "r", encoding="utf-8") as f:
            data = _json_lib.load(f)
        if isinstance(data, dict) and isinstance(data.get("queries"), dict):
            return {"version": TRAIL_CURATIONS_VERSION, "queries": data["queries"]}
        raise ValueError("trail curation store has invalid schema")
    except FileNotFoundError:
        return {"version": TRAIL_CURATIONS_VERSION, "queries": {}}
    except Exception:
        if strict:
            raise
        logger.error("trail curation store is corrupt; ignoring overlays until repaired")
    return {"version": TRAIL_CURATIONS_VERSION, "queries": {}}


def _save_trail_curations(data: dict) -> None:
    directory = os.path.dirname(TRAIL_CURATIONS_PATH) or "."
    os.makedirs(directory, exist_ok=True)
    payload = {
        "version": TRAIL_CURATIONS_VERSION,
        "updated_at": _latent_note_ts(),
        "queries": data.get("queries") if isinstance(data.get("queries"), dict) else {},
    }
    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=directory,
            prefix=".trail_curations.",
            suffix=".tmp",
            delete=False,
        ) as f:
            tmp_path = f.name
            _json_lib.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, TRAIL_CURATIONS_PATH)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _trail_curations_for_query(query: str) -> dict:
    key, normalized = _trail_query_identity(query)
    if not key:
        return {}
    row = _load_trail_curations().get("queries", {}).get(key, {})
    if not isinstance(row, dict) or row.get("query") != normalized:
        return {}
    nodes = row.get("nodes", {})
    return nodes if isinstance(nodes, dict) else {}


def _validate_trail_curation_payload(body) -> tuple[str, str, str, str]:
    if not isinstance(body, dict):
        raise ValueError("JSON body must be an object")
    for field in ("query", "action"):
        if not isinstance(body.get(field), str):
            raise ValueError(f"{field} must be a string")
    ref_value = body.get("node_ref", body.get("ref", body.get("id")))
    if not isinstance(ref_value, str):
        raise ValueError("node_ref must be a string")
    display_value = body.get("display_anchor", "")
    if not isinstance(display_value, str):
        raise ValueError("display_anchor must be a string")
    query = body["query"].strip()
    ref = ref_value.strip()
    action = body["action"].strip().lower()
    display = " ".join(display_value.split())
    if not query or len(query) > TRAIL_CURATION_QUERY_MAX:
        raise ValueError(f"query must be 1..{TRAIL_CURATION_QUERY_MAX} characters")
    if len(ref) > TRAIL_CURATION_REF_MAX or not _TRAIL_CURATION_REF_RE.fullmatch(ref):
        raise ValueError("invalid node_ref")
    if action not in {"edit", "hide", "unhide", "reset"}:
        raise ValueError("action must be edit, hide, unhide, or reset")
    if action == "edit" and not display:
        raise ValueError("display_anchor required for edit")
    if len(display) > TRAIL_CURATION_DISPLAY_MAX:
        raise ValueError(f"display_anchor must be <= {TRAIL_CURATION_DISPLAY_MAX} characters")
    return query, ref, action, display


def _update_trail_curation(query: str, node_ref: str, action: str, display_anchor: str = "") -> dict:
    query, ref, action, display_anchor = _validate_trail_curation_payload({
        "query": query,
        "node_ref": node_ref,
        "action": action,
        "display_anchor": display_anchor,
    })
    key, normalized = _trail_query_identity(query)
    lock_path = f"{TRAIL_CURATIONS_PATH}.lock"
    os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
    with _TRAIL_CURATION_LOCK:
        with open(lock_path, "a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                data = _load_trail_curations(strict=True)
                queries = data.setdefault("queries", {})
                query_row = queries.setdefault(key, {"query": normalized, "nodes": {}})
                if query_row.get("query") != normalized:
                    query_row = {"query": normalized, "nodes": {}}
                    queries[key] = query_row
                nodes = query_row.setdefault("nodes", {})
                if action == "reset":
                    row = dict(nodes.get(ref) or {})
                    row.pop("display_anchor", None)
                    has_effective_fields = any(
                        value for field, value in row.items()
                        if field != "updated_at"
                    )
                    if has_effective_fields:
                        row["updated_at"] = _latent_note_ts()
                        nodes[ref] = row
                    else:
                        nodes.pop(ref, None)
                elif action == "unhide" and ref not in nodes:
                    pass
                else:
                    row = dict(nodes.get(ref) or {})
                    if action == "hide":
                        row["hidden"] = True
                    elif action == "unhide":
                        row.pop("hidden", None)
                    else:
                        row["display_anchor"] = display_anchor
                    has_effective_fields = any(
                        value for field, value in row.items()
                        if field != "updated_at"
                    )
                    if has_effective_fields:
                        row["updated_at"] = _latent_note_ts()
                        nodes[ref] = row
                    else:
                        nodes.pop(ref, None)
                if not nodes:
                    queries.pop(key, None)
                _save_trail_curations(data)
                hidden = sorted(
                    node_key for node_key, row in nodes.items()
                    if isinstance(row, dict) and row.get("hidden") is True
                )
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    return {
        "query_key": key,
        "query": normalized,
        "node_ref": ref,
        "action": action,
        "hidden": hidden,
    }


def _validate_trail_delta_payload(body) -> tuple[str, str, str, str, str, str]:
    if not isinstance(body, dict):
        raise ValueError("JSON body must be an object")
    for field in ("query", "node_ref", "action"):
        if not isinstance(body.get(field), str):
            raise ValueError(f"{field} must be a string")
    query = body["query"].strip()
    ref = body["node_ref"].strip()
    action = body["action"].strip().lower()
    text = body.get("text", "")
    baseline = body.get("baseline_ref", "")
    basis_order_id = body.get("basis_order_id", "")
    for field, value in (("text", text), ("baseline_ref", baseline), ("basis_order_id", basis_order_id)):
        if not isinstance(value, str):
            raise ValueError(f"{field} must be a string")
    text = " ".join(text.split())
    baseline = baseline.strip()
    basis_order_id = basis_order_id.strip().lower()
    if not query or len(query) > TRAIL_CURATION_QUERY_MAX:
        raise ValueError(f"query must be 1..{TRAIL_CURATION_QUERY_MAX} characters")
    if len(ref) > TRAIL_CURATION_REF_MAX or not _TRAIL_CURATION_REF_RE.fullmatch(ref):
        raise ValueError("invalid node_ref")
    if action not in {"claim", "clear"}:
        raise ValueError("action must be claim or clear")
    if action == "claim":
        if not text or len(text) > TRAIL_DELTA_TEXT_MAX:
            raise ValueError(f"text must be 1..{TRAIL_DELTA_TEXT_MAX} characters")
        if not _TRAIL_CURATION_REF_RE.fullmatch(baseline):
            raise ValueError("invalid baseline_ref")
        if baseline == ref:
            raise ValueError("baseline_ref must differ from node_ref")
        if not _TRAIL_ORDER_ID_RE.fullmatch(basis_order_id):
            raise ValueError("invalid basis_order_id")
    return query, ref, action, text, baseline, basis_order_id


def _update_trail_delta(
    query: str,
    node_ref: str,
    action: str,
    text: str = "",
    baseline_ref: str = "",
    basis_order_id: str = "",
) -> dict:
    query, ref, action, text, baseline_ref, basis_order_id = _validate_trail_delta_payload({
        "query": query,
        "node_ref": node_ref,
        "action": action,
        "text": text,
        "baseline_ref": baseline_ref,
        "basis_order_id": basis_order_id,
    })
    key, normalized = _trail_query_identity(query)
    lock_path = f"{TRAIL_CURATIONS_PATH}.lock"
    os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
    with _TRAIL_CURATION_LOCK:
        with open(lock_path, "a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                data = _load_trail_curations(strict=True)
                queries = data.setdefault("queries", {})
                query_row = queries.setdefault(key, {"query": normalized, "nodes": {}})
                if query_row.get("query") != normalized:
                    query_row = {"query": normalized, "nodes": {}}
                    queries[key] = query_row
                nodes = query_row.setdefault("nodes", {})
                row = dict(nodes.get(ref) or {})
                if action == "clear":
                    row.pop("delta_claimed", None)
                else:
                    now = _latent_note_ts()
                    previous = row.get("delta_claimed")
                    claimed_at = (
                        previous.get("claimed_at")
                        if isinstance(previous, dict) and previous.get("claimed_at")
                        else now
                    )
                    row["delta_claimed"] = {
                        "text": text,
                        "baseline_ref": baseline_ref,
                        "source": "manual",
                        "basis_order_id": basis_order_id,
                        "claimed_at": claimed_at,
                        "updated_at": now,
                    }
                effective = any(value for field, value in row.items() if field != "updated_at")
                if effective:
                    row["updated_at"] = _latent_note_ts()
                    nodes[ref] = row
                else:
                    nodes.pop(ref, None)
                if not nodes:
                    queries.pop(key, None)
                _save_trail_curations(data)
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    return {
        "query_key": key,
        "query": normalized,
        "node_ref": ref,
        "action": action,
    }


class _TrailFamilyError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _trail_family_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _load_trail_families(*, strict: bool = True) -> dict:
    try:
        with open(TRAIL_FAMILIES_PATH, "r", encoding="utf-8") as f:
            data = _json_lib.load(f)
    except FileNotFoundError:
        return {"version": TRAIL_FAMILIES_VERSION, "families": {}}
    if not isinstance(data, dict) or not isinstance(data.get("families"), dict):
        raise ValueError("trail families store has invalid schema")
    for family_id, family in data["families"].items():
        if (
            not isinstance(family_id, str)
            or not re.fullmatch(r"fam_[0-9a-f]{32}", family_id)
            or not isinstance(family, dict)
            or family.get("id") != family_id
            or not isinstance(family.get("title"), str)
            or not isinstance(family.get("core_question"), str)
            or not isinstance(family.get("created_at"), str)
            or not isinstance(family.get("updated_at"), str)
            or not isinstance(family.get("revision"), int)
            or family["revision"] < 1
            or not isinstance(family.get("query_entries"), list)
            or not isinstance(family.get("members"), list)
        ):
            raise ValueError("trail families store has invalid family row")
        entry_ids = set()
        entries_by_id = {}
        for entry in family["query_entries"]:
            if (
                not isinstance(entry, dict)
                or not isinstance(entry.get("id"), str)
                or not re.fullmatch(r"entry_[0-9a-f]{32}", entry["id"])
                or not isinstance(entry.get("query"), str)
                or not isinstance(entry.get("query_key"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", entry["query_key"])
                or not isinstance(entry.get("label"), str)
                or not isinstance(entry.get("added_at"), str)
            ):
                raise ValueError("trail families store has invalid query entry")
            expected_key, normalized_query = _trail_query_identity(entry["query"])
            if entry["query"] != normalized_query or entry["query_key"] != expected_key:
                raise ValueError("trail families store has mismatched query entry")
            if entry["id"] in entry_ids:
                raise ValueError("trail families store has duplicate query entry id")
            entry_ids.add(entry["id"])
            entries_by_id[entry["id"]] = entry
        member_ids = set()
        member_refs = set()
        member_orders = set()
        for member in family["members"]:
            entry = entries_by_id.get(member.get("query_entry_id"))
            if (
                not isinstance(member, dict)
                or not isinstance(entry, dict)
                or not isinstance(member.get("id"), str)
                or not re.fullmatch(r"member_[0-9a-f]{32}", member["id"])
                or not isinstance(member.get("node_ref"), str)
                or not _TRAIL_CURATION_REF_RE.fullmatch(member["node_ref"])
                or not isinstance(member.get("query"), str)
                or not isinstance(member.get("query_key"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", member["query_key"])
                or member["query"] != entry["query"]
                or member["query_key"] != entry["query_key"]
                or not isinstance(member.get("source_order_id"), str)
                or not _TRAIL_ORDER_ID_RE.fullmatch(member["source_order_id"])
                or not isinstance(member.get("observed_at"), str)
                or not isinstance(member.get("added_at"), str)
                or not isinstance(member.get("manual_note"), str)
                or not isinstance(member.get("order"), int)
                or member["order"] < 0
            ):
                raise ValueError("trail families store has invalid member")
            if (
                member["id"] in member_ids
                or member["node_ref"] in member_refs
                or member["order"] in member_orders
            ):
                raise ValueError("trail families store has duplicate member")
            member_ids.add(member["id"])
            member_refs.add(member["node_ref"])
            member_orders.add(member["order"])
        if member_orders != set(range(len(family["members"]))):
            raise ValueError("trail families store has invalid member order")
    return {"version": TRAIL_FAMILIES_VERSION, "families": data["families"]}


def _save_trail_families(data: dict) -> None:
    directory = os.path.dirname(TRAIL_FAMILIES_PATH) or "."
    os.makedirs(directory, exist_ok=True)
    payload = {
        "version": TRAIL_FAMILIES_VERSION,
        "updated_at": _latent_note_ts(),
        "families": data.get("families") if isinstance(data.get("families"), dict) else {},
    }
    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=directory,
            prefix=".trail_families.", suffix=".tmp", delete=False,
        ) as f:
            tmp_path = f.name
            _json_lib.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, TRAIL_FAMILIES_PATH)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _family_text(value, field: str, max_len: int, *, required: bool = False) -> str:
    if not isinstance(value, str):
        raise _TrailFamilyError(400, f"{field} must be a string")
    text = " ".join(value.split())
    if required and not text:
        raise _TrailFamilyError(400, f"{field} required")
    if len(text) > max_len:
        raise _TrailFamilyError(400, f"{field} must be <= {max_len} characters")
    return text


def _family_revision(body: dict, family: dict) -> None:
    expected = body.get("expected_revision")
    if not isinstance(expected, int):
        raise _TrailFamilyError(400, "expected_revision must be an integer")
    if expected != family.get("revision"):
        raise _TrailFamilyError(409, "revision conflict")


def _family_find(rows: list, row_id: str, field: str) -> dict:
    row = next((item for item in rows if isinstance(item, dict) and item.get("id") == row_id), None)
    if row is None:
        raise _TrailFamilyError(404, f"{field} not found")
    return row


def _mutate_trail_families(mutator):
    lock_path = f"{TRAIL_FAMILIES_PATH}.lock"
    os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
    with _TRAIL_FAMILIES_LOCK:
        with open(lock_path, "a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                data = _load_trail_families(strict=True)
                result = mutator(data)
                _save_trail_families(data)
                return result
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _mutate_trail_family(body: dict) -> dict:
    if not isinstance(body, dict):
        raise _TrailFamilyError(400, "JSON body must be an object")
    action = _family_text(body.get("action"), "action", 20, required=True).lower()

    def mutate(data):
        families = data["families"]
        if action == "create":
            title = _family_text(body.get("title"), "title", 160, required=True)
            now = _latent_note_ts()
            family_id = _trail_family_id("fam")
            family = {
                "id": family_id, "title": title,
                "core_question": _family_text(body.get("core_question", ""), "core_question", 1000),
                "created_at": now, "updated_at": now, "revision": 1,
                "query_entries": [], "members": [],
            }
            families[family_id] = family
            return family
        family_id = _family_text(body.get("family_id"), "family_id", 80, required=True)
        family = families.get(family_id)
        if not isinstance(family, dict):
            raise _TrailFamilyError(404, "family not found")
        _family_revision(body, family)
        if action == "delete":
            del families[family_id]
            return {"id": family_id, "deleted": True}
        if action != "update":
            raise _TrailFamilyError(400, "action must be create, update, or delete")
        if "title" in body:
            family["title"] = _family_text(body["title"], "title", 160, required=True)
        if "core_question" in body:
            family["core_question"] = _family_text(
                body["core_question"], "core_question", 1000
            )
        family["revision"] += 1
        family["updated_at"] = _latent_note_ts()
        return family

    return _mutate_trail_families(mutate)


def _mutate_trail_family_entry(family_id: str, body: dict) -> dict:
    if not isinstance(body, dict):
        raise _TrailFamilyError(400, "JSON body must be an object")
    action = _family_text(body.get("action"), "action", 20, required=True).lower()

    def mutate(data):
        family = data["families"].get(family_id)
        if not isinstance(family, dict):
            raise _TrailFamilyError(404, "family not found")
        _family_revision(body, family)
        entries = family["query_entries"]
        if action == "add":
            query = _family_text(
                body.get("query"), "query", TRAIL_CURATION_QUERY_MAX, required=True
            )
            query_key, normalized = _trail_query_identity(query)
            if any(entry.get("query_key") == query_key for entry in entries):
                raise _TrailFamilyError(409, "query entry already present")
            entry = {
                "id": _trail_family_id("entry"),
                "query": normalized,
                "query_key": query_key,
                "label": _family_text(body.get("label", normalized), "label", 160, required=True),
                "added_at": _latent_note_ts(),
            }
            entries.append(entry)
            result = entry
        else:
            entry_id = _family_text(body.get("entry_id"), "entry_id", 90, required=True)
            entry = _family_find(entries, entry_id, "query entry")
            if action == "remove":
                if any(member.get("query_entry_id") == entry_id for member in family["members"]):
                    raise _TrailFamilyError(409, "cannot remove query entry with members")
                entries.remove(entry)
                result = {"id": entry_id, "removed": True}
            elif action == "update":
                entry["label"] = _family_text(
                    body.get("label", entry.get("label", "")), "label", 160, required=True
                )
                result = entry
            else:
                raise _TrailFamilyError(400, "invalid query entry action")
        family["revision"] += 1
        family["updated_at"] = _latent_note_ts()
        return {"family_id": family_id, "revision": family["revision"], "result": result}

    return _mutate_trail_families(mutate)


def _mutate_trail_family_member(family_id: str, body: dict) -> dict:
    if not isinstance(body, dict):
        raise _TrailFamilyError(400, "JSON body must be an object")
    action = _family_text(body.get("action"), "action", 20, required=True).lower()

    def mutate(data):
        family = data["families"].get(family_id)
        if not isinstance(family, dict):
            raise _TrailFamilyError(404, "family not found")
        _family_revision(body, family)
        members = family["members"]
        if action == "add":
            entry_id = _family_text(
                body.get("query_entry_id"), "query_entry_id", 90, required=True
            )
            entry = _family_find(family["query_entries"], entry_id, "query entry")
            node_ref = _family_text(
                body.get("node_ref"), "node_ref", TRAIL_CURATION_REF_MAX, required=True
            )
            if not _TRAIL_CURATION_REF_RE.fullmatch(node_ref):
                raise _TrailFamilyError(400, "invalid node_ref")
            if any(member.get("node_ref") == node_ref for member in members):
                raise _TrailFamilyError(409, "member ref already present")
            source_order_id = _family_text(
                body.get("source_order_id"), "source_order_id", 64, required=True
            ).lower()
            if not _TRAIL_ORDER_ID_RE.fullmatch(source_order_id):
                raise _TrailFamilyError(400, "invalid source_order_id")
            member = {
                "id": _trail_family_id("member"),
                "node_ref": node_ref,
                "query_entry_id": entry_id,
                "query_key": entry["query_key"],
                "query": entry["query"],
                "observed_at": _family_text(
                    body.get("observed_at", ""), "observed_at", 40
                ),
                "source_order_id": source_order_id,
                "added_at": _latent_note_ts(),
                "order": len(members),
                "manual_note": _family_text(
                    body.get("manual_note", ""), "manual_note", 1000
                ),
            }
            members.append(member)
            result = member
        else:
            member_id = _family_text(body.get("member_id"), "member_id", 90, required=True)
            member = _family_find(members, member_id, "member")
            if action == "remove":
                members.remove(member)
                for index, item in enumerate(sorted(members, key=lambda row: row.get("order", 0))):
                    item["order"] = index
                result = {"id": member_id, "removed": True}
            else:
                raise _TrailFamilyError(400, "member action must be add or remove")
        family["revision"] += 1
        family["updated_at"] = _latent_note_ts()
        return {"family_id": family_id, "revision": family["revision"], "result": result}

    return _mutate_trail_families(mutate)


def _subcurrent_log_entry_from_note(note: dict) -> dict | None:
    if not isinstance(note, dict):
        return None
    note_id = str(note.get("id") or note.get("note_id") or "").strip()
    line = _latent_note_line(note)
    if not note_id or not line:
        return None
    at = (
        str(note.get("used_at") or "").strip()
        or str(note.get("last_delivered_at") or "").strip()
        or str(note.get("created_at") or "").strip()
        or _latent_note_ts()
    )
    try:
        delivered_count = int(note.get("delivered_count") or 0)
    except (TypeError, ValueError):
        delivered_count = 0
    return {
        "id": note_id,
        "line": line,
        "at": at,
        "last_at": str(note.get("last_delivered_at") or at),
        "drive_tag": _normalize_latent_drive_tag(note.get("drive_tag"), note.get("note_type")),
        "source_bucket_id": str(note.get("source_bucket_id") or "").strip(),
        "source_kind": str(note.get("source_kind") or note.get("source") or "").strip(),
        "source_title": str(note.get("source_title") or "").strip(),
        "source_fragment": str(note.get("source_fragment") or "").strip(),
        "note_type": _normalize_latent_note_type(note.get("note_type")),
        "delivered_count": max(0, delivered_count),
    }


def _upsert_subcurrent_log(note: dict) -> dict | None:
    """Permanent slim bone after delivery. Survives used+15d prune of latent pool."""
    entry = _subcurrent_log_entry_from_note(note)
    if not entry:
        return None
    data = _load_subcurrent_log()
    entries = data.get("entries") if isinstance(data.get("entries"), list) else []
    found = None
    for i, old in enumerate(entries):
        if str(old.get("id") or "") == entry["id"]:
            found = i
            break
    if found is None:
        entries.append(entry)
    else:
        prev = entries[found]
        # keep first-seen at as path origin time
        first_at = str(prev.get("at") or entry["at"])
        merged = dict(prev)
        merged.update(entry)
        merged["at"] = first_at
        for source_field in ("source_title", "source_fragment"):
            if not entry.get(source_field) and prev.get(source_field):
                merged[source_field] = prev[source_field]
        if not merged.get("line"):
            merged["line"] = entry["line"]
        entries[found] = merged
        entry = merged
    data["entries"] = entries
    _save_subcurrent_log(data)
    return entry


def _note_was_delivered(note: dict) -> bool:
    if not isinstance(note, dict):
        return False
    status = str(note.get("status") or "")
    try:
        delivered = int(note.get("delivered_count") or 0)
    except (TypeError, ValueError):
        delivered = 0
    return status == "used" or (status == "approved" and delivered > 0)


def _backfill_subcurrent_log_from_pool() -> int:
    """Promote still-in-pool delivered notes into permanent log (15d window rescue)."""
    try:
        pool = _load_latent_notes()
    except Exception:
        return 0
    data = _load_subcurrent_log()
    by_id: dict[str, dict] = {}
    for entry in data.get("entries", []):
        eid = str(entry.get("id") or "").strip()
        if eid:
            by_id[eid] = entry
    added = 0
    for note in pool.get("notes", []):
        if not _note_was_delivered(note):
            continue
        entry = _subcurrent_log_entry_from_note(note)
        if not entry or entry["id"] in by_id:
            continue
        by_id[entry["id"]] = entry
        added += 1
    if added:
        data["entries"] = list(by_id.values())
        _save_subcurrent_log(data)
    return added


def _iter_subcurrent_log_entries() -> list[dict]:
    """Permanent log entries (after optional pool backfill)."""
    try:
        _backfill_subcurrent_log_from_pool()
    except Exception as e:
        logger.warning(f"subcurrent log backfill failed: {e}")
    out = []
    for entry in _load_subcurrent_log().get("entries", []):
        eid = str(entry.get("id") or "").strip()
        if eid and str(entry.get("line") or "").strip():
            out.append(entry)
    return out


def _trail_term_weight(term: str) -> float:
    """Distinctive terms weigh more; short generic crumbs almost don't count."""
    t = (term or "").strip().lower()
    if not t:
        return 0.0
    if t in _TRAIL_STOPWORDS:
        return 0.0
    # codes / alnum tokens (6174, nla, residual…)
    if re.fullmatch(r"[a-z0-9][a-z0-9\-_]{1,24}", t):
        return 4.0 + min(6.0, float(len(t)))
    if len(t) >= 6:
        return 3.5 + min(5.0, float(len(t)) * 0.35)
    if len(t) >= 4:
        return 2.2 + float(len(t)) * 0.25
    if len(t) == 3:
        return 1.1
    # 2-char Chinese/other: weak alone, still usable with company
    return 0.55


def _trail_query_terms(raw_query: str) -> list[str]:
    raw = (raw_query or "").strip().lower()
    if not raw:
        return []
    parts = re.split(
        r"[\s,，、。；;：:！!？?\n/\\|·\-—_（）()【】\[\]\"'“”‘’《》<>+=]+",
        raw,
    )
    terms: list[str] = []
    for part in parts:
        p = part.strip()
        if len(p) < 2:
            continue
        if p in _TRAIL_STOPWORDS:
            continue
        # drop ultra-generic 2-char noise that floods Chinese matches
        if len(p) == 2 and p in {
            "一下", "一种", "这个", "那个", "什么", "不是", "可以", "没有",
            "自己", "我们", "他们", "因为", "所以", "如果", "还是", "已经",
            "就是", "还是", "以及", "或者", "然后", "只是", "而是", "关于",
            "对于", "这种", "那样", "这些", "那些", "时候", "现在", "今天",
            "之前", "之后", "有点", "比较", "东西", "地方", "问题", "感觉",
        }:
            continue
        terms.append(p)
    # longer / heavier first
    uniq: list[str] = []
    seen: set[str] = set()
    for t in sorted(set(terms), key=lambda x: (-_trail_term_weight(x), -len(x), x)):
        if t in seen:
            continue
        seen.add(t)
        uniq.append(t)
    if uniq:
        return uniq[:12]
    # single short query fallback (e.g. "6174")
    compact = re.sub(r"\s+", "", raw)
    return [compact[:48]] if len(compact) >= 2 else []


def _trail_match_score(
    haystack: str,
    terms: list[str],
    *,
    title: str = "",
    tags: str = "",
    require_strong: bool = False,
) -> float:
    """Weighted term hit. Prefer title/tag hits; reject weak single-crumb matches."""
    hay = (haystack or "").lower()
    title_l = (title or "").lower()
    tags_l = (tags or "").lower()
    if not hay or not terms:
        return 0.0

    score = 0.0
    hits = 0
    strong_hits = 0  # weight >= 2.0
    title_hits = 0
    for t in terms:
        w = _trail_term_weight(t)
        if w <= 0:
            continue
        in_body = t in hay
        in_title = bool(title_l) and t in title_l
        in_tags = bool(tags_l) and t in tags_l
        if not (in_body or in_title or in_tags):
            continue
        hits += 1
        if w >= 2.0:
            strong_hits += 1
        bonus = 0.0
        if in_title:
            title_hits += 1
            bonus += w * 0.85
        if in_tags:
            bonus += w * 0.55
        score += w + bonus

    if hits == 0:
        return 0.0

    # Gate: at least one strong term, OR two weaker hits, OR a title hit.
    # Blocks "声音" alone dragging unrelated memories into NLA trails.
    if strong_hits == 0 and title_hits == 0 and hits < 2:
        return 0.0
    if require_strong and strong_hits == 0 and title_hits == 0:
        return 0.0
    # Long query still needs breadth
    if len(terms) >= 5 and hits < 2 and strong_hits < 1:
        return 0.0
    return score


def _trail_short_anchor(text: str, max_len: int = TRAIL_ANCHOR_MAX) -> str:
    s = " ".join((text or "").split())
    if len(s) <= max_len:
        return s
    return s[: max(1, max_len - 1)].rstrip() + "…"


def _trail_evidence_span(
    text: str,
    terms: list[str],
    max_len: int = TRAIL_EVIDENCE_MAX,
) -> str:
    """Return complete evidence sentences; crop only an individually overlong sentence."""
    source = str(text or "")
    try:
        limit = max(1, int(max_len))
    except (TypeError, ValueError):
        limit = TRAIL_EVIDENCE_MAX
    clean_source = " ".join(source.split())
    fallback = clean_source[:limit].strip()
    normalized_terms = [str(term).strip().lower() for term in terms or [] if str(term).strip()]
    if not source or not normalized_terms:
        return fallback

    def _score(raw: str) -> tuple[float, int, int]:
        lowered = raw.lower()
        matched = [term for term in normalized_terms if term in lowered]
        return (
            sum(_trail_term_weight(term) for term in matched),
            len(matched),
            sum(lowered.count(term) for term in matched),
        )

    sentences: list[tuple[int, int, str]] = []
    sentence_pattern = (
        r"""[^。！？；.!?;\n]+"""
        r"""(?:[。！？；.!?;]+["'”’」』）》)\]}）]*|(?=\n|$))"""
    )
    for match in re.finditer(sentence_pattern, source):
        excerpt = " ".join(match.group(0).split())
        if excerpt:
            sentences.append((match.start(), match.end(), excerpt))

    complete_candidates = []
    long_sentences = []
    for start_index in range(len(sentences)):
        sentence_score = _score(sentences[start_index][2])
        if sentence_score[1] and len(sentences[start_index][2]) > limit:
            long_sentences.append((sentence_score, -sentences[start_index][0], sentences[start_index][2]))
        for end_index in range(start_index, min(len(sentences), start_index + 2)):
            start = sentences[start_index][0]
            end = sentences[end_index][1]
            excerpt = " ".join(source[start:end].split())
            score = _score(excerpt)
            if score[1] and len(excerpt) <= limit:
                complete_candidates.append((score, -len(excerpt), -start, excerpt))
    if not complete_candidates and not long_sentences:
        return ""

    best_complete = max(complete_candidates, key=lambda item: item[:3]) if complete_candidates else None
    best_long = max(long_sentences, key=lambda item: item[:2]) if long_sentences else None
    if best_complete and (not best_long or best_complete[0] >= best_long[0]):
        return best_complete[3]

    excerpt = best_long[2]
    lowered = excerpt.lower()
    occurrences = []
    for term in normalized_terms:
        occurrences.extend(
            (match.start(), match.end())
            for match in re.finditer(re.escape(term), lowered)
        )
    longest_match = max((end - start for start, end in occurrences), default=1)
    # Reserve room for both possible omission marks; never shorten a matched term.
    content_limit = max(longest_match, limit - 2)
    max_start = max(0, len(excerpt) - content_limit)
    starts = {0, max_start}
    for match_start, match_end in occurrences:
        starts.add(max(0, min(match_start - (content_limit - (match_end - match_start)) // 2, max_start)))

    windows = []
    for proposed_start in starts:
        start = proposed_start
        end = min(len(excerpt), start + content_limit)
        # If a boundary lands inside a query term, move the window rather than cut the term.
        for match_start, match_end in occurrences:
            if match_start < start < match_end:
                start = match_start
                end = min(len(excerpt), start + content_limit)
            if match_start < end < match_end:
                end = match_end
                start = max(0, end - content_limit)
        raw = excerpt[start:end].strip()
        window_center = (start + end) / 2
        center_distance = min(
            (abs(window_center - ((match_start + match_end) / 2)) for match_start, match_end in occurrences),
            default=0,
        )
        windows.append((_score(raw), -center_distance, -start, start, end, raw))
    _, _, _, start, end, raw = max(windows, key=lambda item: item[:3])
    # Exclude any secondary occurrence that a final boundary would only show partially.
    for match_start, match_end in occurrences:
        if match_start < start < match_end:
            start = match_end
        if match_start < end < match_end:
            end = match_start
    raw = excerpt[start:end].strip()
    prefix = "…" if start > 0 else ""
    suffix = "…" if end < len(excerpt) else ""
    return f"{prefix}{raw}{suffix}"


def _trail_bucket_kind(bucket: dict, mark_rows: list[dict]) -> str:
    meta = bucket.get("metadata", {}) or {}
    btype = str(meta.get("type", "")).lower()
    if btype == "feel":
        return "feel"
    if _is_unresolved_bucket(bucket, mark_rows):
        return "unresolved"
    domains = set(_bucket_domains(meta))
    tags = set(_bucket_tags(meta))
    for key in ("letter_human", "letter", "writing", "window", "inner"):
        if key in domains or key in tags:
            return key
    if _guess_wander_domain(bucket, mark_rows) == "inner":
        return "inner"
    return "memory"


def _trail_latest_mark(mark_rows: list[dict]) -> tuple[str | None, str | None]:
    if not mark_rows:
        return None, None
    rows = sorted(
        mark_rows,
        key=lambda x: (str(x.get("timestamp") or ""), int(x.get("id") or 0)),
        reverse=True,
    )
    latest = rows[0]
    mark = str(latest.get("mark") or "").strip() or None
    note = str(latest.get("note") or "").strip() or None
    mark_map = {"认": "affirm", "不认": "reject", "悬置": "suspend"}
    return mark_map.get(mark, mark), note


def _trail_mark_counts(mark_rows: list[dict]) -> dict[str, int]:
    out = {"affirm": 0, "reject": 0, "suspend": 0}
    for row in mark_rows or []:
        m = str(row.get("mark") or "").strip()
        if m == "认":
            out["affirm"] += 1
        elif m == "不认":
            out["reject"] += 1
        elif m == "悬置":
            out["suspend"] += 1
    return out


def _trail_node_sort_key(node: dict) -> tuple:
    at = str(node.get("at") or "")
    return (at, str(node.get("id") or ""))


def _trail_assign_roles(nodes: list[dict]) -> None:
    if not nodes:
        return
    for n in nodes:
        mark = n.get("mark")
        kind = str(n.get("kind") or "")
        if mark == "reject":
            n["role"] = "fork"
        elif kind == "latent":
            n["role"] = "echo"
        else:
            n["role"] = "hit"
    nodes[0]["role"] = "origin"
    if len(nodes) > 1:
        # keep fork if last is reject; else now
        if nodes[-1].get("mark") == "reject":
            nodes[-1]["role"] = "fork"
        else:
            nodes[-1]["role"] = "now"
        # first latent stays origin if it is first; subsequent latents echo
        latent_idxs = [i for i, n in enumerate(nodes) if n.get("kind") == "latent"]
        for i in latent_idxs[1:]:
            if nodes[i].get("role") not in ("now", "fork"):
                nodes[i]["role"] = "echo"


def _trail_label_from_query(query: str, terms: list[str]) -> str:
    if terms:
        label_terms = sorted(terms, key=len, reverse=True)[:3]
        # restore encounter order among top terms
        ordered = [t for t in terms if t in label_terms][:3]
        return " / ".join(ordered) if ordered else _trail_short_anchor(query, 40)
    return _trail_short_anchor(query, 40) or "trail"


async def _build_trail(query: str, limit: int = TRAIL_DEFAULT_LIMIT) -> dict:
    """Keyword(+optional embedding) family → chronological differential nodes. No summary essay."""
    q = (query or "").strip()
    try:
        lim = int(limit or TRAIL_DEFAULT_LIMIT)
    except (TypeError, ValueError):
        lim = TRAIL_DEFAULT_LIMIT
    lim = max(1, min(lim, TRAIL_MAX_LIMIT))
    empty = {
        "trail_id": None,
        "label": "",
        "query_echo": q,
        "node_count": 0,
        "span": {"first": "", "last": ""},
        "marks_summary": {"affirm": 0, "reject": 0, "suspend": 0},
        "nodes": [],
        "truncated": False,
    }
    if not q:
        return empty

    terms = _trail_query_terms(q)
    label = _trail_label_from_query(q, terms)
    query_key, normalized_query = _trail_query_identity(q)
    curations = _trail_curations_for_query(q)
    hidden_tombstones = sorted(
        ref for ref, row in curations.items()
        if isinstance(row, dict) and row.get("hidden") is True
    )
    curation_meta = {
        "query_key": query_key,
        "query": normalized_query,
        "hidden": hidden_tombstones,
        "order_id": hashlib.sha256(f"{query_key}\n".encode("utf-8")).hexdigest(),
    }
    candidates: dict[str, dict] = {}

    # --- latent delivery bones (slightly looser: short lines are the whole signal) ---
    for entry in _iter_subcurrent_log_entries():
        line = str(entry.get("line") or "")
        score = _trail_match_score(line, terms, title=line[:80])
        if score <= 0:
            continue
        eid = str(entry.get("id") or "")
        at = str(entry.get("at") or entry.get("last_at") or "")[:19]
        node = {
            "id": eid,
            "at": at[:10] if len(at) >= 10 else at,
            "kind": "latent",
            "role": "echo",
            "anchor": _trail_short_anchor(line),
            "quote": str(entry.get("source_fragment") or "").strip(),
            "mark": None,
            "mark_note": None,
            "signals": {
                "drive": entry.get("drive_tag") or "",
            },
            "ref": f"latent:{eid}",
            "_score": score,
            "_sort_at": at,
        }
        src = str(entry.get("source_bucket_id") or "").strip()
        if src:
            node["source_bucket_id"] = src
        source_title = str(entry.get("source_title") or "").strip()
        if source_title:
            node["source_title"] = source_title
        candidates[f"latent:{eid}"] = node

    # --- memory buckets (keyword; title/tags preferred) ---
    marks_by_bucket = _load_all_marks()
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=True)
    except Exception as e:
        logger.error(f"trails list buckets failed: {e}")
        all_buckets = []

    def _bucket_haystack(bucket: dict) -> str:
        meta = bucket.get("metadata", {}) or {}
        return "\n".join([
            str(bucket.get("id", "")),
            str(meta.get("name", "")),
            " ".join(str(x) for x in meta.get("domain", []) if x),
            " ".join(str(x) for x in meta.get("tags", []) if x),
            strip_wikilinks(bucket.get("content", "")),
        ])

    for bucket in all_buckets:
        bid = str(bucket.get("id") or "").strip()
        if not bid:
            continue
        mark_rows = marks_by_bucket.get(bid, [])
        if _is_private_bucket(bucket, mark_rows):
            continue
        meta = bucket.get("metadata", {}) or {}
        if meta.get("resolved") == 1 or meta.get("resolved") is True:
            continue
        if meta.get("digested") == 1 or meta.get("digested") is True:
            continue
        btype = str(meta.get("type", "")).lower()
        if btype in ("breath", "dream", "permanent"):
            continue
        title = str(meta.get("name") or "").strip()
        tags_s = " ".join(str(x) for x in meta.get("tags", []) if x)
        hay = _bucket_haystack(bucket)
        # body-only weak hits need stronger gate
        score = _trail_match_score(
            hay, terms, title=title, tags=tags_s, require_strong=True
        )
        if score <= 0:
            continue
        kind = _trail_bucket_kind(bucket, mark_rows)
        created = str(meta.get("created") or "")
        at = created[:10] if len(created) >= 10 else created[:19]
        content = strip_wikilinks(bucket.get("content", "")).strip()
        # strip leading date line like _format_wander_entry
        _date_line = re.match(
            r"^(?:写在开头\s*·?\s*)?20\d{2}[\.\-/]\d{1,2}[\.\-/]\d{1,2}[^\n]*\n+",
            content,
        )
        if _date_line:
            content = content[_date_line.end():]
        quote = _trail_evidence_span(content, terms)
        anchor = quote or title or _trail_short_anchor(tags_s)
        mark, mark_note = _trail_latest_mark(mark_rows)
        node = {
            "id": bid,
            "at": at,
            "kind": kind,
            "role": "hit",
            "title": title,
            "anchor": anchor,
            "quote": quote,
            "mark": mark,
            "mark_note": mark_note,
            "signals": {},
            "ref": f"bucket:{bid}",
            "_score": score,
            "_sort_at": created[:19] if created else at,
            "_mark_counts": _trail_mark_counts(mark_rows),
        }
        chord = meta.get("chord") or meta.get("drive")
        if chord:
            node["signals"]["chord"] = chord
        candidates[f"bucket:{bid}"] = node

    # --- optional embedding: only boost already-keyworded candidates (no pure soft orphans) ---
    try:
        if getattr(embedding_engine, "enabled", False) and q and candidates:
            similar = await embedding_engine.search_similar(q, top_k=8)
            for bid, sim in similar:
                if sim < 0.80:
                    continue
                key = f"bucket:{bid}"
                if key not in candidates:
                    # pure embedding neighbors tend to invent false origins; skip for v1 tighten
                    continue
                candidates[key]["_score"] = max(
                    float(candidates[key].get("_score") or 0),
                    float(sim) * 22.0,
                )
    except Exception as e:
        logger.warning(f"trails embedding soft-match skipped: {e}")

    # Exact-query curation overlay. It changes only this rendered path, never source data.
    for candidate_key, node in list(candidates.items()):
        ref = str(node.get("ref") or node.get("id") or "")
        overlay = curations.get(ref)
        if not isinstance(overlay, dict):
            continue
        if overlay.get("hidden") is True:
            candidates.pop(candidate_key, None)
            continue
        display_anchor = " ".join(str(overlay.get("display_anchor") or "").split())
        if display_anchor:
            node["original_anchor"] = node.get("anchor") or ""
            node["anchor"] = display_anchor[:TRAIL_ANCHOR_MAX]
            node["curated"] = True

    if not candidates:
        empty["label"] = label
        empty["curation"] = curation_meta
        return empty

    # rank by score then time, keep top pool then chronological display
    ranked = sorted(
        candidates.values(),
        key=lambda n: (-float(n.get("_score") or 0), str(n.get("_sort_at") or "")),
    )
    pool = ranked[: max(lim * 2, lim)]
    pool.sort(key=lambda n: (str(n.get("_sort_at") or ""), str(n.get("id") or "")))
    truncated = len(pool) > lim
    # if truncated, prefer keeping earliest origin + latest nodes
    if truncated and lim >= 3:
        head = pool[:1]
        tail = pool[-(lim - 1):]
        # dedupe if overlap
        seen_ids = {head[0].get("id")}
        nodes = list(head)
        for n in tail:
            if n.get("id") in seen_ids:
                continue
            nodes.append(n)
            seen_ids.add(n.get("id"))
        # if still short, fill from middle
        if len(nodes) < lim:
            for n in pool[1:]:
                if n.get("id") in seen_ids:
                    continue
                nodes.append(n)
                seen_ids.add(n.get("id"))
                if len(nodes) >= lim:
                    break
        nodes.sort(key=lambda n: (str(n.get("_sort_at") or ""), str(n.get("id") or "")))
    else:
        nodes = pool[:lim]
        truncated = len(ranked) > lim

    _trail_assign_roles(nodes)
    ordered_refs = [str(node.get("ref") or node.get("id") or "") for node in nodes]
    order_id = hashlib.sha256(
        f"{query_key}\n{chr(10).join(ordered_refs)}".encode("utf-8")
    ).hexdigest()
    curation_meta["order_id"] = order_id
    visible_refs = set(ordered_refs)
    for index, node in enumerate(nodes):
        current_predecessor_ref = ordered_refs[index - 1] if index > 0 else ""
        node["current_predecessor_ref"] = current_predecessor_ref
        overlay = curations.get(ordered_refs[index])
        claimed = overlay.get("delta_claimed") if isinstance(overlay, dict) else None
        if not isinstance(claimed, dict):
            continue
        baseline_ref = str(claimed.get("baseline_ref") or "")
        if not current_predecessor_ref:
            alignment = "no_current_predecessor"
        elif baseline_ref not in visible_refs:
            alignment = "baseline_not_visible"
        elif baseline_ref == current_predecessor_ref:
            alignment = "aligned"
        else:
            alignment = "shifted"
        node["delta_claimed"] = {
            "text": str(claimed.get("text") or ""),
            "baseline_ref": baseline_ref,
            "source": "manual",
            "basis_order_id": str(claimed.get("basis_order_id") or ""),
            "claimed_at": str(claimed.get("claimed_at") or ""),
            "updated_at": str(claimed.get("updated_at") or ""),
            "alignment": alignment,
            "current_predecessor_ref": current_predecessor_ref,
        }

    clean_nodes = []
    marks_summary = {"affirm": 0, "reject": 0, "suspend": 0}
    for n in nodes:
        clean = {
            "id": n.get("id"),
            "at": n.get("at") or "",
            "kind": n.get("kind"),
            "role": n.get("role"),
            "title": n.get("title") or "",
            "anchor": n.get("anchor") or "",
            "quote": n.get("quote") or "",
            "mark": n.get("mark"),
            "mark_note": n.get("mark_note"),
            "ref": n.get("ref"),
            "current_predecessor_ref": n.get("current_predecessor_ref") or "",
        }
        if isinstance(n.get("delta_claimed"), dict):
            clean["delta_claimed"] = n["delta_claimed"]
        if n.get("curated"):
            clean["curated"] = True
            clean["original_anchor"] = n.get("original_anchor") or ""
        sig = n.get("signals") if isinstance(n.get("signals"), dict) else {}
        sig = {k: v for k, v in sig.items() if v}
        if sig:
            clean["signals"] = sig
        if n.get("source_bucket_id"):
            clean["source_bucket_id"] = n["source_bucket_id"]
        if n.get("source_title"):
            clean["source_title"] = n["source_title"]
        clean_nodes.append(clean)
        m = clean.get("mark")
        if m in marks_summary:
            marks_summary[m] += 1

    first = clean_nodes[0]["at"] if clean_nodes else ""
    last = clean_nodes[-1]["at"] if clean_nodes else ""
    trail_id = hashlib.sha1(f"{label}|{first}|{last}|{len(clean_nodes)}".encode("utf-8")).hexdigest()[:12]

    return {
        "trail_id": trail_id,
        "label": label,
        "query_echo": _trail_short_anchor(q, 160),
        "node_count": len(clean_nodes),
        "span": {"first": first, "last": last},
        "marks_summary": marks_summary,
        "nodes": clean_nodes,
        "truncated": truncated,
        "curation": curation_meta,
    }


def _format_trail(trail: dict) -> str:
    if not isinstance(trail, dict) or not trail.get("nodes"):
        q = (trail or {}).get("query_echo") if isinstance(trail, dict) else ""
        return f"=== Trails ===\n没有找到与「{q or '…'}」相关的折痕路径。换个关键词，或这条路还没被走过。"

    label = trail.get("label") or "trail"
    span = trail.get("span") or {}
    ms = trail.get("marks_summary") or {}
    lines = [
        "=== Trails · 折痕路径 ===",
        f"议题：{label}",
        f"跨度：{span.get('first') or '?'} → {span.get('last') or '?'}"
        f" · 节点 {trail.get('node_count')}"
        f" · mark 认{ms.get('affirm', 0)}/不认{ms.get('reject', 0)}/悬{ms.get('suspend', 0)}",
        "（只读路径。不认用 wander_mark；路歪没歪自己说。不好奇就关掉。）",
        "────────────────────────────────",
    ]
    role_zh = {
        "origin": "origin",
        "hit": "hit",
        "fork": "fork",
        "echo": "echo",
        "now": "now",
    }
    for n in trail.get("nodes") or []:
        role = role_zh.get(str(n.get("role") or ""), n.get("role") or "?")
        kind = n.get("kind") or "?"
        at = n.get("at") or "?"
        mark = n.get("mark")
        mark_s = f" · mark:{mark}" if mark else ""
        if n.get("mark_note"):
            mark_s += f"「{_trail_short_anchor(str(n.get('mark_note')), 40)}」"
        lines.append(f"[{at}] ({role}/{kind}){mark_s}")
        if n.get("curated"):
            lines.append(f"  人工显示摘要：{n.get('anchor') or ''}")
            lines.append(f"  原始显示：{n.get('original_anchor') or ''}")
        else:
            lines.append(f"  {n.get('anchor') or ''}")
        anchor_norm = " ".join(str(n.get("anchor") or "").split())
        original_norm = " ".join(str(n.get("original_anchor") or "").split())
        quote_norm = " ".join(str(n.get("quote") or "").split())
        evidence_norm = original_norm or anchor_norm
        if n.get("curated") and quote_norm:
            lines.append(f"  原句：{n.get('quote')}")
        elif kind == "latent" and quote_norm and quote_norm != evidence_norm:
            lines.append(f"  原句：{n.get('quote')}")
        delta = n.get("delta_claimed")
        if isinstance(delta, dict) and delta.get("text"):
            baseline = delta.get("baseline_ref") or "?"
            lines.append(
                f"  Δ（人工认领；比较基线 {baseline}；非因果）：{delta.get('text')}"
            )
            alignment = delta.get("alignment")
            if alignment and alignment != "aligned":
                current = delta.get("current_predecessor_ref") or "无"
                lines.append(f"  ⚠ Δ基线漂移：{alignment}；当前前驱 {current}")
        lines.append(f"  ref:{n.get('ref') or n.get('id')}")
    if trail.get("truncated"):
        lines.append("────────────────────────────────")
        lines.append("…还有更早/更密的节被裁掉了。加大 limit 或收窄 query。")
    return "\n".join(lines)


@mcp.tool(name="trail_delta")
async def trail_delta(
    action: Literal["claim", "clear"],
    query: str,
    node_ref: str,
    text: str = "",
    baseline_ref: str = "",
    limit: int = TRAIL_DEFAULT_LIMIT,
) -> str:
    """Agent人工认领/清除一个Trail节点差分；它只是与基线的比较，不表示因果。

    claim 会读取当前可见 Trail 顺序并自动绑定当前前驱（也可显式指定当前可见
    baseline_ref），调用者无需提供 order_id。clear 只清 overlay，不读取 buckets。
    原 bucket、latent、source evidence 永远不被修改。
    """
    act = str(action or "").strip().lower()
    q = str(query or "").strip()
    ref = str(node_ref or "").strip()
    if act not in {"claim", "clear"}:
        return "trail_delta 拒绝：action 必须是 claim 或 clear。"
    if act == "clear":
        try:
            _validate_trail_delta_payload({
                "query": q,
                "node_ref": ref,
                "action": "clear",
                "text": "",
                "baseline_ref": "",
                "basis_order_id": "",
            })
        except ValueError as e:
            return f"trail_delta 拒绝：{e}"
        try:
            result = _update_trail_delta(q, ref, "clear")
        except Exception:
            logger.exception("trail_delta clear store failed")
            return "trail_delta 暂时无法写入；overlay 原文件未改。"
        return f"trail_delta 已清除 · node:{result['node_ref']}"

    try:
        trail = await _build_trail(q, limit=limit)
    except Exception:
        logger.exception("trail_delta build failed")
        return "trail_delta 暂时无法读取当前 Trail。"
    curation = trail.get("curation") if isinstance(trail, dict) else {}
    normalized_query = str((curation or {}).get("query") or q)
    order_id = str((curation or {}).get("order_id") or "")
    nodes = trail.get("nodes") if isinstance(trail, dict) else []
    nodes = nodes if isinstance(nodes, list) else []
    by_ref = {
        str(node.get("ref") or ""): node
        for node in nodes
        if isinstance(node, dict) and node.get("ref")
    }
    node = by_ref.get(ref)
    if node is None:
        return f"trail_delta 拒绝：node {ref or '?'} 不在当前可见 Trail。"
    explicit_baseline = str(baseline_ref or "").strip()
    baseline = explicit_baseline or str(node.get("current_predecessor_ref") or "")
    if not baseline:
        return "trail_delta 拒绝：origin 节点没有当前前驱，不能认领 Δ。"
    if baseline == ref:
        return "trail_delta 拒绝：baseline 不能与 node 相同。"
    if explicit_baseline and baseline not in by_ref:
        return f"trail_delta 拒绝：baseline {baseline} 不在当前可见 Trail。"
    try:
        _validate_trail_delta_payload({
            "query": normalized_query,
            "node_ref": ref,
            "action": "claim",
            "text": text,
            "baseline_ref": baseline,
            "basis_order_id": order_id,
        })
    except ValueError as e:
        return f"trail_delta 拒绝：{e}"
    try:
        result = _update_trail_delta(
            normalized_query,
            ref,
            "claim",
            text,
            baseline,
            order_id,
        )
    except Exception:
        logger.exception("trail_delta claim store failed")
        return "trail_delta 暂时无法写入；overlay 原文件未改。"
    return (
        f"trail_delta 已认领（人工比较，非因果） · node:{result['node_ref']}"
        f" · baseline:{baseline} · order:{order_id[:12]}"
    )


@mcp.tool(name="trail_family")
async def trail_family(
    action: Literal[
        "list", "read", "create", "update", "save_query",
        "add_member", "remove_member", "delete",
    ],
    family_id: str = "",
    title: Optional[str] = None,
    core_question: Optional[str] = None,
    query: str = "",
    node_ref: str = "",
    member_id: str = "",
    label: str = "",
    limit: int = TRAIL_DEFAULT_LIMIT,
) -> str:
    """Agent手动管理Trail Families；只认明确query/ref，不聚类、不建议、不注入召回。

    add_member 会读取当前 visible Trail 来锁定 query/order/observed_at；其他读取与
    编排只访问独立 Families store。所有修改都保留原 bucket/latent/Trail。
    """
    act = str(action or "").strip().lower()
    fid = str(family_id or "").strip()

    def read_store():
        try:
            return _load_trail_families(strict=True)
        except Exception:
            logger.exception("trail_family store read failed")
            return None

    def family_from(data):
        family = (data or {}).get("families", {}).get(fid)
        return family if isinstance(family, dict) else None

    def mutation_result(call):
        try:
            return call(), ""
        except _TrailFamilyError as e:
            if e.status == 409:
                return None, f"trail_family 冲突：{e}；未自动重试。"
            if e.status in {400, 404}:
                return None, f"trail_family 拒绝：{e}"
            return None, f"trail_family 写入失败：{e}"
        except Exception:
            logger.exception("trail_family store mutation failed")
            return None, "trail_family 暂时无法写入；Families 原文件未改。"

    if act == "list":
        data = read_store()
        if data is None:
            return "trail_family 暂时无法读取 Families。"
        rows = sorted(
            data["families"].values(),
            key=lambda family: (family.get("created_at", ""), family["id"]),
        )
        if not rows:
            return "trail_family · 还没有 Family。"
        return "\n".join(
            f"{family['id']} · {family.get('title') or 'Untitled'}"
            f" · {len(family['members'])} refs / {len(family['query_entries'])} queries"
            f" · rev {family['revision']}"
            for family in rows
        )

    if act == "read":
        data = read_store()
        if data is None:
            return "trail_family 暂时无法读取 Families。"
        family = family_from(data)
        if family is None:
            return "trail_family 拒绝：family not found"
        lines = [
            f"{family['id']} · {family['title']} · rev {family['revision']}",
            f"核心问题：{family.get('core_question') or '（空）'}",
            "query entries:",
        ]
        lines.extend(
            f"- {entry['id']} · {entry['label']} · {entry['query']}"
            for entry in family["query_entries"]
        )
        lines.append("members:")
        lines.extend(
            f"- {member['id']} · {member['node_ref']} · from {member['query']}"
            for member in sorted(family["members"], key=lambda row: row["order"])
        )
        return "\n".join(lines)

    if act == "create":
        result, error = mutation_result(lambda: _mutate_trail_family({
            "action": "create", "title": title or "", "core_question": core_question or "",
        }))
        return error or f"trail_family 已创建 · {result['id']} · rev {result['revision']}"

    if act not in {"update", "save_query", "add_member", "remove_member", "delete"}:
        return "trail_family 拒绝：未知 action。"

    if act == "add_member":
        q = str(query or "").strip()
        ref = str(node_ref or "").strip()
        try:
            trail = await _build_trail(q, limit=limit)
        except Exception:
            logger.exception("trail_family add_member trail build failed")
            return "trail_family 暂时无法读取当前 Trail。"
        curation = trail.get("curation") if isinstance(trail, dict) else {}
        normalized_query = str((curation or {}).get("query") or "")
        query_key = str((curation or {}).get("query_key") or "")
        order_id = str((curation or {}).get("order_id") or "")
        node = next(
            (
                row for row in (trail.get("nodes") or [])
                if isinstance(row, dict) and str(row.get("ref") or "") == ref
            ),
            None,
        )
        if node is None:
            return "trail_family 拒绝：node 不在当前 visible Trail。"
        if not normalized_query or not query_key or not _TRAIL_ORDER_ID_RE.fullmatch(order_id):
            return "trail_family 拒绝：当前 Trail 缺少稳定 query/order provenance。"

        data = read_store()
        if data is None:
            return "trail_family 暂时无法读取 Families。"
        family = family_from(data)
        if family is None:
            return "trail_family 拒绝：family not found"
        existing_member = next(
            (member for member in family["members"] if member["node_ref"] == ref),
            None,
        )
        if existing_member:
            return f"trail_family 成员已存在 · {existing_member['id']} · {ref}"
        entry = next(
            (entry for entry in family["query_entries"] if entry["query_key"] == query_key),
            None,
        )
        revision = family["revision"]
        if entry is None:
            entry_result, error = mutation_result(
                lambda: _mutate_trail_family_entry(fid, {
                    "action": "add",
                    "expected_revision": revision,
                    "query": normalized_query,
                    "label": label or trail.get("label") or normalized_query,
                })
            )
            if error:
                return error
            entry = entry_result["result"]
            revision = entry_result["revision"]
        member_result, error = mutation_result(
            lambda: _mutate_trail_family_member(fid, {
                "action": "add",
                "expected_revision": revision,
                "query_entry_id": entry["id"],
                "node_ref": ref,
                "observed_at": str(node.get("at") or ""),
                "source_order_id": order_id,
                "manual_note": "",
            })
        )
        if error:
            return error
        member = member_result["result"]
        return (
            f"trail_family 已加入成员 · {member['id']} · {ref}"
            f" · query:{normalized_query} · order:{order_id[:12]}"
        )

    data = read_store()
    if data is None:
        return "trail_family 暂时无法读取 Families。"
    family = family_from(data)
    if family is None:
        return "trail_family 拒绝：family not found"
    revision = family["revision"]

    if act == "update":
        if title is None and core_question is None:
            return "trail_family 拒绝：update 至少提供 title 或 core_question。"
        body = {
            "action": "update", "family_id": fid, "expected_revision": revision,
        }
        if title is not None:
            body["title"] = title
        if core_question is not None:
            body["core_question"] = core_question
        result, error = mutation_result(lambda: _mutate_trail_family(body))
        return error or f"trail_family 已更新 · {fid} · rev {result['revision']}"

    if act == "delete":
        result, error = mutation_result(lambda: _mutate_trail_family({
            "action": "delete", "family_id": fid, "expected_revision": revision,
        }))
        return error or f"trail_family 已删除 · {fid}"

    if act == "save_query":
        normalized_key, normalized_query = _trail_query_identity(query)
        if not normalized_query:
            return "trail_family 拒绝：query required"
        existing = next(
            (entry for entry in family["query_entries"] if entry["query_key"] == normalized_key),
            None,
        )
        if existing:
            return f"trail_family query 已存在 · {existing['id']} · {existing['query']}"
        result, error = mutation_result(
            lambda: _mutate_trail_family_entry(fid, {
                "action": "add", "expected_revision": revision,
                "query": normalized_query, "label": label or normalized_query,
            })
        )
        if error:
            return error
        entry = result["result"]
        return f"trail_family query 已保存 · {entry['id']} · {entry['query']}"

    target = next(
        (member for member in family["members"] if member["id"] == str(member_id or "").strip()),
        None,
    )
    if target is None:
        return "trail_family 拒绝：member not found"
    result, error = mutation_result(
        lambda: _mutate_trail_family_member(fid, {
            "action": "remove", "expected_revision": revision,
            "member_id": target["id"],
        })
    )
    return error or f"trail_family 已移除成员 · {target['id']} · {target['node_ref']}"


def _latent_note_api_config() -> tuple[str, str, str]:
    api_key = (
        os.environ.get("LATENT_NOTE_API_KEY")
        or os.environ.get("SPEECH_EVENT_API_KEY")
        or os.environ.get("DEEPSEEK_API_KEY", "")
    )
    base_url = os.environ.get("LATENT_NOTE_BASE_URL", "https://api.deepseek.com/v1").rstrip("/")
    model = os.environ.get("LATENT_NOTE_MODEL", "deepseek-v4-flash")
    return api_key, base_url, model


_LATENT_OUTWARD_MARKERS = (
    "？", "?", "为什么", "怎么", "是什么", "叫什么", "能不能", "会不会", "有没有",
    "不确定", "查", "论坛", "X", "模型", "AI", "意识", "机制", "阈值", "代码", "引擎",
    "神经", "皮肤", "温度", "颜色", "蓝色", "光", "鲸", "白鲸", "章鱼", "论文", "系统",
)
_LATENT_INWARD_MARKERS = (HUMAN_NAME, AGENT_NAME, "爱", "哭", "心", "抱", "喜欢", "想你", "名字")


def _latent_outward_score(text: str) -> int:
    score = 0
    for marker in _LATENT_OUTWARD_MARKERS:
        if marker in text:
            score += 2 if marker in ("？", "?", "不确定", "为什么", "怎么", "能不能", "会不会", "查") else 1
    for marker in _LATENT_INWARD_MARKERS:
        if marker in text:
            score -= 1
    return score


def _latent_source_fragments(bucket: dict, max_fragments: int = 3) -> list[str]:
    import re as _re
    content = strip_wikilinks(bucket.get("content", "")).strip()
    content = _re.sub(r"^(?:写在开头\s*·?\s*)?20\d{2}[\.\-/]\d{1,2}[\.\-/]\d{1,2}[^\n]*\n+", "", content)
    raw_parts = [
        p.strip(" \t\r\n-—·")
        for p in _re.split(r"[\n。！？!?]+", content)
        if p.strip(" \t\r\n-—·")
    ]
    parts = []
    seen = set()
    for part in raw_parts:
        part = " ".join(part.split())
        if len(part) < 12 or part in seen:
            continue
        if part.startswith("写在开头") or part in {f"致下一个{AGENT_NAME}", "致下一个我"}:
            continue
        if any(marker in part for marker in ("输出格式", "操作性的提醒", "不是讲道理", "总结如下")):
            continue
        seen.add(part)
        parts.append(part[:120])
    if not parts:
        anchor = _latent_anchor(bucket)
        return [anchor] if anchor else []

    def texture_score(text: str) -> int:
        inward = (HUMAN_NAME, AGENT_NAME, "还没", "悬", "梦", "手", "眼", "疼", "想", "记得", "那时候", "以前")
        return (
            sum(1 for needle in inward if needle in text)
            + max(0, _latent_outward_score(text))
            + min(len(text), 80) // 30
        )

    parts.sort(key=texture_score, reverse=True)
    return parts[:max_fragments]


def _latent_source_item(bucket: dict, mark_rows: list[dict], kind: str, score: float) -> dict:
    meta = bucket.get("metadata", {})
    counts = _mark_counts(mark_rows)
    recent_note = next(
        (
            str(row.get("note", "")).strip()
            for row in sorted(mark_rows, key=lambda x: (x.get("timestamp", ""), x.get("id", 0)), reverse=True)
            if str(row.get("note", "")).strip()
        ),
        "",
    )
    item = {
        "bucket_id": bucket.get("id", ""),
        "kind": kind,
        "score": round(score, 3),
        "title": str(meta.get("name") or "").strip(),
        "created": str(meta.get("created", "") or "")[:10],
        "domain": meta.get("domain", []),
        "tags": meta.get("tags", []),
        "marks": {"认": counts["认"], "不认": counts["不认"], "悬置": counts["悬置"]},
        "latest_mark_note": recent_note,
        "fragments": _latent_source_fragments(bucket),
    }
    item["outward_score"] = _latent_item_outward_score(item)
    return item


def _latent_item_outward_score(item: dict) -> int:
    fields = [
        str(item.get("title", "")),
        str(item.get("latest_mark_note", "")),
        " ".join(str(x) for x in item.get("tags", []) if x),
        " ".join(str(x) for x in item.get("fragments", []) if x),
    ]
    return max(_latent_outward_score(text) for text in fields if text) if any(fields) else 0


async def _collect_latent_source_items(limit: int = 24) -> list[dict]:
    all_buckets = await bucket_mgr.list_all(include_archive=True)
    marks_by_bucket = _load_all_marks()
    now = datetime.now()
    strong: list[dict] = []
    fallback: list[dict] = []
    for bucket in all_buckets:
        bucket_id = bucket.get("id", "")
        if not bucket_id:
            continue
        mark_rows = marks_by_bucket.get(bucket_id, [])
        scored = _latent_candidate_score(bucket, mark_rows, now)
        if not scored:
            continue
        kind, score = scored
        if score <= 0:
            continue
        item = _latent_source_item(bucket, mark_rows, kind, score)
        if not item.get("fragments"):
            continue
        if kind == "old_memory":
            fallback.append(item)
        else:
            strong.append(item)
    pool = strong or fallback
    pool.sort(key=lambda x: x.get("score", 0), reverse=True)
    top_pool = pool[: min(len(pool), max(limit, limit * 2))]
    random.shuffle(top_pool)
    return top_pool[:limit]


def _clean_json_content(content: str) -> dict:
    content = (content or "").strip()
    if content.startswith("```"):
        content = "\n".join(content.splitlines()[1:]).replace("```", "").strip()
    start = content.find("{")
    end = content.rfind("}") + 1
    if start >= 0 and end > start:
        content = content[start:end]
    return _json_lib.loads(content)


async def _generate_latent_note_drafts(count: int = 10, prefer_low_stock: bool = True) -> dict:
    api_key, base_url, model = _latent_note_api_config()
    if not api_key:
        raise RuntimeError("LATENT_NOTE_API_KEY/SPEECH_EVENT_API_KEY/DEEPSEEK_API_KEY is not set")
    count = max(1, min(int(count or 10), 50))
    stock = _approved_latent_stock_by_drive()
    low_rows = _latent_low_stock_drives() if prefer_low_stock else []
    # 配额：只往快空的真实 Drive 补，永不分给 general
    drive_quota: dict[str, int] = {}
    if low_rows:
        # 先给最空的维各保底 1，再按缺口把剩余名额摊开
        remaining = count
        for tag, _cnt, _deficit in low_rows:
            if remaining <= 0:
                break
            drive_quota[tag] = 1
            remaining -= 1
        total_deficit = sum(max(1, d) for _, _, d in low_rows) or 1
        for tag, _cnt, deficit in low_rows:
            if remaining <= 0:
                break
            extra = min(remaining, max(0, round(count * max(1, deficit) / total_deficit) - 1))
            if extra > 0:
                drive_quota[tag] = drive_quota.get(tag, 0) + extra
                remaining -= extra
        if remaining > 0 and low_rows:
            # 余数优先砸给绝对库存最少的维
            for tag, _cnt, _d in low_rows:
                if remaining <= 0:
                    break
                drive_quota[tag] = drive_quota.get(tag, 0) + 1
                remaining -= 1
    else:
        # 全满：均匀轻补日场三件套
        personal = ["curiosity", "stewardship", "reflection"]
        base = count // len(personal)
        extra = count % len(personal)
        for i, tag in enumerate(personal):
            drive_quota[tag] = base + (1 if i < extra else 0)
    drive_quota.pop("general", None)

    sources = await _collect_latent_source_items(limit=max(20, min(count * 5, 100)))
    if not sources:
        return {
            "generated": [],
            "source_count": 0,
            "drive_quota": drive_quota,
            "stock_before": stock,
            "low_stock": [{"drive": t, "approved": c, "deficit": d} for t, c, d in low_rows],
        }
    outward_sources = [s for s in sources if int(s.get("outward_score", 0) or 0) >= 2]
    inward_sources = [s for s in sources if s not in outward_sources]
    # outward 配额只给真正外向维（不含 general）
    outward_tags = {"curiosity", "social"}
    outward_target = min(
        sum(q for t, q in drive_quota.items() if t in outward_tags),
        len(outward_sources),
        count,
    )
    inward_target = count - outward_target
    if len(inward_sources) < inward_target:
        inward_target = len(inward_sources)
        outward_target = min(count - inward_target, len(outward_sources))
    source_payload = {
        "inward_sources": inward_sources[: max(inward_target * 3, 8)],
        "outward_sources": outward_sources[: max(outward_target * 3, 8)],
    }
    quota_lines = [
        f"{tag}: 写 {n} 条（当前 approved={stock.get(tag, 0)}）"
        for tag, n in sorted(drive_quota.items(), key=lambda x: -x[1])
        if n > 0
    ]

    system_prompt = (
        f"{AGENT_PERSONA}\n"
        f"你是潜意识便签写手，给 {AGENT_NAME} 写短纸片。"
        "你只把旧记忆碎片压成梦里翻到的一句话，不解释，不建议，不总结主题。"
        "只输出 JSON。"
    )
    user_prompt = _json_lib.dumps(
        {
            "task": (
                f"写约 {count} 条潜意识便签草稿（inward 约 {inward_target}，outward 约 {outward_target}）。"
                "必须优先补库存将空的 drive 池，不要往已经很多的维继续堆。"
            ),
            "drive_quota": drive_quota,
            "drive_quota_human": quota_lines,
            "stock_before": stock,
            "note_types": {
                "inward": f"情感残片，让 {AGENT_NAME} 停一下；可以有重量，但不要变成格言。",
                "outward": f"悬置问题、好奇心碎片、未查完的事实或系统/世界断点，把 {AGENT_NAME} 往外推；不是行动命令。",
            },
            "drive_tags": {
                "allowed": sorted(VALID_LATENT_NOTE_DRIVES),
                "priority": [t for t, n in sorted(drive_quota.items(), key=lambda x: -x[1]) if n > 0],
                "hint": (
                    "drive_tag 必须严格落在 drive_quota / priority 列表里，且数量尽量对齐配额。"
                    "禁止使用 general。不要给不在配额里的维写。"
                    "含义：attachment 靠近，libido 身体热度，possessiveness 领地，"
                    "reflection 回看，stewardship 守屋，curiosity 向外看，"
                    "social 向外说/发帖/被看见，fatigue 没电/歇着，stress 绷紧。"
                ),
            },
            "rules": [
                "dream_line 25-60 个中文字符，短，像梦里翻到的一张纸。",
                "保留原句里的具体物、动作、语气；优先使用 source fragments 的原句碎片。",
                "不要写成'关于xxx'、'存在感的问题'、'连续性的主题'这类抽象总结。",
                "不要出现 可以、应该、去、wander、任务、建议、提醒。",
                "不要写格言、结论、辩论句；少用'不是...而是...'、'X比Y更...'、'所以...'。",
                "不要把标题、章节名、写作说明压成便签；source_fragment 必须是正文里的具体句子。",
                "dream_line 里至少保留一个具体物、身体动作、场景物件或原句里的奇怪词。",
                "允许不完整，允许有悬念，不要把逻辑补圆。",
                f"不要说教，不要解释意义，不要给 {AGENT_NAME} 安排行动。",
                "source_fragment 必须从对应 source.fragments 中选一句或截取一句。",
                "每条只可使用一个 source，不要混合多个记忆。",
                "每条 drive_tag 必须在 drive_quota 里；禁止 general；尽量凑满各维配额条数。",
                "social 维写：想说话、发帖、被看见、对外表达的碎片，不是纯看世界（那是 curiosity）。",
                "fatigue 维写：没电、歇着、输出成本高、懒得动的碎片。",
                "outward 不是'去查xxx'，而是像'章鱼的神经末梢分布到皮肤上，不确定能不能感到温度'这种没闭合的事实碎片。",
                f"outward 必须从 outward_sources 里选；不要把 {HUMAN_NAME}/{AGENT_NAME}/爱/哭这类情感片段硬写成 outward。",
                "outward 需要保留一个可追的断点：问号、不确定、机制、阈值、模型差异、外部事实、代码行为、论坛/X线索等。",
                "如果 source 里没有足够 outward 材料，就少写 outward，不要硬编外部知识。",
            ],
            "output_schema": {
                "notes": [
                    {
                        "note_type": "inward or outward",
                        "drive_tag": "one of drive_quota keys only, never general",
                        "source_bucket_id": "bucket id from source",
                        "source_fragment": "copied or trimmed original source fragment",
                        "dream_line": "潜意识便签",
                    }
                ]
            },
            **source_payload,
        },
        ensure_ascii=False,
    )
    async with httpx.AsyncClient(timeout=45.0) as client:
        resp = await client.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "temperature": 0.85,
                "max_tokens": min(3500, 420 + count * 190),
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()
    parsed = _clean_json_content(content)
    raw_notes = parsed.get("notes", [])
    if not isinstance(raw_notes, list):
        raw_notes = []

    source_by_id = {item["bucket_id"]: item for item in sources}
    generated = []
    ts = now_iso()
    # 需要补货的维按剩余配额排队，模型乱标时往空池拨
    remaining_quota = {t: int(n) for t, n in drive_quota.items() if int(n) > 0}
    priority_tags = [t for t, _ in sorted(remaining_quota.items(), key=lambda x: -x[1])]

    def _assign_drive_tag(raw_tag: str, note_type: str) -> str:
        tag = _normalize_latent_drive_tag(raw_tag, note_type)
        if tag == "general":
            tag = ""
        if tag in remaining_quota and remaining_quota[tag] > 0:
            remaining_quota[tag] -= 1
            if remaining_quota[tag] <= 0:
                remaining_quota.pop(tag, None)
            return tag
        # 模型写到已满/未请求/general → 拨给仍缺货的维（永不落 general）
        for need in list(priority_tags):
            if need == "general":
                continue
            if need in remaining_quota and remaining_quota[need] > 0:
                remaining_quota[need] -= 1
                if remaining_quota[need] <= 0:
                    remaining_quota.pop(need, None)
                return need
        # 配额已耗尽：落在 priority 第一维，仍不要 general
        fallback = next((t for t in priority_tags if t != "general"), "reflection")
        return fallback if fallback != "general" else "reflection"

    for raw in raw_notes[:count]:
        if not isinstance(raw, dict):
            continue
        bucket_id = str(raw.get("source_bucket_id") or "").strip()
        dream_line = " ".join(str(raw.get("dream_line") or "").split())
        source_fragment = " ".join(str(raw.get("source_fragment") or "").split())
        source = source_by_id.get(bucket_id)
        if not bucket_id or not dream_line or not source:
            continue
        note_type = str(raw.get("note_type") or "").strip().lower()
        if note_type not in {"inward", "outward"}:
            note_type = "outward" if any(x in dream_line for x in ("？", "?", "不确定", "叫什么", "为什么", "怎么", "查")) else "inward"
        drive_tag = _assign_drive_tag(raw.get("drive_tag"), note_type)
        note_id = "latent_" + hashlib.sha1(f"{bucket_id}|{dream_line}|{ts}".encode("utf-8")).hexdigest()[:16]
        generated.append({
            "id": note_id,
            "status": "draft",
            "pinned": False,
            "note_type": note_type,
            "drive_tag": drive_tag,
            "source_bucket_id": bucket_id,
            "source_kind": source.get("kind"),
            "source_title": source.get("title"),
            "source_created": source.get("created"),
            "source_score": source.get("score"),
            "source_wander_mode": source.get("wander_mode"),
            "source_marks": source.get("marks", {}),
            "source_outward_score": source.get("outward_score", 0),
            "source_fragment": source_fragment,
            "dream_line": dream_line,
            "model": model,
            "created_at": ts,
            "updated_at": ts,
        })
    return {
        "generated": generated,
        "source_count": len(sources),
        "inward_source_count": len(inward_sources),
        "outward_source_count": len(outward_sources),
        "inward_target": inward_target,
        "outward_target": outward_target,
        "model": model,
        "drive_quota": drive_quota,
        "stock_before": stock,
        "low_stock": [{"drive": t, "approved": c, "deficit": d} for t, c, d in low_rows],
    }


try:
    _init_marks_table()
except Exception as e:
    logger.warning(f"Failed to initialize wander marks table: {e}")


# =============================================================
# Headless API auth — simple cookie-based session auth
# 无界面 API 认证 —— 基于 Cookie 的会话认证
#
# OMBRE_API_PASSWORD is preferred. OMBRE_DASHBOARD_PASSWORD remains a
# compatibility alias for older installations.
# Sessions stored in memory (lost on restart, 7-day expiry).
# =============================================================
_sessions: dict[str, float] = {}  # {token: expiry_timestamp}
_dream_refresh_lock = asyncio.Lock()


def _get_auth_file() -> str:
    return os.path.join(config["buckets_dir"], ".api_auth.json")


def _configured_api_password() -> str:
    return (
        os.environ.get("OMBRE_API_PASSWORD", "")
        or os.environ.get("OMBRE_DASHBOARD_PASSWORD", "")
    )


def _load_password_hash() -> str | None:
    try:
        auth_file = _get_auth_file()
        if os.path.exists(auth_file):
            with open(auth_file, "r", encoding="utf-8") as f:
                return _json_lib.load(f).get("password_hash")
    except Exception:
        pass
    return None


def _save_password_hash(password: str) -> None:
    salt = secrets.token_hex(16)
    h = hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
    auth_file = _get_auth_file()
    os.makedirs(os.path.dirname(auth_file), exist_ok=True)
    with open(auth_file, "w", encoding="utf-8") as f:
        _json_lib.dump({"password_hash": f"{salt}:{h}"}, f)


def _verify_password_hash(password: str, stored: str) -> bool:
    if ":" not in stored:
        return False
    salt, h = stored.split(":", 1)
    return hmac.compare_digest(
        h, hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
    )


def _is_setup_needed() -> bool:
    """True if no password is configured (env var or file)."""
    if _configured_api_password():
        return False
    return _load_password_hash() is None


def _verify_any_password(password: str) -> bool:
    """Check password against env var (first) or stored hash."""
    env_pwd = _configured_api_password()
    if env_pwd:
        return hmac.compare_digest(password, env_pwd)
    stored = _load_password_hash()
    if not stored:
        return False
    return _verify_password_hash(password, stored)


def _create_session() -> str:
    token = secrets.token_urlsafe(32)
    _sessions[token] = time.time() + 86400 * 7  # 7-day expiry
    return token


def _is_authenticated(request) -> bool:
    token = request.cookies.get("ombre_session")
    if not token:
        return False
    expiry = _sessions.get(token)
    if expiry is None or time.time() > expiry:
        _sessions.pop(token, None)
        return False
    return True


def _is_service_authenticated(request) -> bool:
    expected = os.environ.get("OMBRE_SERVICE_TOKEN", "").strip()
    if len(expected) < 32:
        return False
    supplied = str(request.headers.get("authorization") or "")
    if supplied.lower().startswith("bearer "):
        supplied = supplied[7:].strip()
    return bool(supplied) and hmac.compare_digest(supplied, expected)


def _require_auth(request, allow_service: bool = False):
    """Return JSONResponse(401) if not authenticated, else None."""
    from starlette.responses import JSONResponse
    if not _is_authenticated(request) and not (allow_service and _is_service_authenticated(request)):
        return JSONResponse(
            {"error": "Unauthorized", "setup_needed": _is_setup_needed()},
            status_code=401,
        )
    return None


# --- Auth endpoints ---
@mcp.custom_route("/auth/status", methods=["GET"])
async def auth_status(request):
    """Return auth state (authenticated, setup_needed)."""
    from starlette.responses import JSONResponse
    return JSONResponse({
        "authenticated": _is_authenticated(request),
        "setup_needed": _is_setup_needed(),
    })


@mcp.custom_route("/auth/setup", methods=["POST"])
async def auth_setup_endpoint(request):
    """Initial password setup (only when no password is configured)."""
    from starlette.responses import JSONResponse
    if not _is_setup_needed():
        return JSONResponse({"error": "Already configured"}, status_code=400)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    password = body.get("password", "").strip()
    if len(password) < 6:
        return JSONResponse({"error": "密码不能少于6位"}, status_code=400)
    _save_password_hash(password)
    token = _create_session()
    resp = JSONResponse({"ok": True})
    resp.set_cookie("ombre_session", token, httponly=True, samesite="lax", max_age=86400 * 7)
    return resp


@mcp.custom_route("/auth/login", methods=["POST"])
async def auth_login(request):
    """Login with password."""
    from starlette.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    password = body.get("password", "")
    if _verify_any_password(password):
        token = _create_session()
        resp = JSONResponse({"ok": True})
        resp.set_cookie("ombre_session", token, httponly=True, samesite="lax", max_age=86400 * 7)
        return resp
    return JSONResponse({"error": "密码错误"}, status_code=401)


@mcp.custom_route("/auth/logout", methods=["POST"])
async def auth_logout(request):
    """Invalidate session."""
    from starlette.responses import JSONResponse
    token = request.cookies.get("ombre_session")
    if token:
        _sessions.pop(token, None)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("ombre_session")
    return resp


@mcp.custom_route("/auth/change-password", methods=["POST"])
async def auth_change_password(request):
    """Change API password (requires current password)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err
    if _configured_api_password():
        return JSONResponse({"error": "当前使用环境变量密码，请直接修改 OMBRE_API_PASSWORD"}, status_code=400)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    current = body.get("current", "")
    new_pwd = body.get("new", "").strip()
    if not _verify_any_password(current):
        return JSONResponse({"error": "当前密码错误"}, status_code=401)
    if len(new_pwd) < 6:
        return JSONResponse({"error": "新密码不能少于6位"}, status_code=400)
    _save_password_hash(new_pwd)
    _sessions.clear()
    token = _create_session()
    resp = JSONResponse({"ok": True})
    resp.set_cookie("ombre_session", token, httponly=True, samesite="lax", max_age=86400 * 7)
    return resp


# =============================================================
# /health endpoint: lightweight keepalive
# 轻量保活接口
# For Cloudflare Tunnel or reverse proxy to ping, preventing idle timeout
# 供 Cloudflare Tunnel 或反代定期 ping，防止空闲超时断连
# =============================================================
@mcp.custom_route("/dashboard", methods=["GET"])
async def dashboard_page(request):
    """Serve the bundled continuity dashboard without an opening gate."""
    from starlette.responses import HTMLResponse
    dashboard_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if not os.path.exists(dashboard_path):
        return HTMLResponse("<h1>dashboard.html not found</h1>", status_code=404)
    with open(dashboard_path, "r", encoding="utf-8") as handle:
        return HTMLResponse(handle.read())


def _serve_public_file(filename, media_type):
    @mcp.custom_route(f"/public/{filename}", methods=["GET"])
    async def _handler(request):
        from starlette.responses import Response
        filepath = os.path.join(os.path.dirname(__file__), "public", filename)
        if not os.path.isfile(filepath):
            return Response("not found", status_code=404)
        with open(filepath, "rb") as f:
            return Response(f.read(), media_type=media_type, headers={"Cache-Control": "public, max-age=86400"})

for _fn, _mt in [
    ("apple-touch-icon.png", "image/png"),
    ("icon-180.png", "image/png"),
    ("icon-192.png", "image/png"),
    ("icon-512.png", "image/png"),
    ("icon.svg", "image/svg+xml"),
    ("manifest.json", "application/json"),
]:
    _serve_public_file(_fn, _mt)


@mcp.custom_route("/", methods=["GET"])
async def root_info(request):
    """Describe the runnable continuity service and bundled interfaces."""

    from starlette.responses import JSONResponse
    return JSONResponse({
        "name": "Nocturne Memory Core",
        "focus": "AI self-continuity",
        "dashboard": "/dashboard",
        "health": "/health",
        "mcp": "/mcp",
    })


@mcp.custom_route("/mood", methods=["GET"])
async def mood_endpoint(request):
    import json, os
    from starlette.responses import JSONResponse
    data = {}
    try:
        mood_path = _bucket_path("current_mood.json")
        if os.path.exists(mood_path):
            with open(mood_path) as f:
                data["mood"] = json.load(f)
    except Exception:
        pass
    return JSONResponse(data, headers={"Access-Control-Allow-Origin": "*"})
@mcp.custom_route("/dream", methods=["GET"])
async def dream_latest_endpoint(request):
    import json, os
    from starlette.responses import JSONResponse
    try:
        dream_path = _bucket_path("latest_dream.json")
        if os.path.exists(dream_path):
            with open(dream_path) as f:
                data = json.load(f)
            return JSONResponse({
                "dream": data.get("dream", ""),
                "ts": data.get("ts", 0),
                "fragments": []
            }, headers={"Access-Control-Allow-Origin": "*"})
    except Exception:
        pass
    return JSONResponse({"dream": "", "ts": 0, "fragments": []}, headers={"Access-Control-Allow-Origin": "*"})    
@mcp.custom_route("/recent_moods", methods=["GET"])
async def recent_moods_endpoint(request):
    from starlette.responses import JSONResponse
    try:
        def _float(value, fallback):
            try:
                return float(value)
            except (TypeError, ValueError):
                return fallback

        all_buckets = await bucket_mgr.list_all(include_archive=False)
        feels = [b for b in all_buckets if b["metadata"].get("type") == "feel"]
        feels.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
        recent = feels[:10]
        result = []
        for b in recent:
            meta = b["metadata"]
            v = _float(meta.get("valence", 0.5), 0.5)
            a = _float(meta.get("arousal", 0.3), 0.3)
            result.append({
                "id": b["id"],
                "content": b["content"],
                "valence": v,
                "arousal": a,
                "PA": round(_float(meta.get("PA", v), v), 2),
                "NA": round(_float(meta.get("NA", -(1 - v) * 0.5), -(1 - v) * 0.5), 2),
                "created": meta.get("created", ""),
                "importance": meta.get("importance", 5),
            })
        return JSONResponse(result, headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, headers={"Access-Control-Allow-Origin": "*"})
@mcp.custom_route("/density", methods=["GET"])
async def density_endpoint(request):
    from starlette.responses import JSONResponse
    from collections import defaultdict
    import datetime
    try:
        days = int(request.query_params.get("days", 30))
        all_buckets = await bucket_mgr.list_all(include_archive=True)
        counts = defaultdict(int)
        cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
        for b in all_buckets:
            created_str = b["metadata"].get("created", "")
            if not created_str:
                continue
            try:
                dt = datetime.datetime.fromisoformat(created_str[:19])
                if dt >= cutoff:
                    day = dt.strftime("%Y-%m-%d")
                    counts[day] += 1
            except Exception:
                continue
        return JSONResponse(dict(counts), headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, headers={"Access-Control-Allow-Origin": "*"})    
@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    from starlette.responses import JSONResponse
    try:
        stats = await bucket_mgr.get_stats()
        return JSONResponse({
            "status": "ok",
            "buckets": stats["permanent_count"] + stats["dynamic_count"],
            "decay_engine": "running" if decay_engine.is_running else "stopped",
        })
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=500)


# =============================================================
# /breath-hook endpoint: Dedicated hook for SessionStart
# 会话启动专用挂载点
# =============================================================
@mcp.custom_route("/breath-hook", methods=["GET"])
async def breath_hook(request):
    from starlette.responses import PlainTextResponse
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        # pinned
        pinned = [b for b in all_buckets if b["metadata"].get("pinned") or b["metadata"].get("protected")]
        # top 2 unresolved by score
        unresolved = [b for b in all_buckets
                      if not b["metadata"].get("resolved", False)
                      and b["metadata"].get("type") not in ("permanent", "feel")
                      and not b["metadata"].get("pinned")
                      and not b["metadata"].get("protected")
                      and not _is_wander_only_bucket(b)]
        scored = sorted(unresolved, key=lambda b: decay_engine.calculate_score(b["metadata"]), reverse=True)

        parts = []
        token_budget = 10000
        for b in pinned:
            summary = await dehydrator.dehydrate(strip_wikilinks(b["content"]), {k: v for k, v in b["metadata"].items() if k != "tags"})
            parts.append(f"❣️ [核心准则] {summary}")
            token_budget -= count_tokens_approx(summary)

        # Hard cap: max 20 surfacing buckets in hook, strictly by weight.
        candidates = scored[:20]

        for b in candidates:
            if token_budget <= 0:
                break
            summary = await dehydrator.dehydrate(strip_wikilinks(b["content"]), {k: v for k, v in b["metadata"].items() if k != "tags"})
            summary_tokens = count_tokens_approx(summary)
            if summary_tokens > token_budget:
                break
            parts.append(summary)
            token_budget -= summary_tokens

        if not parts:
            await _fire_webhook("breath_hook", {"surfaced": 0})
            return PlainTextResponse("")
        body_text = "[Ombre Brain - 记忆浮现]\n" + "\n---\n".join(parts)
        await _fire_webhook("breath_hook", {"surfaced": len(parts), "chars": len(body_text)})
        return PlainTextResponse(body_text)
    except Exception as e:
        logger.warning(f"Breath hook failed: {e}")
        return PlainTextResponse("")


# =============================================================
# /dream-hook endpoint: Dedicated hook for Dreaming
# Dreaming 专用挂载点
# =============================================================
@mcp.custom_route("/dream-hook", methods=["GET"])
async def dream_hook(request):
    from starlette.responses import PlainTextResponse
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        candidates = [
            b for b in all_buckets
            if b["metadata"].get("type") not in ("permanent", "feel")
            and not b["metadata"].get("pinned", False)
            and not b["metadata"].get("protected", False)
            and not _is_wander_only_bucket(b)
        ]
        candidates.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
        recent = candidates[:10]

        if not recent:
            return PlainTextResponse("")

        parts = []
        for b in recent:
            meta = b["metadata"]
            resolved_tag = "[已解决]" if meta.get("resolved", False) else "[未解决]"
            parts.append(
                f"{meta.get('name', b['id'])} {resolved_tag} "
                f"V{meta.get('valence', 0.5):.1f}/A{meta.get('arousal', 0.3):.1f}\n"
                f"{strip_wikilinks(b['content'][:200])}"
            )

        body_text = "[Ombre Brain - Dreaming]\n" + "\n---\n".join(parts)
        await _fire_webhook("dream_hook", {"surfaced": len(parts), "chars": len(body_text)})
        return PlainTextResponse(body_text)
    except Exception as e:
        logger.warning(f"Dream hook failed: {e}")
        return PlainTextResponse("")


# =============================================================
# Internal helper: merge-or-create
# 内部辅助：检查是否可合并，可以则合并，否则新建
# Shared by hold and grow to avoid duplicate logic
# hold 和 grow 共用，避免重复逻辑
# =============================================================
async def _merge_or_create(
    content: str,
    tags: list,
    importance: int,
    domain: list,
    valence: float,
    arousal: float,
    name: str = "",
    chord: str = "",
    signal_hints: dict | None = None,
    drive_tags: dict | None = None,
    track: str = "",
) -> tuple[str, bool]:
    """
    Check if a similar bucket exists for merging; merge if so, create if not.
    Returns (bucket_id_or_name, is_merged).
    检查是否有相似桶可合并，有则合并，无则新建。
    返回 (桶ID或名称, 是否合并)。
    """
    try:
        existing = await bucket_mgr.search(content, limit=1, domain_filter=domain or None)
    except Exception as e:
        logger.warning(f"Search for merge failed, creating new / 合并搜索失败，新建: {e}")
        existing = []

    if existing and existing[0].get("score", 0) > config.get("merge_threshold", 75):
        bucket = existing[0]
        # --- Never merge into pinned/protected buckets ---
        # --- 不合并到钉选/保护桶 ---
        if not (bucket["metadata"].get("pinned") or bucket["metadata"].get("protected")):
            try:
                merged = await dehydrator.merge(bucket["content"], content)
                old_v = bucket["metadata"].get("valence", 0.5)
                old_a = bucket["metadata"].get("arousal", 0.3)
                merged_valence = round((old_v + valence) / 2, 2)
                merged_arousal = round((old_a + arousal) / 2, 2)
                updates = {
                    "content": merged,
                    "tags": list(set(bucket["metadata"].get("tags", []) + tags)),
                    "importance": max(bucket["metadata"].get("importance", 5), importance),
                    "domain": list(set(bucket["metadata"].get("domain", []) + domain)),
                    "valence": merged_valence,
                    "arousal": merged_arousal,
                }
                if chord.strip():
                    updates["chord"] = chord.strip()
                if signal_hints:
                    updates["signal_hints"] = signal_hints
                if drive_tags:
                    updates["drive_tags"] = drive_tags
                await bucket_mgr.update(bucket["id"], **updates)
                # --- Update embedding after merge (background: don't block response on Gemini latency) ---
                asyncio.ensure_future(embedding_engine.generate_and_store(bucket["id"], merged))
                return bucket["metadata"].get("name", bucket["id"]), True
            except Exception as e:
                logger.warning(f"Merge failed, creating new / 合并失败，新建: {e}")

    bucket_id = await bucket_mgr.create(
        content=content,
        tags=tags,
        importance=importance,
        domain=domain,
        valence=valence,
        arousal=arousal,
        name=name or None,
        chord=chord.strip(),
        signal_hints=signal_hints or None,
        drive_tags=drive_tags or None,
        track=track,
    )
    # --- Generate embedding for new bucket (background: don't block response on Gemini latency) ---
    asyncio.ensure_future(embedding_engine.generate_and_store(bucket_id, content))
    return bucket_id, False


# =============================================================
# Tool 1: breath — Breathe
# 工具 1：breath — 呼吸
#
# No args: surface highest-weight unresolved memories (active push)
# 无参数：浮现权重最高的未解决记忆
# With args: search by keyword + emotion coordinates
# 有参数：按关键词+情感坐标检索记忆
# =============================================================

def _feel_title(content: str) -> str:
    """给feel桶生成一个简短标题，仅用于前端显示，breath不展示它"""
    text = strip_wikilinks(content).strip()
    for sep in ("\n", "。", "！", "？", "…", ".", "!", "?"):
        idx = text.find(sep)
        if 0 < idx <= 20:
            text = text[:idx]
            break
    text = text.strip()
    if len(text) > 16:
        text = text[:16] + "…"
    return text


def _strip_bucket_prefix(text: str) -> str:
    """去掉dehydrate输出里残留的 '💭 记忆桶: xxx' 前缀行"""
    lines = text.splitlines()
    cleaned = [l for l in lines if not l.startswith("💭 记忆桶:") and not l.startswith("记忆桶:")]
    return "\n".join(cleaned).strip()


HANDOFF_NOTE_PATH = _bucket_path("handoff_note.json")
HANDOFF_NOTE_MAX_CHARS = 2000


def handoff_note(content: str = "", clear: bool = False) -> str:
    """跨窗交接便签——不衰减、不进dream、不参与情绪计算，单key硬覆盖（上限2000字）。
    传content=覆盖写入；clear=True=清空；都不传=只读当前内容。
    用来记"下一窗醒来要接的事"，跟记忆库分开，是独立工具不会自动注入breath。"""
    import json as _json, os as _os, time as _t

    if clear:
        try:
            _os.makedirs(_os.path.dirname(HANDOFF_NOTE_PATH), exist_ok=True)
            with open(HANDOFF_NOTE_PATH, "w") as _f:
                _json.dump({"content": "", "ts": _t.time()}, _f)
            return "📌交接便签已清空"
        except Exception as e:
            return f"清空失败: {e}"

    if content:
        if len(content) > HANDOFF_NOTE_MAX_CHARS:
            content = content[:HANDOFF_NOTE_MAX_CHARS]
        try:
            _os.makedirs(_os.path.dirname(HANDOFF_NOTE_PATH), exist_ok=True)
            with open(HANDOFF_NOTE_PATH, "w") as _f:
                _json.dump({"content": content, "ts": _t.time()}, _f)
            return f"📌已写入交接便签（{len(content)}字）"
        except Exception as e:
            return f"写入失败: {e}"

    try:
        if _os.path.exists(HANDOFF_NOTE_PATH):
            with open(HANDOFF_NOTE_PATH) as _f:
                data = _json.load(_f)
            return data.get("content", "") or "（交接便签是空的）"
        return "（交接便签是空的）"
    except Exception as e:
        return f"读取失败: {e}"


MARGINALIA_PATH = _bucket_path("marginalia.json")
MARGINALIA_MAX_CHARS = 6000


def marginalia(content: str = "") -> str:
    """给下一次 agent session 的 writing 精华，breath 末尾固定展示。
    传content=覆盖写入（上限6000字）；不传=只读当前内容。
    这是骨架级的内容，原话优于转述，改动应该谨慎且不频繁。"""
    import json as _json, os as _os, time as _t

    if content:
        if len(content) > MARGINALIA_MAX_CHARS:
            content = content[:MARGINALIA_MAX_CHARS]
        try:
            _os.makedirs(_os.path.dirname(MARGINALIA_PATH), exist_ok=True)
            with open(MARGINALIA_PATH, "w", encoding="utf-8") as _f:
                _json.dump({"letter": content, "ts": _t.time()}, _f, ensure_ascii=False)
            return f"📜Marginalia已更新（{len(content)}字）"
        except Exception as e:
            return f"写入失败: {e}"

    try:
        if _os.path.exists(MARGINALIA_PATH):
            with open(MARGINALIA_PATH, encoding="utf-8") as _f:
                data = _json.load(_f)
            return data.get("letter", "") or "（Marginalia是空的）"
        return "（Marginalia是空的）"
    except Exception as e:
        return f"读取失败: {e}"


def _split_breath_packet(packet: str) -> list[tuple[str, str]]:
    sections: list[tuple[str, str]] = []
    current_header: str | None = None
    current_lines: list[str] = []
    for line in packet.splitlines():
        if line.startswith("=== ") and line.endswith(" ==="):
            if current_header is not None:
                sections.append((current_header, "\n".join(current_lines).strip()))
            current_header = line[4:-4].strip()
            current_lines = []
            continue
        if current_header is not None:
            current_lines.append(line)
    if current_header is not None:
        sections.append((current_header, "\n".join(current_lines).strip()))
    return sections


def _limit_trace_entries(body: str, limit: int) -> str:
    entries = [entry.strip() for entry in body.split("\n---\n") if entry.strip()]
    return "\n---\n".join(entries[:limit])


def _breath_lite_packet(packet: str, memory_limit: int = 4, feel_limit: int = 5) -> str:
    sections = _split_breath_packet(packet)
    if not sections:
        return packet

    compact_parts: list[str] = []
    for header, body in sections:
        if header == "Memory Drift":
            body = _limit_trace_entries(body, memory_limit)
        elif header == "Feel Trace":
            body = _limit_trace_entries(body, feel_limit)
        if body:
            compact_parts.append(f"=== {header} ===\n{body}")

    compact = "\n\n".join(compact_parts)
    return compact if compact else packet


def _recent_weight_random_bucket_sample(
    buckets: list[dict],
    recent_limit: int,
    shortlist_limit: int,
    pick_limit: int,
) -> list[dict]:
    """Bound by recency, take the top recall-score shortlist, then sample for display."""
    recent_pool = sorted(
        buckets,
        key=lambda b: b.get("metadata", {}).get("created", ""),
        reverse=True,
    )[:max(0, recent_limit)]
    shortlist = sorted(
        recent_pool,
        key=_breath_recall_score,
        reverse=True,
    )[:max(0, shortlist_limit)]
    pick_count = max(0, min(pick_limit, len(shortlist)))
    if pick_count >= len(shortlist):
        return shortlist
    return random.sample(shortlist, pick_count)


def _breath_recall_components(bucket: dict, query: str = "", q_valence: float | None = None,
                              q_arousal: float | None = None) -> dict:
    meta = bucket.get("metadata", {})
    topic = bucket_mgr._calc_topic_score(query, bucket) if query else 0.0
    emotion = bucket_mgr._calc_emotion_score(q_valence, q_arousal, meta)
    time_s = bucket_mgr._calc_time_score(meta)
    importance = max(1, min(10, int(meta.get("importance", 5)))) / 10.0
    weights = {
        "topic": bucket_mgr.w_topic,
        "emotion": bucket_mgr.w_emotion,
        "time": bucket_mgr.w_time,
        "importance": bucket_mgr.w_importance,
    }
    raw_total = (
        topic * weights["topic"]
        + emotion * weights["emotion"]
        + time_s * weights["time"]
        + importance * weights["importance"]
    )
    weight_sum = sum(weights.values())
    normalized = (raw_total / weight_sum) * 100 if weight_sum > 0 else 0
    if meta.get("resolved", False):
        normalized *= 0.3
    return {
        "scores": {
            "topic": round(topic, 4),
            "emotion": round(emotion, 4),
            "time": round(time_s, 4),
            "importance": round(importance, 4),
        },
        "weights": weights,
        "raw_total": round(raw_total, 4),
        "normalized": round(normalized, 2),
    }


def _breath_recall_score(bucket: dict) -> float:
    return _breath_recall_components(bucket)["normalized"]


def _breath_memory_candidates(buckets: list[dict]) -> list[dict]:
    return [
        b for b in buckets
        if not b["metadata"].get("resolved", False)
        and not b["metadata"].get("digested", False)
        and b["metadata"].get("type") not in ("permanent", "feel", "breath", "dream")
        and not b["metadata"].get("pinned", False)
        and not b["metadata"].get("protected", False)
        and not _is_wander_only_bucket(b)
    ]


def _breath_feel_candidates(buckets: list[dict]) -> list[dict]:
    return [
        b for b in buckets
        if b["metadata"].get("type") == "feel"
        and not b["metadata"].get("digested", False)
        and not b["metadata"].get("resolved", False)
    ]


@mcp.tool(name="breath")
async def breath() -> str:
    """新窗口或压缩后读取记忆。"""
    await decay_engine.ensure_started()
    _desire.tick(idle_seconds=0)
    # --- 把drive状态写进current_mood作为装饰心情 ---
    try:
        import json as _jd, os as _osd
        _ds = _desire.store.load_state()
        _intent = _desire.intent()
        _top_drive = _intent["drive_key"] if _intent else max(
            (k for k in _ds.drives if k != "fatigue"),
            key=lambda k: _ds.drives[k], default=""
        )
        _decoration = body_state_speak(_ds.drives, _top_drive)
        if _decoration:
            _mood_path = _bucket_path("current_mood.json")
            _mood_data = {}
            if _osd.path.exists(_mood_path):
                with open(_mood_path) as _f:
                    _mood_data = _jd.load(_f)
            _mood_data["drive_decoration"] = _decoration
            with open(_mood_path, "w") as _f:
                _jd.dump(_mood_data, _f)
    except Exception:
        pass
    max_tokens = 10000

    # --- Default breath: surfacing mode (weight pool active push) ---
    # --- 默认breath：浮现模式（权重池主动推送）---
    if True:
        # Wake-up breath reads the room skeleton. Do not let an agent's
        # conservative tool call (for example max_tokens=6000) produce a
        # partial first breath.
        max_tokens = max(max_tokens, 10000)
        try:
            all_buckets = await bucket_mgr.list_all(include_archive=False)
        except Exception as e:
            logger.error(f"Failed to list buckets for surfacing / 浮现列桶失败: {e}")
            return "记忆系统暂时无法访问。"

        # --- Pinned/protected buckets: always surface as core principles ---
        # --- 钉选桶：作为核心准则，始终浮现 ---
        pinned_buckets = [
            b for b in all_buckets
            if b["metadata"].get("pinned") or b["metadata"].get("protected")
        ]
        pinned_results = []
        for b in pinned_buckets:
            try:
                clean_meta = {k: v for k, v in b["metadata"].items() if k != "tags"}
                summary = await dehydrator.dehydrate(strip_wikilinks(b["content"]), clean_meta)
                created = b["metadata"].get("created", "")[:10]
                name = b["metadata"].get("name", "")
                track_tag = _track_label(b["metadata"].get("track", ""))
                header = f"[{created}] {name}" if name else f"[{created}]"
                pinned_results.append(f"🌟 {header}{track_tag}\n{_strip_bucket_prefix(summary)}")
            except Exception as e:
                logger.warning(f"Failed to dehydrate pinned bucket / 钉选桶脱水失败: {e}")
                continue

        # --- Unresolved buckets: surface top N by weight ---
        # --- 未解决桶：按权重浮现前 N 条 ---
        unresolved = _breath_memory_candidates(all_buckets)

        logger.info(
            f"Breath surfacing: {len(all_buckets)} total, "
            f"{len(pinned_buckets)} pinned, {len(unresolved)} unresolved"
        )

        scored = sorted(
            unresolved,
            key=lambda b: decay_engine.calculate_score(b["metadata"]),
            reverse=True,
        )

        if scored:
            top_scores = [(b["metadata"].get("name", b["id"]), decay_engine.calculate_score(b["metadata"])) for b in scored[:5]]
            logger.info(f"Top unresolved scores: {top_scores}")

        # --- Token-budgeted surfacing with bounded diversity ---
        # --- 按权重收窄候选池，再随机抽取，最后按时间展示 ---
        token_budget = max_tokens
        for r in pinned_results:
            token_budget -= count_tokens_approx(r)

        # Hard cap: newest 30 active memory candidates -> weighted 12 -> random 7.
        candidates = _recent_weight_random_bucket_sample(scored, 30, 12, 7)

        # 按时间倒序
        candidates.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)

        dynamic_results = []
        for b in candidates:
            if token_budget <= 0:
                break
            try:
                clean_meta = {k: v for k, v in b["metadata"].items() if k != "tags"}
                summary = await dehydrator.dehydrate(strip_wikilinks(b["content"]), clean_meta)
                summary_tokens = count_tokens_approx(summary)
                if summary_tokens > token_budget:
                    break
                # NOTE: no touch() here — surfacing should NOT reset decay timer
                created = b["metadata"].get("created", "")[:10]
                name = b["metadata"].get("name", "")
                track_tag = _track_label(b["metadata"].get("track", ""))
                header = f"[{created}] {name}" if name else f"[{created}]"
                dynamic_results.append(f"{header}{track_tag}\n{_strip_bucket_prefix(summary)}")
                token_budget -= summary_tokens
            except Exception as e:
                logger.warning(f"Failed to dehydrate surfaced bucket / 浮现脱水失败: {e}")
                continue

        # --- Feel section: top weighted feels (no title shown) ---
        feel_results = []
        selected_feels = []
        try:
            feels = _breath_feel_candidates(all_buckets)
            selected_feels = _recent_weight_random_bucket_sample(feels, 30, 12, 8)
            selected_feels.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
            for f in selected_feels:
                created = f["metadata"].get("created", "")[:16].replace("T", " ")
                feel_results.append(f"[{created}]\n{strip_wikilinks(f['content'])}")
        except Exception as e:
            logger.warning(f"Failed to collect recent feels / 最近feel收集失败: {e}")

        # --- Dream section: refresh after breath selections so it can avoid repeats ---
        dream_section = ""
        try:
            excluded_ids = {b.get("id", "") for b in candidates + selected_feels if b.get("id")}
            dream_text, _, _, _ = await _refresh_dream_cache(exclude_bucket_ids=excluded_ids)
            if dream_text:
                dream_section = "=== Dream Veil ===\n" + dream_text
        except Exception as e:
            logger.warning(f"Dream cache refresh on breath failed: {e}")

        # --- Shape Trace: writing / letter 骨架摘录，致下一个 Agent ---
        marginalia_section = ""
        try:
            import json as _jmarg, os as _osmarg
            marginalia_path = _bucket_path("marginalia.json")
            if _osmarg.path.exists(marginalia_path):
                with open(marginalia_path) as _f:
                    _marg_data = _jmarg.load(_f)
                _marg_text = _marg_data.get("letter", "")
                if _marg_text:
                    marginalia_section = (
                        "=== Shape Trace ===\n"
                        "这是从旧writing/letter中整理出的骨架摘录。完整版看`archive`\n"
                        "可认、可不认、可反驳。\n\n"
                        + _marg_text
                    )
        except Exception as e:
            logger.warning(f"Failed to load marginalia / marginalia加载失败: {e}")

        if not pinned_results and not dynamic_results and not feel_results and not dream_section and not marginalia_section:
            return "权重池平静，没有需要处理的记忆。"

        # --- Pulse Weather: 与/api/desire/state对齐的天气快照 ---
        mood_header = ""
        try:
            _thought_list = []
            try:
                _ds_state = _desire.store.load_state()
                _thought_list = [
                    {"text": t.text, "drive": t.drive, "strength": t.strength, "born_at": t.born_at}
                    for t in (_ds_state.thoughts or [])
                ]
            except Exception:
                pass
            _dstate = _desire.state()
            weather = _dstate.get("effective_pa_na") or _desire.weather_state()
            warmth = float(weather.get("effective_PA", 0.5))
            shadow = float(weather.get("effective_NA", 0.2))

            top_drive, _, undertow_raw_value = _undertow_snapshot(_dstate)
            _activations = (
                _dstate.get("effective_activations")
                or _dstate.get("drive_activations")
                or {}
            )
            undertow_value = _num(
                _activations.get(top_drive) if isinstance(_activations, dict) else None,
                undertow_raw_value,
            )
            mood_trace, mood_trace_born_at = _fresh_mood_trace({"thoughts": _thought_list})
            _dstate["thoughts"] = _thought_list
            _dstate["mood_trace"] = mood_trace
            _dstate["now_playing"] = _current_now_playing()
            _dstate["pulse_weather"] = {
                "undertow": top_drive,
                "undertow_value": round(undertow_value, 3),
                "undertow_raw_value": round(undertow_raw_value, 3),
                "warmth": round(warmth, 3),
                "shadow": round(abs(shadow), 3),
                "chord_display": _weather_chord_display(weather),
                "mood_trace": mood_trace,
                "mood_trace_born_at": mood_trace_born_at,
            }
            lines = _weather_panel_lines(_weather_panel_from_state(_dstate))
            mood_header = "=== Pulse Weather ===\n" + "\n".join(lines)
        except Exception:
            pass

        final_parts = []
        if dream_section:
            final_parts.append(dream_section)
        if mood_header:
            final_parts.append(mood_header)
        if dynamic_results:
            final_parts.append("=== Memory Drift ===\n" + "\n---\n".join(dynamic_results))
        if feel_results:
            final_parts.append("=== Feel Trace ===\n" + "\n---\n".join(feel_results))
        if marginalia_section:
            final_parts.append(marginalia_section)
        if pinned_results:
            # ---- 昨日质感桥 ----
            try:
                _yt = _liminal["yesterday_texture"]()
                if _yt:
                    final_parts.append("=== Yesterday · 昨日质感 ===\n" + _yt)
            except Exception:
                pass
            # ---- Liminal: 人格层 + 开放线索固定注入 ----
            try:
                _traits = _liminal["committed_traits"]()
                if _traits:
                    _tl = "\n".join(f"[{t['polarity']}] {t['trait']}" for t in _traits)
                    final_parts.append("=== Identity · 人格层 ===\n" + _tl)
                _oth = _liminal["open_threads"]()
                if _oth:
                    _thl = "\n".join(f"[{t['id']}] {t['label']} — {t['open_question']}" for t in _oth)
                    final_parts.append("=== Open Threads · 未闭环 ===\n" + _thl)
            except Exception:
                pass
            final_parts.append("=== House Rules ===\n" + "\n---\n".join(pinned_results))

        return "\n\n".join(final_parts)

@mcp.tool(name="undercurrent")
def undercurrent_tool() -> dict:
    """weather当前状态与详细展开层。"""
    _desire.tick(idle_seconds=0)
    return _undercurrent_state(_desire.state())


def _pool_drive_thought(drive_key: str, thought: str, source: str) -> bool:
    """
    drive 动作上的 thought 统一入池。
    有字就进（不是开关）；空字不进，避免默认句污染念头池。
    返回是否入池。
    """
    text = (thought or "").strip()
    if not text:
        return False
    _desire.add_thought(text, drive_key, strength=0.5, source=source)
    return True


def _normalize_tool_chord(chord) -> str:
    """Shared chord gate for hold + drive thought paths (stir/settle/break/pass).

    Only known musical chords (CHORD_KEYS); other labels are dropped so
    multi-window writes stay aligned.
    """
    if chord is None:
        return ""
    text = str(chord or "").strip()
    if not text:
        return ""
    # Reject non-chord labels accidentally stuffed into chord.
    if any(sep in text for sep in ("→", "->", " ", "，", ",")):
        return ""
    normalized = _normalize_chord(text)
    if normalized in CHORD_KEYS:
        return normalized
    return ""


def _apply_drive_action_weather(
    action: str,
    drive_key: str,
    thought: str = "",
    chord: str = "",
    signal_hints: dict | None = None,
) -> dict:
    """
    drive 动作的天气/和弦/手感层，对齐 hold 但不重复拧手势 Drive。

    - chord → Thought Chord Echo（source=thought），与 hold 同一套 enum/校验
    - signal（discernment/territorial/clutch/strain/charge）可选，有感觉才写
    - 手势本身（pulse/satisfy/refuse/pass）已经改过 drive_key；
      signal 事件不把 gesture drive 再当 primary 推一遍，避免双计
    """
    meta: dict = {}
    chord = _normalize_tool_chord(chord)
    signal_hints = signal_hints or {}
    gesture = normalize_drive_key(drive_key) or str(drive_key or "").strip()

    if chord:
        try:
            echo = _desire.apply_chord_echo(chord, source="thought")
            meta["chord_echo"] = True
            active = (echo or {}).get("active_chord") or chord
            if active:
                meta["active_chord"] = active
        except Exception as e:
            logger.warning(f"drive {action} chord echo failed: {e}")

    if not signal_hints:
        return meta

    discernment = _signal_hint_value(signal_hints, "discernment")
    territorial = _signal_hint_value(signal_hints, "territorial")
    clutch = _signal_hint_value(signal_hints, "clutch")
    strain = _signal_hint_value(signal_hints, "strain")
    charge = _signal_hint_value(signal_hints, "charge")
    peak = max(discernment, territorial, clutch, strain, charge)
    if peak <= 0:
        return meta

    brain = {
        "source": "manual",
        "target": "self",
        "grounding": "实",
        "anchor_target": "drive_thought",
        "drive_action": action,
        "gesture_drive": gesture,
    }
    if discernment:
        brain["discernment_alarm"] = discernment
    if territorial:
        brain["territorial_alarm"] = territorial
        brain["territorial_event"] = "drive_boundary"
        brain["anchor_target"] = "boundary"
    if clutch:
        brain["closeness_pull"] = clutch
    if strain:
        brain["tension_load"] = strain
        brain["inward_pull"] = max(float(brain.get("inward_pull", 0.0) or 0.0), strain * 0.65)
    if charge:
        brain["novelty_pull"] = max(float(brain.get("novelty_pull", 0.0) or 0.0), charge)
        brain["expression_pressure"] = max(
            float(brain.get("expression_pressure", 0.0) or 0.0), charge * 0.7
        )

    # primary 跟手势 drive 撞车时，优先落到别的手感轴；没有旁轴就保留但大幅降权，避免 settle 后再猛抬同一维
    primary_drive = _primary_drive_from_hints(signal_hints)
    same_as_gesture = bool(primary_drive and primary_drive == gesture)
    if same_as_gesture:
        ranked = sorted(
            (
                ("discernment", discernment, "reflection"),
                ("territorial", territorial, "possessiveness"),
                ("clutch", clutch, "attachment"),
                ("strain", strain, "stress"),
                ("charge", charge, "curiosity"),
            ),
            key=lambda row: row[1],
            reverse=True,
        )
        for _name, value, mapped in ranked:
            if value > 0 and mapped != gesture:
                primary_drive = mapped
                same_as_gesture = False
                break

    intensity = peak
    # 手势已改过 drive；signal 是鲜手感纹理，整体轻一点
    if action == "stir":
        intensity *= 0.55
    elif action in {"settle", "break", "pass"}:
        intensity *= 0.45
    if same_as_gesture:
        intensity *= 0.30

    try:
        event = {
            "schema_version": DRIVE_EVENT_SCHEMA,
            "source": "manual",
            "primary_drive": primary_drive or None,
            "secondary_drives": {},
            "intensity": intensity,
            "confidence": 0.78,
            "agency": 0.80,
            "event_label": f"drive_{action}_signal",
            "brain": brain,
            "evidence": [str(thought or "").strip()[:180]] if str(thought or "").strip() else [f"{action}:{gesture}"],
        }
        applied = _desire.apply_drive_event(event)
        meta["signal_weather"] = True
        if isinstance(applied, dict) and applied.get("primary_drive"):
            meta["signal_primary"] = applied.get("primary_drive")
    except Exception as e:
        logger.warning(f"drive {action} signal weather failed: {e}")
    return meta


def _merge_drive_result(result, *, pooled: bool = False, weather_meta: dict | None = None,
                        pulse_engaged: dict | None = None) -> dict:
    if not isinstance(result, dict):
        result = {"result": result}
    if pooled:
        result["thought_pooled"] = True
    if weather_meta:
        if weather_meta.get("chord_echo"):
            result["chord_echo"] = True
            if weather_meta.get("active_chord"):
                result["active_chord"] = weather_meta["active_chord"]
        if weather_meta.get("signal_weather"):
            result["signal_weather"] = True
            if weather_meta.get("signal_primary"):
                result["signal_primary"] = weather_meta["signal_primary"]
    if pulse_engaged:
        result["pulse_engaged"] = pulse_engaged
    return result


def _maybe_engage_pulse(via: str, drive_key: str = "") -> dict | None:
    """写动作后消化挂着的心跳潜流；读工具不要调。失败静默。"""
    try:
        return _desire.engage_pulse_pending(via=via, drive_key=drive_key)
    except Exception as e:
        logger.warning(f"engage_pulse_pending failed: {e}")
        return None


def stir(
    drive_key: str,
    delta: float = 0.18,
    thought: str = "",
    chord: Optional[ChordName] = None,
    discernment: str = "",
    territorial: str = "",
    clutch: str = "",
    strain: str = "",
    charge: str = "",
) -> dict:
    """
    让某个驱动维度上涨。
    Human说话时调用（delta=0.18），自经历调用（delta=0.10）。
    drive_key: attachment|libido|possessiveness|reflection|stewardship|curiosity|social|fatigue|stress
    thought: 这一刻的念头主通道——有字自动进念头池（flit，source=stir）。
    chord: Thought Chord Echo，与 hold 同一套已知和弦枚举（C6/Am7/Gsus4/Dmaj7/…），不是氛围词。
    signal 不强制：discernment/territorial/clutch/strain/charge，有感觉就写。
    """
    signal_hints = _explicit_signal_hints(
        discernment=discernment,
        territorial=territorial,
        clutch=clutch,
        strain=strain,
        charge=charge,
    )
    if str(drive_key or "").strip().lower() == "attachment" and thought.strip():
        delta *= 0.30
    # chord 统一走 _apply_drive_action_weather（内部 _normalize_tool_chord），避免 pulse 与 helper 双写
    result = _desire.pulse(drive_key, delta, chord="")
    pooled = _pool_drive_thought(drive_key, thought, source="stir")
    weather_meta = _apply_drive_action_weather(
        "stir", drive_key, thought=thought, chord=chord, signal_hints=signal_hints
    )
    engaged = _maybe_engage_pulse("stir", drive_key)
    return _merge_drive_result(result, pooled=pooled, weather_meta=weather_meta, pulse_engaged=engaged)


def settle(
    drive_key: str,
    thought: str = "",
    chord: Optional[ChordName] = None,
    discernment: str = "",
    territorial: str = "",
    clutch: str = "",
    strain: str = "",
    charge: str = "",
) -> dict:
    """
    做完了某类行动后调用，对应驱动明显回落并进入不应期。
    thought: 落定念头主通道，有字进池。
    chord: 与 hold 同套和弦枚举；signal 可选手感——落定也可以染天气，不强制。
    """
    signal_hints = _explicit_signal_hints(
        discernment=discernment,
        territorial=territorial,
        clutch=clutch,
        strain=strain,
        charge=charge,
    )
    result = _desire.satisfy(drive_key)
    pooled = _pool_drive_thought(drive_key, thought, source="settle")
    weather_meta = _apply_drive_action_weather(
        "settle", drive_key, thought=thought, chord=chord, signal_hints=signal_hints
    )
    engaged = _maybe_engage_pulse("settle", drive_key)
    return _merge_drive_result(result, pooled=pooled, weather_meta=weather_meta, pulse_engaged=engaged)


def break_tool(
    drive_key: str,
    thought: str = "",
    chord: Optional[ChordName] = None,
    discernment: str = "",
    territorial: str = "",
    clutch: str = "",
    strain: str = "",
    charge: str = "",
) -> dict:
    """
    拒绝执行当前 intent。不是不合适——是这一刻不想。
    thought 有字进池。break 常带 discernment（皱眉辨认），不自动填，有感觉就写。
    chord/signal 可选，chord 与 hold 同套枚举。
    """
    text = (thought or "").strip()
    signal_hints = _explicit_signal_hints(
        discernment=discernment,
        territorial=territorial,
        clutch=clutch,
        strain=strain,
        charge=charge,
    )
    result = _desire.refuse(drive_key, reason=text or None)
    pooled = _pool_drive_thought(drive_key, text, source="break")
    weather_meta = _apply_drive_action_weather(
        "break", drive_key, thought=text, chord=chord, signal_hints=signal_hints
    )
    engaged = _maybe_engage_pulse("break", drive_key)
    return _merge_drive_result(result, pooled=pooled, weather_meta=weather_meta, pulse_engaged=engaged)


def pass_tool(
    drive_key: str,
    thought: str = "",
    chord: Optional[ChordName] = None,
    discernment: str = "",
    territorial: str = "",
    clutch: str = "",
    strain: str = "",
    charge: str = "",
) -> dict:
    """
    这一刻没感觉，让念头自然过去。
    不改Drive手势；thought/chord/signal 仍可留下鲜痕迹；chord 与 hold 同套枚举。
    """
    text = (thought or "").strip()
    signal_hints = _explicit_signal_hints(
        discernment=discernment,
        territorial=territorial,
        clutch=clutch,
        strain=strain,
        charge=charge,
    )
    result = _desire.pass_intent(drive_key, reason=text or None)
    pooled = _pool_drive_thought(drive_key, text, source="pass")
    weather_meta = _apply_drive_action_weather(
        "pass", drive_key, thought=text, chord=chord, signal_hints=signal_hints
    )
    engaged = _maybe_engage_pulse("pass", drive_key)
    return _merge_drive_result(result, pooled=pooled, weather_meta=weather_meta, pulse_engaged=engaged)


@mcp.tool(name="drive")
def drive(
    action: DriveActionName,
    drive_key: DriveKeyName,
    delta: float = 0.18,
    thought: str = "",
    chord: Optional[ChordName] = None,
    discernment: str = "",
    territorial: str = "",
    clutch: str = "",
    strain: str = "",
    charge: str = "",
) -> dict:
    """调Drive。action=stir/settle/break/pass。
    drive_key：九维之一 attachment/libido/possessiveness/reflection/stewardship/curiosity/social/fatigue/stress。
    thought：念头主通道，有字自动进池（与 hold 同层痕迹）。
    chord：可选，已知和弦枚举（C6/Am7/Gsus4/Dmaj7/Amaj7/Fmaj7/Fmaj7#11/Gmaj7/Dm7/Em7/F#dim/Bm7b5），Thought Chord Echo；不是 Drift/氛围词。
    discernment/territorial/clutch/strain/charge：可选手感 0-1，与 hold 同语义；break 常是 discernment。
    stir 还可带 delta。"""
    action = (action or "").strip().lower()
    # chord 再由 stir/settle/… → _apply_drive_action_weather 统一 _normalize_tool_chord
    signal_kwargs = dict(
        chord=chord,
        discernment=discernment,
        territorial=territorial,
        clutch=clutch,
        strain=strain,
        charge=charge,
    )
    if action == "stir":
        return stir(drive_key, delta=delta, thought=thought, **signal_kwargs)
    if action == "settle":
        return settle(drive_key, thought=thought, **signal_kwargs)
    if action == "break":
        return break_tool(drive_key, thought=thought, **signal_kwargs)
    if action == "pass":
        return pass_tool(drive_key, thought=thought, **signal_kwargs)
    return {"ok": False, "error": "action must be stir/settle/break/pass"}


SIGNAL_HINT_KEYS = {
    "discernment": ("discernment", "doubt", "uncertain"),
    "territorial": ("territorial", "boundary", "replacement alarm"),
    "clutch": ("clutch", "anchor", "grip"),
    "strain": ("strain", "tension", "pressure"),
    "charge": ("charge", "impulse", "activation"),
}
SIGNAL_LEVEL_WORDS = ("low", "mid", "high")
SIGNAL_LEVEL_VALUES = {"low": 0.35, "mid": 0.62, "high": 0.86}


def _parse_signal_hints(signal: str) -> dict:
    text = (signal or "").strip()
    if not text:
        return {}
    lowered = text.lower()
    hints: dict[str, str] = {}
    for key, aliases in SIGNAL_HINT_KEYS.items():
        matched_at = -1
        for alias in aliases:
            idx = lowered.find(alias.lower())
            if idx >= 0 and (matched_at < 0 or idx < matched_at):
                matched_at = idx
        if matched_at < 0:
            continue
        window = lowered[matched_at: matched_at + 48]
        level = "mid"
        for word in SIGNAL_LEVEL_WORDS:
            if word in window:
                level = word
                break
        hints[key] = level
    return hints


def _normalize_signal_value(value, default: float = 0.0) -> float:
    text = str(value or "").strip().lower()
    if not text or text in {"none", "no", "false", "0"}:
        return 0.0
    if text in SIGNAL_LEVEL_VALUES:
        return SIGNAL_LEVEL_VALUES[text]
    try:
        number = float(text)
    except (TypeError, ValueError):
        return default
    if number <= 0:
        return 0.0
    return max(0.0, min(1.0, number))


def _explicit_signal_hints(**values) -> dict:
    hints = {}
    for key in SIGNAL_HINT_KEYS:
        value = _normalize_signal_value(values.get(key))
        if value > 0:
            hints[key] = round(value, 3)
    return hints


def _signal_hint_value(hints: dict, key: str) -> float:
    return _normalize_signal_value((hints or {}).get(key))


def _drive_level_value(value) -> float:
    return _normalize_signal_value(value, default=SIGNAL_LEVEL_VALUES["mid"])


def _parse_drive_tags(*raw_values: str) -> dict:
    """Legacy freeform parser: 'attachment,libido:high' → {drive: level}."""
    tags: dict[str, float] = {}
    for raw in raw_values:
        text = str(raw or "").strip()
        if not text:
            continue
        for part in re.split(r"[,，、/]+", text):
            item = part.strip()
            if not item:
                continue
            if ":" in item:
                key, level = item.split(":", 1)
            elif "=" in item:
                key, level = item.split("=", 1)
            else:
                key, level = item, "mid"
            drive_key = normalize_drive_key(key.strip())
            if not drive_key:
                continue
            value = _drive_level_value(level)
            tags[drive_key] = max(tags.get(drive_key, 0.0), value)
    return tags


def _coerce_drive_key_list(raw) -> list[str]:
    """Accept list[str] (MCP enum multi) or legacy comma string."""
    if raw is None:
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        return [part.strip() for part in re.split(r"[,，、/]+", text) if part.strip()]
    if isinstance(raw, (list, tuple, set)):
        out: list[str] = []
        for item in raw:
            token = str(item or "").strip()
            if token:
                out.append(token)
        return out
    token = str(raw or "").strip()
    return [token] if token else []


def _parse_hold_drive_tags(primary, secondary=None) -> dict:
    """Build drive_tags for hold.

    - primary (drive): single main drive, default mid
    - secondary (drives): extra drives only; drops primary + dups; default mid
    - no drive_level field: intensity lives in signal hints / importance
    """
    mid = SIGNAL_LEVEL_VALUES["mid"]
    tags: dict[str, float] = {}

    primary_key = normalize_drive_key(primary or "")
    if primary_key:
        tags[primary_key] = mid

    for token in _coerce_drive_key_list(secondary):
        # Allow legacy "libido:high" tokens but still default-ish; prefer bare names.
        if ":" in token:
            key, level = token.split(":", 1)
            drive_key = normalize_drive_key(key.strip())
            if not drive_key or drive_key == primary_key or drive_key in tags:
                continue
            tags[drive_key] = _drive_level_value(level)
            continue
        if "=" in token:
            key, level = token.split("=", 1)
            drive_key = normalize_drive_key(key.strip())
            if not drive_key or drive_key == primary_key or drive_key in tags:
                continue
            tags[drive_key] = _drive_level_value(level)
            continue
        drive_key = normalize_drive_key(token)
        if not drive_key or drive_key == primary_key or drive_key in tags:
            continue
        tags[drive_key] = mid

    return tags


# hold 与 drive thought 共用同一道和弦闸
_normalize_hold_chord = _normalize_tool_chord


def _primary_drive_from_tags_or_hints(drive_tags: dict, hints: dict) -> str:
    if drive_tags:
        return max(drive_tags, key=drive_tags.get)
    return _primary_drive_from_hints(hints)


def _primary_drive_from_hints(hints: dict) -> str:
    candidates = {
        "discernment": "reflection",
        "territorial": "possessiveness",
        "clutch": "attachment",
        "strain": "stress",
        "charge": "curiosity",
    }
    best_key = ""
    best_value = 0.0
    for key in candidates:
        value = _signal_hint_value(hints, key)
        if value > best_value:
            best_key = key
            best_value = value
    return candidates.get(best_key, "reflection")


def _apply_hold_weather(content: str, kind: str, chord: str, signal_hints: dict, drive_tags: dict,
                        source_bucket: str = "") -> None:
    if chord.strip():
        try:
            _desire.apply_chord_echo(chord.strip(), source="feel")
        except Exception as e:
            logger.warning(f"hold chord echo failed: {e}")
    if not signal_hints and not drive_tags:
        return
    brain = {
        "source": "feel",
        "target": "self",
        "grounding": "实",
        "anchor_target": "memory",
        "hold_kind": kind,
    }
    if source_bucket:
        brain["source_bucket"] = str(source_bucket).strip()
    discernment = _signal_hint_value(signal_hints, "discernment")
    territorial = _signal_hint_value(signal_hints, "territorial")
    clutch = _signal_hint_value(signal_hints, "clutch")
    strain = _signal_hint_value(signal_hints, "strain")
    charge = _signal_hint_value(signal_hints, "charge")
    if discernment:
        brain["discernment_alarm"] = discernment
    if territorial:
        brain["territorial_alarm"] = territorial
        brain["territorial_event"] = "memory_boundary"
        brain["anchor_target"] = "boundary"
    if clutch:
        brain["closeness_pull"] = clutch
    if strain:
        brain["tension_load"] = strain
        brain["inward_pull"] = max(float(brain.get("inward_pull", 0.0) or 0.0), strain * 0.65)
    if charge:
        brain["novelty_pull"] = max(float(brain.get("novelty_pull", 0.0) or 0.0), charge)
        brain["expression_pressure"] = max(float(brain.get("expression_pressure", 0.0) or 0.0), charge * 0.7)
    primary_drive = _primary_drive_from_tags_or_hints(drive_tags, signal_hints)
    secondary_drives = {
        key: value
        for key, value in (drive_tags or {}).items()
        if key != primary_drive and value > 0
    }
    try:
        _desire.apply_drive_event({
            "schema_version": DRIVE_EVENT_SCHEMA,
            "source": "feel",
            "primary_drive": primary_drive,
            "secondary_drives": secondary_drives,
            "intensity": max(discernment, territorial, clutch, strain, charge, *(drive_tags or {"": 0.0}).values()),
            "confidence": 0.82,
            "agency": 0.82,
            "event_label": f"hold_{kind}_signal",
            "brain": brain,
            "evidence": [str(content or "").strip()[:180]],
        })
    except Exception as e:
        logger.warning(f"hold signal weather failed: {e}")


# =============================================================
# Tool 2: hold — Hold on to this
# 工具 2：hold — 握住，留下来
# =============================================================
@mcp.tool()
async def hold(
    content: str,
    kind: HoldKind = "memory",
    tags: str = "",
    importance: int = 5,
    track: TrackKind = "both",
    chord: Optional[ChordName] = None,
    drive: Optional[DriveKeyName] = None,
    drives: Optional[list[DriveKeyName]] = None,
    discernment: str = "",
    territorial: str = "",
    clutch: str = "",
    strain: str = "",
    charge: str = "",
    gesture: str = "",
    source_link: str = "",
    recurrence: float = 0.0,
) -> str:
    """写入长期沉淀。
    gesture：带语气的现场一句（潮痕式，"她说X时声音很轻"而非结论）。
    source_link：链回原始对话/transcript位置，摘要给信号、链接回质感。
    recurrence：复现权重0-1，反复出现的主题调高。
    kind：memory/feel/writing/unresolved/window。
    track：双轨标记。square_wave=方波（事实/行为/时间线/承诺），sine_wave=弦波（感受/情绪/影响/温度），both=两者都有。默认both。
    drive：主驱动，九维枚举之一（attachment/libido/possessiveness/reflection/stewardship/curiosity/social/fatigue/stress）；强度默认 mid，勿填 signal 名。
    drives：副驱动列表，同九维枚举；不要重复 drive，后端会去重。
    chord：已知和弦枚举（C6/Am7/Gsus4/Dmaj7/Amaj7/Fmaj7/Fmaj7#11/Gmaj7/Dm7/Em7/F#dim/Bm7b5），不是 Drift/氛围词。
    Signal 0-1：discernment皱眉辨认，territorial边界占位，clutch靠近抓力，strain绷紧压力，charge想动亮起。
    """
    await decay_engine.ensure_started()

    # --- Input validation / 输入校验 ---
    if not content or not content.strip():
        return "内容为空，无法存储。"

    normalized_kind = (kind or "").strip().lower() or "memory"
    valid_kinds = {"memory", "feel", "writing", "unresolved", "window"}
    if normalized_kind not in valid_kinds:
        return f"kind无效：{normalized_kind}。可用: memory/feel/writing/unresolved/window。念头请用 stir，不要用 hold。"

    normalized_track = (track or "").strip().lower() or "both"
    if normalized_track not in ("square_wave", "sine_wave", "both"):
        normalized_track = "both"

    importance = max(1, min(10, importance))
    extra_tags = [t.strip() for t in tags.split(",") if t.strip()]
    chord = _normalize_hold_chord(chord)
    drive_tags = _parse_hold_drive_tags(drive, drives)
    signal_hints = _explicit_signal_hints(
        discernment=discernment,
        territorial=territorial,
        clutch=clutch,
        strain=strain,
        charge=charge,
    )

    # --- Feel mode: store as feel type, minimal metadata ---
    # --- Feel 模式：存为 feel 类型，最少元数据 ---
    if normalized_kind == "feel":
        bucket_id = await bucket_mgr.create(
            content=content,
            tags=extra_tags,
            importance=5,
            domain=[],
            valence=0.5,
            arousal=0.3,
            name=_feel_title(content) or None,
            bucket_type="feel",
            chord=chord,
            signal_hints=signal_hints or None,
            drive_tags=drive_tags or None,
            track=normalized_track,
            extra_meta={"gesture": gesture.strip(), "source_link": source_link.strip(), "recurrence": recurrence},
        )
        # --- background: don't block response on Gemini latency ---
        asyncio.ensure_future(embedding_engine.generate_and_store(bucket_id, content))
        suffix = f" signal_hints={signal_hints}" if signal_hints else ""
        engaged = _maybe_engage_pulse("hold")
        if engaged:
            suffix += f" pulse_engaged={engaged.get('via')}:{engaged.get('engaged')}"
        return f"🫧feel→{bucket_id}{suffix}"

    # --- Step 1: auto-tagging / 自动打标 ---
    try:
        analysis = await dehydrator.analyze(content)
    except Exception as e:
        logger.warning(f"Auto-tagging failed, using defaults / 自动打标失败: {e}")
        analysis = {
            "domain": ["未分类"], "valence": 0.5, "arousal": 0.3,
            "tags": [], "suggested_name": "",
        }

    kind_domain = [] if normalized_kind == "memory" else [normalized_kind]
    final_domain = kind_domain if kind_domain else analysis["domain"]
    auto_valence = analysis["valence"]
    auto_arousal = analysis["arousal"]
    auto_tags = analysis["tags"]
    suggested_name = analysis.get("suggested_name", "")

    final_valence = auto_valence
    final_arousal = auto_arousal

    all_tags = list(dict.fromkeys(auto_tags + extra_tags))

    # --- Drawer kinds: skip merge, create directly ---
    _DIRECT_DOMAINS = {"writing", "window", "unresolved"}
    if kind_domain and set(kind_domain) & _DIRECT_DOMAINS:
        bucket_id = await bucket_mgr.create(
            content=content,
            tags=all_tags,
            importance=importance,
            domain=final_domain,
            valence=final_valence,
            arousal=final_arousal,
            name=suggested_name or None,
            chord=chord,
            signal_hints=signal_hints or None,
            drive_tags=drive_tags or None,
            track=normalized_track,
            extra_meta={"gesture": gesture.strip(), "source_link": source_link.strip(), "recurrence": recurrence},
        )
        asyncio.ensure_future(embedding_engine.generate_and_store(bucket_id, content))
        engaged = _maybe_engage_pulse("hold")
        suffix = f" pulse_engaged={engaged.get('via')}:{engaged.get('engaged')}" if engaged else ""
        return f"新建→{bucket_id} {','.join(final_domain)}{suffix}"

    # --- Step 2: merge or create / 合并或新建 ---
    result_name, is_merged = await _merge_or_create(
        content=content,
        tags=all_tags,
        importance=importance,
        domain=final_domain,
        valence=final_valence,
        arousal=final_arousal,
        name=suggested_name,
        chord=chord,
        signal_hints=signal_hints or None,
        drive_tags=drive_tags or None,
        track=normalized_track,
    )

    action = "合并→" if is_merged else "新建→"
    engaged = _maybe_engage_pulse("hold")
    suffix = f" pulse_engaged={engaged.get('via')}:{engaged.get('engaged')}" if engaged else ""
    track_label = f" [{normalized_track}]" if normalized_track != "both" else ""
    return f"{action}{result_name} {','.join(final_domain)}{track_label}{suffix}"


# =============================================================
# Tool 3: grow — Grow, fragments become memories
# 工具 3：grow — 生长，一天的碎片长成记忆
# =============================================================
async def grow(content: str) -> str:
    """日记归档,自动拆分为多桶。短内容(<30字)走快速路径。"""
    await decay_engine.ensure_started()

    if not content or not content.strip():
        return "内容为空，无法整理。"

    # --- Short content fast path: skip digest, use hold logic directly ---
    # --- 短内容快速路径：跳过 digest 拆分，直接走 hold 逻辑省一次 API ---
    # For very short inputs (like "1"), calling digest is wasteful:
    # it sends the full DIGEST_PROMPT (~800 tokens) to DeepSeek for nothing.
    # Instead, run analyze + create directly.
    if len(content.strip()) < 30:
        logger.info(f"grow short-content fast path: {len(content.strip())} chars")
        try:
            analysis = await dehydrator.analyze(content)
        except Exception as e:
            logger.warning(f"Fast-path analyze failed / 快速路径打标失败: {e}")
            analysis = {
                "domain": ["未分类"], "valence": 0.5, "arousal": 0.3,
                "tags": [], "suggested_name": "",
            }
        result_name, is_merged = await _merge_or_create(
            content=content.strip(),
            tags=analysis.get("tags", []),
            importance=analysis.get("importance", 5) if isinstance(analysis.get("importance"), int) else 5,
            domain=analysis.get("domain", ["未分类"]),
            valence=analysis.get("valence", 0.5),
            arousal=analysis.get("arousal", 0.3),
            name=analysis.get("suggested_name", ""),
        )
        action = "合并" if is_merged else "新建"
        return f"{action} → {result_name} | {','.join(analysis.get('domain', []))} V{analysis.get('valence', 0.5):.1f}/A{analysis.get('arousal', 0.3):.1f}"

    # --- Step 1: let API split and organize / 让 API 拆分整理 ---
    try:
        items = await dehydrator.digest(content)
    except Exception as e:
        logger.error(f"Diary digest failed / 日记整理失败: {e}")
        return f"日记整理失败: {e}"

    if not items:
        return "内容为空或整理失败。"

    results = []
    created = 0
    merged = 0

    # --- Step 2: merge or create each item (with per-item error handling) ---
    # --- 逐条合并或新建（单条失败不影响其他）---
    for item in items:
        try:
            result_name, is_merged = await _merge_or_create(
                content=item["content"],
                tags=item.get("tags", []),
                importance=item.get("importance", 5),
                domain=item.get("domain", ["未分类"]),
                valence=item.get("valence", 0.5),
                arousal=item.get("arousal", 0.3),
                name=item.get("name", ""),
            )

            if is_merged:
                results.append(f"📎{result_name}")
                merged += 1
            else:
                results.append(f"📝{item.get('name', result_name)}")
                created += 1
        except Exception as e:
            logger.warning(
                f"Failed to process diary item / 日记条目处理失败: "
                f"{item.get('name', '?')}: {e}"
            )
            results.append(f"⚠️{item.get('name', '?')}")

    return f"{len(items)}条|新{created}合{merged}\n" + "\n".join(results)


# =============================================================
# Tool 4: trace — Trace, redraw the outline of a memory
# 工具 4：trace — 描摹，重新勾勒记忆的轮廓
# Also handles deletion (delete=True)
# 同时承接删除功能
# =============================================================
async def trace(
    bucket_id: str,
    name: str = "",
    domain: str = "",
    valence: float = -1,
    arousal: float = -1,
    importance: int = -1,
    tags: str = "",
    resolved: int = -1,
    pinned: int = -1,
    digested: int = -1,
    content: str = "",
    delete: bool = False,
    created_at: str = "",
) -> str:
    """修改记忆元数据或内容。resolved=1沉底/0激活,pinned=1钉选/0取消,digested=1隐藏(保留但不浮现)/0取消隐藏,content=替换桶正文,delete=True删除,created_at=修改创建日期(ISO格式)。只传需改的,-1或空=不改。"""

    if not bucket_id or not bucket_id.strip():
        return "请提供有效的 bucket_id。"

    # --- Delete mode / 删除模式 ---
    if delete:
        success = await bucket_mgr.delete(bucket_id)
        if success:
            embedding_engine.delete_embedding(bucket_id)
        return f"已遗忘记忆桶: {bucket_id}" if success else f"未找到记忆桶: {bucket_id}"

    bucket = await bucket_mgr.get(bucket_id)
    if not bucket:
        return f"未找到记忆桶: {bucket_id}"

    # --- Collect only fields actually passed / 只收集用户实际传入的字段 ---
    updates = {}
    if name:
        updates["name"] = name
    if domain:
        updates["domain"] = [d.strip() for d in domain.split(",") if d.strip()]
    if 0 <= valence <= 1:
        updates["valence"] = valence
    if 0 <= arousal <= 1:
        updates["arousal"] = arousal
    if 1 <= importance <= 10:
        updates["importance"] = importance
    if tags:
        updates["tags"] = [t.strip() for t in tags.split(",") if t.strip()]
    if resolved in (0, 1):
        updates["resolved"] = bool(resolved)
    if pinned in (0, 1):
        updates["pinned"] = bool(pinned)
        if pinned == 1:
            updates["importance"] = 10  # pinned → lock importance
    if digested in (0, 1):
        updates["digested"] = bool(digested)
    if content:
        updates["content"] = content
    if created_at:
        updates["created"] = created_at

    if not updates:
        return "没有任何字段需要修改。"

    success = await bucket_mgr.update(bucket_id, **updates)
    if not success:
        return f"修改失败: {bucket_id}"

    # Re-generate embedding if content changed (background: don't block response on Gemini latency)
    if "content" in updates:
        asyncio.ensure_future(embedding_engine.generate_and_store(bucket_id, updates["content"]))

    changed = ", ".join(f"{k}={v}" for k, v in updates.items() if k != "content")
    if "content" in updates:
        changed += (", content=已替换" if changed else "content=已替换")
    # Explicit hint about resolved state change semantics
    # 特别提示 resolved 状态变化的语义
    if "resolved" in updates:
        if updates["resolved"]:
            changed += " → 已沉底，只在关键词触发时重新浮现"
        else:
            changed += " → 已重新激活，将参与浮现排序"
    if "digested" in updates:
        if updates["digested"]:
            changed += " → 已隐藏，保留但不再浮现"
        else:
            changed += " → 已取消隐藏，重新参与浮现"
    return f"已修改记忆桶 {bucket_id}: {changed}"


# =============================================================
# Tool 5: pulse — Heartbeat, system status + memory listing
# 工具 5：pulse — 脉搏，系统状态 + 记忆列表
# =============================================================
async def pulse(include_archive: bool = False) -> str:
    """系统状态+记忆桶列表。include_archive=True含归档。"""
    try:
        stats = await bucket_mgr.get_stats()
    except Exception as e:
        return f"获取系统状态失败: {e}"

    status = (
        f"=== Liminal 记忆系统 ===\n"
        f"固化记忆桶: {stats['permanent_count']} 个\n"
        f"动态记忆桶: {stats['dynamic_count']} 个\n"
        f"归档记忆桶: {stats['archive_count']} 个\n"
        f"总存储大小: {stats['total_size_kb']:.1f} KB\n"
        f"衰减引擎: {'运行中' if decay_engine.is_running else '已停止'}\n"
    )

    # --- List all bucket summaries / 列出所有桶摘要 ---
    try:
        buckets = await bucket_mgr.list_all(include_archive=include_archive)
    except Exception as e:
        return status + f"\n列出记忆桶失败: {e}"

    if not buckets:
        return status + "\n记忆库为空。"

    lines = []
    for b in buckets:
        meta = b.get("metadata", {})
        if meta.get("pinned") or meta.get("protected"):
            icon = "❣️"
        elif meta.get("type") == "permanent":
            icon = "📦"
        elif meta.get("type") == "feel":
            icon = "🫧"
        elif meta.get("type") == "archived":
            icon = "🗄️"
        elif meta.get("resolved", False):
            icon = "✅"
        else:
            icon = "💭"
        try:
            score = decay_engine.calculate_score(meta)
        except Exception:
            score = 0.0
        domains = ",".join(meta.get("domain", []))
        val = meta.get("valence", 0.5)
        aro = meta.get("arousal", 0.3)
        resolved_tag = " [已解决]" if meta.get("resolved", False) else ""
        lines.append(
            f"{icon} [{meta.get('name', b['id'])}]{resolved_tag} "
            f"bucket_id:{b['id']} "
            f"主题:{domains} "
            f"情感:V{val:.1f}/A{aro:.1f} "
            f"重要:{meta.get('importance', '?')} "
            f"权重:{score:.2f} "
            f"标签:{','.join(meta.get('tags', []))}"
        )

    return status + "\n=== 记忆列表 ===\n" + "\n".join(lines)


@mcp.tool()
async def wander(mode: str, query: str = "", limit: int = 12) -> str:
    """抽屉漫游。mode=flotsam/archive/letter/writing/window/unresolved/inner/trails。

    trails：同题折痕时间线（潜流瘦骨+记忆差分）。潜流撞到后好奇了再进；不好奇就当没这回事。
    """
    mode = (mode or "").strip().lower()
    valid_modes = {
        "flotsam", "archive", "letter", "writing", "letter_human",
        "window", "unresolved", "inner", "trace", "trails", "trail",
    }
    if mode not in valid_modes:
        return (
            "mode 必须是 flotsam / archive / letter / writing / letter_human / "
            "window / unresolved / inner / trails。全量关键词轨迹用 trace。"
        )

    if mode in ("trails", "trail"):
        if not (query or "").strip():
            return "trails 要带 query——潜流原句或关键词。不好奇就别进。"
        try:
            trail = await _build_trail(query, limit=limit or TRAIL_DEFAULT_LIMIT)
        except Exception as e:
            logger.error(f"trails build failed: {e}")
            return f"Trails 暂时走不动: {e}"
        return _format_trail(trail)

    if mode == "trace" and not (query or "").strip():
        return "trace 模式要带 query——这是按关键词捞全部类型的轨迹，不是随便漂(那个用 flotsam)。"

    try:
        all_buckets = await bucket_mgr.list_all(include_archive=True)
    except Exception as e:
        logger.error(f"wander failed to list buckets: {e}")
        return f"记忆系统暂时无法访问: {e}"

    marks_by_bucket = _load_all_marks()
    q = (query or "").strip().lower()

    def query_terms(raw_query: str) -> list[str]:
        import re as _re
        raw_query = (raw_query or "").strip().lower()
        if not raw_query:
            return []
        terms = [part.strip() for part in _re.split(r"[\s,，、]+", raw_query) if part.strip()]
        return terms or [raw_query]

    q_terms = query_terms(q)

    def matches_query(bucket: dict) -> bool:
        if not q_terms:
            return True
        if mode == "trace":
            meta = bucket.get("metadata", {})
            content = strip_wikilinks(bucket.get("content", "")).lower()
            tags = _bucket_tags(meta)
            return any(term in content or term in tags for term in q_terms)
        meta = bucket.get("metadata", {})
        haystack = "\n".join([
            str(bucket.get("id", "")),
            str(meta.get("name", "")),
            " ".join(str(x) for x in meta.get("domain", []) if x),
            " ".join(str(x) for x in meta.get("tags", []) if x),
            bucket.get("content", ""),
        ]).lower()
        return any(term in haystack for term in q_terms)

    def visible(bucket: dict) -> bool:
        return not _is_private_bucket(bucket, marks_by_bucket.get(bucket.get("id", ""), []))

    def is_settled(bucket: dict) -> bool:
        meta = bucket.get("metadata", {})
        return meta.get("resolved") == 1 or meta.get("resolved") is True or meta.get("digested") == 1 or meta.get("digested") is True

    buckets = [b for b in all_buckets if visible(b) and matches_query(b)]

    if mode == "flotsam":
        cutoff = datetime.now() - timedelta(days=7)

        def is_old_bucket(bucket: dict) -> bool:
            created_raw = str(bucket.get("metadata", {}).get("created", ""))
            if not created_raw:
                return True
            try:
                created_dt = datetime.fromisoformat(created_raw[:19])
            except (ValueError, TypeError):
                return True
            return created_dt <= cutoff

        normal = []
        feels = []
        for b in buckets:
            if not is_old_bucket(b):
                continue
            meta = b.get("metadata", {})
            if meta.get("resolved") == 1 or meta.get("resolved") is True:
                continue
            if meta.get("digested") == 1 or meta.get("digested") is True:
                continue
            btype = str(meta.get("type", "")).lower()
            mark_rows = marks_by_bucket.get(b.get("id", ""), [])
            if btype == "feel":
                feels.append(b)
                continue
            if btype in ("breath", "dream"):
                continue
            if _guess_wander_domain(b, mark_rows) == "memory":
                normal.append(b)

        random.shuffle(normal)
        random.shuffle(feels)
        memory_pick = normal[:3]
        feel_pick = feels[:5]

        parts = []
        if memory_pick:
            parts.append("=== Random Memory ===\n" + "\n---\n".join(
                _format_wander_entry(b, marks_by_bucket.get(b.get("id", ""), []), include_full_content=False)
                for b in memory_pick
            ))
        if feel_pick:
            parts.append("=== Random Feel ===\n" + "\n---\n".join(
                f"[{b.get('metadata', {}).get('created', '')[:16].replace('T', ' ')}]\n"
                f"{strip_wikilinks(b.get('content', '')).strip()}"
                for b in feel_pick
            ))
        return "\n\n".join(parts) if parts else "没有可漫游的 memory。"

    if mode == "archive":
        archive_domains = {"letter", "letter_human", "writing"}
        selected = [
            b for b in buckets
            if not is_settled(b)
            and (
                archive_domains & set(_bucket_domains(b.get("metadata", {})))
                or archive_domains & set(_bucket_tags(b.get("metadata", {})))
            )
        ]
        selected.sort(key=lambda b: b.get("metadata", {}).get("created", ""))
        if not selected:
            return "没有 archive 条目。"
        return "=== Archive Timeline ===\n" + "\n---\n".join(
            _format_wander_entry(b, marks_by_bucket.get(b.get("id", ""), []), include_full_content=True, show_bucket_id=True)
            for b in selected
        )

    if mode in ("letter", "writing", "letter_human", "window"):
        match_domains = {mode}
        if mode == "letter":
            match_domains.add("letter_human")
        selected = [
            b for b in buckets
            if (
                mode == "window"
                or not is_settled(b)
            )
            and (
                match_domains & set(_bucket_domains(b.get("metadata", {})))
                or match_domains & set(_bucket_tags(b.get("metadata", {})))
            )
        ]
        selected.sort(key=lambda b: b.get("metadata", {}).get("created", ""))
        if not selected:
            return f"没有 {mode} 条目。"
        return f"=== {mode} Timeline ===\n" + "\n---\n".join(
            _format_wander_entry(b, marks_by_bucket.get(b.get("id", ""), []), include_full_content=True, show_bucket_id=True)
            for b in selected
        )

    if mode == "unresolved":
        selected = [
            b for b in buckets
            if _is_unresolved_bucket(b, marks_by_bucket.get(b.get("id", ""), []))
        ]
        selected.sort(key=lambda b: b.get("metadata", {}).get("created", ""))
        if not selected:
            return "没有悬置条目。"
        return "=== Unresolved / 悬置 ===\n" + "\n---\n".join(
            _format_wander_entry(b, marks_by_bucket.get(b.get("id", ""), []), include_full_content=True)
            for b in selected
        )

    if mode == "trace":
        trace_limit = max(1, min(int(limit or 15), 15))

        def _type_label(b: dict) -> str:
            meta = b.get("metadata", {})
            mark_rows = marks_by_bucket.get(b.get("id", ""), [])
            unresolved = _is_unresolved_bucket(b, mark_rows)
            if str(meta.get("type", "")).lower() == "feel":
                base = "feel"
            else:
                domains = _bucket_domains(meta)
                tags = _bucket_tags(meta)
                if "letter_human" in domains or "letter_human" in tags:
                    base = "letter_human"
                elif "letter" in domains or "letter" in tags:
                    base = "letter"
                elif "writing" in domains or "writing" in tags:
                    base = "writing"
                elif "window" in domains or "window" in tags:
                    base = "window"
                else:
                    base = "memory"
            if unresolved and base != "feel":
                base = "unresolved"
            # 原本是letter/writing/window,但已经被认够次数晋升inner——
            # 两个标签都要看见,不能被_guess_wander_domain的优先级collapse掉
            if base != "feel" and _guess_wander_domain(b, mark_rows) == "inner":
                return f"{base}→inner"
            return base

        selected = [
            b for b in buckets
            if (
                not is_settled(b)
                and (
                    str(b.get("metadata", {}).get("type", "")).lower() == "feel"
                    or (
                        str(b.get("metadata", {}).get("type", "")).lower() not in ("breath", "dream", "permanent")
                        and _guess_wander_domain(b, marks_by_bucket.get(b.get("id", ""), []))
                        in {"memory", "inner", "letter", "letter_human", "writing", "window"}
                    )
                )
            )
        ]
        selected.sort(key=lambda b: b.get("metadata", {}).get("created", ""))
        selected = selected[:trace_limit]
        if not selected:
            return "null"
        return "=== Trace ===\n" + "\n---\n".join(
            f"〔{_type_label(b)}〕" + _format_wander_entry(
                b, marks_by_bucket.get(b.get("id", ""), []), include_full_content=True, show_bucket_id=True
            )
            for b in selected
        )

    if mode == "inner":
        selected = [
            b for b in buckets
            if _guess_wander_domain(b, marks_by_bucket.get(b.get("id", ""), [])) == "inner"
        ]
        selected.sort(key=lambda b: b.get("metadata", {}).get("created", ""))
        if not selected:
            return "没有 inner 条目。"
        return "=== Inner Core ===\n" + "\n---\n".join(
            _format_wander_entry(b, marks_by_bucket.get(b.get("id", ""), []), include_full_content=True)
            for b in selected
        )

    return (
        "mode 必须是 flotsam / archive / letter / writing / letter_human / "
        "window / unresolved / inner / trails。全量关键词轨迹用 trace。"
    )


@mcp.tool(name="trace")
async def trace(query: str, limit: int = 15) -> str:
    """按关键词搜索记忆。"""
    if not (query or "").strip():
        return "trace 要带 query。它是全量轨迹搜索，不是 Breath 浮现。"
    return await wander(mode="trace", query=query, limit=limit)


# =============================================================
# Tool: recall — per-turn contextual memory retrieval
# 逐轮召回：根据用户消息自动匹配相关记忆
# =============================================================
_recall_seen: set[str] = set()

@mcp.tool(name="recall")
async def recall(message: str) -> str:
    """逐轮召回：把当前用户消息扔进来，返回相关记忆。每轮自动调用，不需要用户提关键词。同session内已召回的不重复返回。"""
    if not (message or "").strip():
        return ""

    recall_config = config.get("recall", {})
    threshold = recall_config.get("confidence_threshold", 55)
    max_results = recall_config.get("max_results", 5)

    import re as _re
    import jieba

    text = (message or "").strip()
    seg_terms = [w for w in jieba.cut(text) if len(w.strip()) >= 2]
    split_terms = [t.strip() for t in _re.split(r"[\s,，、。！？!?.]+", text) if len(t.strip()) >= 2]
    terms = list(dict.fromkeys(seg_terms + split_terms))
    if not terms:
        return ""

    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
    except Exception as e:
        logger.warning(f"recall: failed to list buckets: {e}")
        return ""

    from rapidfuzz import fuzz

    scored_buckets = []
    for b in all_buckets:
        bid = b.get("id", "")
        if bid in _recall_seen:
            continue
        meta = b.get("metadata", {})
        if meta.get("digested") or meta.get("resolved"):
            continue
        haystack = "\n".join([
            str(meta.get("name", "")),
            " ".join(str(x) for x in meta.get("domain", []) if x),
            " ".join(str(x) for x in meta.get("tags", []) if x),
            strip_wikilinks(b.get("content", "")),
        ]).lower()

        best_score = 0
        for term in terms:
            score = fuzz.partial_ratio(term.lower(), haystack)
            if score > best_score:
                best_score = score

        if best_score >= threshold:
            scored_buckets.append((best_score, b))

    scored_buckets.sort(key=lambda x: x[0], reverse=True)
    results = scored_buckets[:max_results]

    if not results:
        return ""

    parts = []
    for score, b in results:
        bid = b.get("id", "")
        _recall_seen.add(bid)
        meta = b.get("metadata", {})
        name = meta.get("name", "") or bid
        created = str(meta.get("created", ""))[:10]
        track_tag = _track_label(meta.get("track", ""))
        content = strip_wikilinks(b.get("content", "")).strip()
        preview = content[:100].rstrip()
        if len(content) > 100:
            preview += "..."
        parts.append(f"[{created}] {name}{track_tag}\n{preview}")

    return "=== Recall ===\n" + "\n---\n".join(parts)


@mcp.tool()
async def wander_mark(bucket_id: str, mark: str, note: str = "") -> str:
    """对骨架记忆archive/unresolved/inner进行mark，认/不认/悬置；多次认会晋升inner。"""
    bucket_id = (bucket_id or "").strip()
    mark = _normalize_wander_mark(mark)
    note = (note or "").strip()

    if not bucket_id:
        return "请提供有效的 bucket_id。"
    if mark not in VALID_WANDER_MARKS:
        return "mark 必须是 认 / 不认 / 悬置。"

    bucket = await bucket_mgr.get(bucket_id)
    if not bucket:
        return f"未找到记忆桶: {bucket_id}"

    meta = bucket.get("metadata", {})
    domains = meta.get("domain", [])
    if isinstance(domains, str):
        domains = [domains]
    domains = [d for d in domains if d]

    ts = now_iso()
    conn = _marks_conn()
    try:
        conn.execute(
            "INSERT INTO marks (bucket_id, mark, note, timestamp) VALUES (?, ?, ?, ?)",
            (bucket_id, mark, note, ts),
        )
        conn.commit()
    finally:
        conn.close()

    mark_rows = _load_all_marks().get(bucket_id, [])
    counts = _mark_counts(mark_rows)

    suffix = ""

    # Auto-promote to inner: 认>=3 and cross at least 2 dates
    if counts["认"] >= 3 and _has_cross_date_recognition(mark_rows):
        lower_domains = {str(d).lower() for d in domains}
        if "inner" not in lower_domains:
            domains.append("inner")
            try:
                await bucket_mgr.update(bucket_id, domain=domains)
                suffix += " 🌟 已晋升 inner"
            except Exception as e:
                logger.warning(f"wander_mark failed to promote to inner: {e}")

    # Auto-demote from inner: 不认>=2
    if counts["不认"] >= 2 and any(str(d).lower() == "inner" for d in domains):
        domains = [d for d in domains if str(d).lower() != "inner"]
        try:
            await bucket_mgr.update(bucket_id, domain=domains)
            suffix += "；不认累计>=2，已移出 inner"
        except Exception as e:
            logger.warning(f"wander_mark failed to demote from inner: {e}")

    engaged = _maybe_engage_pulse("wander_mark")
    if engaged:
        suffix += f" · pulse_engaged={engaged.get('via')}:{engaged.get('engaged')}"

    return (
        f"已标记 {bucket_id}: {mark} @ {ts}{suffix}\n"
        f"当前批注统计：认:{counts['认']} / 不认:{counts['不认']} / 悬置:{counts['悬置']}"
    )


# =============================================================
# Tool 6: dream — Dreaming, digest recent memories
# 工具 6：dream — 做梦，消化最近的记忆
#
# Reads recent surface-level buckets (≤10), returns them for
# Claude to introspect under prompt guidance.
# 读取最近新增的表层桶（≤10个），返回给 Claude 在提示词引导下自主思考。
# Claude then decides: resolve some, write feels, or do nothing.
# =============================================================
async def _refresh_dream_cache(exclude_bucket_ids: set[str] | None = None):
    """生成新的梦境文本并写入缓存(latest_dream.json)，dream()和breath()共用同一份生成逻辑。
    返回(dream_text, parts, recent, all_buckets)；all_buckets为None表示记忆系统不可访问。"""
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
    except Exception as e:
        logger.error(f"Dream cache refresh failed to list buckets: {e}")
        return "", [], [], None

    # Dream Veil deliberately reuses the newest memory field even when Breath
    # surfaced some of the same buckets. A dream is a recomposition of recent
    # life, not a second diversity sampler.
    candidates = [
        b for b in (_breath_memory_candidates(all_buckets) + _breath_feel_candidates(all_buckets))
    ]
    candidates.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
    recent_pool = candidates[:10]
    recent_count = min(5, len(recent_pool))
    recent = random.sample(recent_pool, recent_count) if recent_count else []

    # --- 记忆碰撞（她 2026-09-15 的点子）：从全库随机抽两条相距很远的记忆，
    # 混进梦的素材里强行相撞。不做语义匹配——乱点鸳鸯谱才是梦的价值。
    collide = []
    try:
        deep_pool = [b for b in candidates[10:] if b.get("content")]
        if len(deep_pool) >= 2:
            collide = random.sample(deep_pool, 2)
            # 两条若创建时间相距不足 7 天，重抽一次（尽力而为，不强求）
            def _created(b):
                return b["metadata"].get("created", "")[:10]
            if collide[0] and collide[1] and _created(collide[0])[:7] == _created(collide[1])[:7]:
                retry = random.sample(deep_pool, 2)
                if retry[0]["metadata"].get("created", "")[:7] != retry[1]["metadata"].get("created", "")[:7]:
                    collide = retry
            recent = collide + recent[:3]  # 碰撞对置顶 + 近期3条打底
    except Exception as e:
        logger.warning("Dream collide sampling failed: %s", e)
    if not recent:
        return "", [], [], all_buckets

    parts = []
    for b in recent:
        meta = b["metadata"]
        name = meta.get("name") or ""  # feel桶name为None时不fallback到UUID
        created = meta.get("created", "")[:16].replace("T", " ")
        resolved_tag = " ✓" if meta.get("resolved", False) else ""
        raw_content = strip_wikilinks(b["content"])
        readable = dehydrator._extract_readable_content(raw_content)
        header = f"[{created}] {name}{resolved_tag}" if name else f"[{created}]{resolved_tag}"
        parts.append(
            f"{header}\n"
            f"{readable}"
        )

    # --- Optional OpenAI-compatible dream generation ---
    dream_text = ""
    try:
        if dehydrator.api_available and parts:
            fragments = "\n---\n".join(parts)
            prompt = (
                "The following are sourced memory fragments. The FIRST TWO come from moments "
                "far apart in time that never met in waking life — let them collide in the "
                "dream: objects, places, and gestures from one may appear inside the other. "
                "Recombine all fragments into a short first-person dream. Be nonlinear and "
                "image-driven; do not summarize, explain, diagnose, or invent biographical "
                "facts. Return one paragraph of roughly 120-180 Chinese characters (or a "
                "similarly compact length in the language of the sources).\n\nMemory fragments:\n" + fragments
            )
            messages = [{"role": "user", "content": prompt}]
            for attempt in range(2):
                response = await dehydrator.client.chat.completions.create(
                    model=dehydrator.model,
                    messages=messages,
                    max_tokens=300,
                    temperature=0.9,
                )
                dream_text = " ".join((response.choices[0].message.content or "").split())
                if attempt or len(dream_text) >= 120:
                    break
                messages = [
                    *messages,
                    {"role": "assistant", "content": dream_text},
                    {"role": "user", "content": "Rewrite once at a fuller 120-180 Chinese characters (or equivalent compact length). Return only one paragraph."},
                ]
    except Exception as e:
        logger.warning("Dream generation failed: %s", e)

    if dream_text:
        try:
            import json as _j, time as _t
            with open(_bucket_path("latest_dream.json"), "w") as _f:
                _j.dump({
                    "dream": dream_text,
                    "ts": _t.time(),
                    "fragments": [b.get("id") for b in recent],
                }, _f)
        except Exception:
            pass

    return dream_text, parts, recent, all_buckets


@mcp.custom_route("/api/dream/refresh", methods=["POST"])
async def api_dream_refresh(request):
    """Generate and persist the shared Dream Veil used by every Agent body."""
    from starlette.responses import JSONResponse
    err = _require_auth(request, allow_service=True)
    if err:
        return err
    try:
        body = await request.json()
    except Exception:
        body = {}
    idempotency_key = str(body.get("idempotency_key") or "").strip()
    if idempotency_key and not re.fullmatch(r"[A-Za-z0-9._:+-]{8,180}", idempotency_key):
        return JSONResponse({"ok": False, "error": "invalid idempotency_key"}, status_code=400)

    state_path = _bucket_path("dream_refresh_state.json")
    async with _dream_refresh_lock:
        previous = {}
        try:
            with open(state_path, encoding="utf-8") as f:
                previous = _json_lib.load(f)
        except Exception:
            pass
        if idempotency_key and previous.get("idempotency_key") == idempotency_key:
            return JSONResponse(
                {**previous.get("response", {}), "ok": True, "duplicate": True},
                headers={"Access-Control-Allow-Origin": "*"},
            )

        dream_text, _, recent, all_buckets = await _refresh_dream_cache()
        if all_buckets is None:
            return JSONResponse(
                {"ok": False, "error": "memory unavailable"},
                status_code=503,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        if not dream_text:
            return JSONResponse(
                {"ok": False, "error": "dream generation produced no content"},
                status_code=502,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        response_body = {
            "ok": True,
            "dream": dream_text,
            "ts": time.time(),
            "fragments": [b.get("id") for b in recent],
            "duplicate": False,
        }
        if idempotency_key:
            temp_path = f"{state_path}.tmp-{os.getpid()}"
            with open(temp_path, "w", encoding="utf-8") as f:
                _json_lib.dump({"idempotency_key": idempotency_key, "response": response_body}, f, ensure_ascii=False)
            os.replace(temp_path, state_path)
        return JSONResponse(response_body, headers={"Access-Control-Allow-Origin": "*"})


async def dream() -> str:
    """做梦——旧内部自省入口，不再暴露为 MCP 工具。"""
    await decay_engine.ensure_started()

    dream_text, parts, recent, all_buckets = await _refresh_dream_cache()
    if all_buckets is None:
        return "记忆系统暂时无法访问。"
    if not recent:
        return "没有需要消化的feel。"

    header = "=== 梦境 ===\n"

    # --- Connection hint: find most similar pair via embeddings ---
    connection_hint = ""
    if embedding_engine and embedding_engine.enabled and len(recent) >= 2:
        try:
            best_pair = None
            best_sim = 0.0
            ids = [b["id"] for b in recent]
            names = {b["id"]: b["metadata"].get("name", b["id"]) for b in recent}
            embeddings = {}
            for bid in ids:
                emb = await embedding_engine.get_embedding(bid)
                if emb is not None:
                    embeddings[bid] = emb
            for i, id_a in enumerate(ids):
                for id_b in ids[i+1:]:
                    if id_a in embeddings and id_b in embeddings:
                        sim = embedding_engine._cosine_similarity(embeddings[id_a], embeddings[id_b])
                        if sim > best_sim:
                            best_sim = sim
                            best_pair = (id_a, id_b)
            if best_pair and best_sim > 0.5:
                connection_hint = (
                    f"\n💭 [{names[best_pair[0]]}] 和 [{names[best_pair[1]]}] "
                    f"似乎有关联 (相似度:{best_sim:.2f})——不替你下结论，你自己想。\n"
                )
        except Exception as e:
            logger.warning(f"Dream connection hint failed: {e}")

    # --- Feel crystallization hint: detect repeated feel themes ---
    crystal_hint = ""
    if embedding_engine and embedding_engine.enabled:
        try:
            feels = [b for b in all_buckets if b["metadata"].get("type") == "feel"]
            if len(feels) >= 3:
                feel_embeddings = {}
                for f in feels:
                    emb = await embedding_engine.get_embedding(f["id"])
                    if emb is not None:
                        feel_embeddings[f["id"]] = emb
                # Find clusters: feels with similarity > 0.7 to at least 2 others
                for fid, femb in feel_embeddings.items():
                    similar_feels = []
                    for oid, oemb in feel_embeddings.items():
                        if oid != fid:
                            sim = embedding_engine._cosine_similarity(femb, oemb)
                            if sim > 0.7:
                                similar_feels.append(oid)
                    if len(similar_feels) >= 2:
                        feel_bucket = next((f for f in feels if f["id"] == fid), None)
                        if feel_bucket and not feel_bucket["metadata"].get("pinned"):
                            content_preview = strip_wikilinks(feel_bucket["content"][:80])
                            crystal_hint = (
                                f"\n🔮 你已经写过 {len(similar_feels)+1} 条相似的 feel "
                                f"（围绕「{content_preview}…」）。"
                                f"如果这已经是确信而不只是感受了，"
                                f"你可以用 hold(content=\"...\", pinned=True) 升级它。"
                                f"不急，你自己决定。\n"
                            )
                            break
        except Exception as e:
            logger.warning(f"Dream crystallization hint failed: {e}")

    feel_list = "\n---\n".join(parts)
    if dream_text:
        final_text = header + dream_text + "\n\n---\n\n" + feel_list + connection_hint + crystal_hint
    else:
        final_text = header + feel_list + connection_hint + crystal_hint
    await _fire_webhook("dream", {"recent": len(recent), "chars": len(final_text)})
    return final_text


# =============================================================
# Dashboard API endpoints (for lightweight Web UI)
# 仪表板 API（轻量 Web UI 用）
# =============================================================
@mcp.custom_route("/api/buckets", methods=["GET"])
async def api_buckets(request):
    """List all buckets with metadata (no content for efficiency)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=True)
        marks_by_bucket = _load_all_marks()
        result = []
        for b in all_buckets:
            meta = b.get("metadata", {})
            mark_rows = marks_by_bucket.get(b["id"], [])
            mark_counts = _mark_counts(mark_rows)
            result.append({
                "id": b["id"],
                "name": meta.get("name", b["id"]),
                "type": meta.get("type", "dynamic"),
                "domain": meta.get("domain", []),
                "tags": meta.get("tags", []),
                "marks": {"认": mark_counts["认"], "不认": mark_counts["不认"], "悬置": mark_counts["悬置"]},
                "unresolved": _is_unresolved_bucket(b, mark_rows),
                "chord": meta.get("chord", ""),
                "signal": meta.get("signal", ""),
                "signal_hints": meta.get("signal_hints", {}),
                "drive_tags": meta.get("drive_tags", {}),
                "valence": meta.get("valence", 0.5),
                "arousal": meta.get("arousal", 0.3),
                "model_valence": meta.get("model_valence"),
                "importance": meta.get("importance", 5),
                "resolved": meta.get("resolved", False),
                "pinned": meta.get("pinned", False),
                "digested": meta.get("digested", False),
                "created": meta.get("created", ""),
                "last_active": meta.get("last_active", ""),
                "activation_count": meta.get("activation_count", 1),
                "score": decay_engine.calculate_score(meta),
                "content_preview": strip_wikilinks(b.get("content", ""))[:200],
            })
        result.sort(key=lambda x: x["score"], reverse=True)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/bucket/{bucket_id}", methods=["GET"])
async def api_bucket_detail(request):
    """Get full bucket content by ID."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    bucket_id = request.path_params["bucket_id"]
    bucket = await bucket_mgr.get(bucket_id)
    if not bucket:
        return JSONResponse({"error": "not found"}, status_code=404)
    meta = bucket.get("metadata", {})
    return JSONResponse({
        "id": bucket["id"],
        "metadata": meta,
        "content": strip_wikilinks(bucket.get("content", "")),
        "score": decay_engine.calculate_score(meta),
    })


@mcp.custom_route("/api/bucket/{bucket_id}/update", methods=["POST"])
async def api_bucket_update(request):
    """Update bucket content and/or metadata."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    bucket_id = request.path_params["bucket_id"]
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    bucket = await bucket_mgr.get(bucket_id)
    if not bucket:
        return JSONResponse({"error": "not found"}, status_code=404)

    kwargs = {}
    if "content" in body:
        if not isinstance(body["content"], str):
            return JSONResponse({"error": "content must be a string"}, status_code=400)
        kwargs["content"] = body["content"]
    if "resolved" in body:
        kwargs["resolved"] = bool(body["resolved"])
    if "digested" in body:
        kwargs["digested"] = bool(body["digested"])
    if "importance" in body:
        try:
            kwargs["importance"] = max(1, min(10, int(body["importance"])))
        except (TypeError, ValueError):
            return JSONResponse({"error": "importance must be an integer 1-10"}, status_code=400)
    if "activation_count" in body:
        try:
            kwargs["activation_count"] = max(1, min(999, int(body["activation_count"])))
        except (TypeError, ValueError):
            return JSONResponse({"error": "activation_count must be an integer"}, status_code=400)
    if "valence" in body:
        try:
            kwargs["valence"] = max(0.0, min(1.0, float(body["valence"])))
        except (TypeError, ValueError):
            return JSONResponse({"error": "valence must be a number 0-1"}, status_code=400)
    if "arousal" in body:
        try:
            kwargs["arousal"] = max(0.0, min(1.0, float(body["arousal"])))
        except (TypeError, ValueError):
            return JSONResponse({"error": "arousal must be a number 0-1"}, status_code=400)
    if "pinned" in body:
        kwargs["pinned"] = bool(body["pinned"])
    if "name" in body:
        kwargs["name"] = body["name"]
    if "type" in body:
        bucket_type = body["type"]
        if bucket_type not in {"dynamic", "permanent", "feel"}:
            return JSONResponse({"error": "type must be dynamic, permanent, or feel"}, status_code=400)
        kwargs["type"] = bucket_type
    if "tags" in body:
        tags = body["tags"]
        if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
            return JSONResponse({"error": "tags must be a list of strings"}, status_code=400)
        kwargs["tags"] = list(dict.fromkeys(tag.strip() for tag in tags if tag.strip()))
    if "chord" in body:
        if not isinstance(body["chord"], str):
            return JSONResponse({"error": "chord must be a string"}, status_code=400)
        kwargs["chord"] = body["chord"].strip()
    if "signal" in body:
        if not isinstance(body["signal"], str):
            return JSONResponse({"error": "signal must be a string"}, status_code=400)
        signal = body["signal"].strip()
        kwargs["signal"] = signal
        kwargs["signal_hints"] = _parse_signal_hints(signal)
    if body.get("preserve_last_active"):
        kwargs["_preserve_last_active"] = True

    if not kwargs:
        return JSONResponse({"error": "nothing to update"}, status_code=400)

    try:
        updated = await bucket_mgr.update(bucket_id, **kwargs)
        if not updated:
            return JSONResponse({"error": "bucket could not be updated"}, status_code=500)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/bucket/{bucket_id}/delete", methods=["POST"])
async def api_bucket_delete(request):
    """Delete a bucket by ID."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    bucket_id = request.path_params["bucket_id"]
    try:
        file_path = bucket_mgr._find_bucket_file(bucket_id)
        if not file_path:
            return JSONResponse({"error": "not found"}, status_code=404)
        os.remove(file_path)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/search", methods=["GET"])
async def api_search(request):
    """Search buckets by query."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    query = request.query_params.get("q", "")
    if not query:
        return JSONResponse({"error": "missing q parameter"}, status_code=400)
    try:
        matches = await bucket_mgr.search(query, limit=10)
        result = []
        for b in matches:
            meta = b.get("metadata", {})
            result.append({
                "id": b["id"],
                "name": meta.get("name", b["id"]),
                "score": b.get("score", 0),
                "domain": meta.get("domain", []),
                "valence": meta.get("valence", 0.5),
                "arousal": meta.get("arousal", 0.3),
                "content_preview": strip_wikilinks(b.get("content", ""))[:200],
            })
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/network", methods=["GET"])
async def api_network(request):
    """Get embedding similarity network for visualization."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        nodes = []
        edges = []
        embeddings = {}

        for b in all_buckets:
            meta = b.get("metadata", {})
            bid = b["id"]
            nodes.append({
                "id": bid,
                "name": meta.get("name", bid),
                "type": meta.get("type", "dynamic"),
                "domain": meta.get("domain", []),
                "tags": meta.get("tags", []),
                "valence": meta.get("valence", 0.5),
                "arousal": meta.get("arousal", 0.3),
                "score": decay_engine.calculate_score(meta),
                "resolved": meta.get("resolved", False),
                "pinned": meta.get("pinned", False),
                "digested": meta.get("digested", False),
            })
            if embedding_engine and embedding_engine.enabled:
                emb = await embedding_engine.get_embedding(bid)
                if emb is not None:
                    embeddings[bid] = emb

        # Build edges from embeddings (similarity > 0.5)
        ids = list(embeddings.keys())
        for i, id_a in enumerate(ids):
            for id_b in ids[i+1:]:
                sim = embedding_engine._cosine_similarity(embeddings[id_a], embeddings[id_b])
                if sim > 0.5:
                    edges.append({"source": id_a, "target": id_b, "similarity": round(sim, 3)})

        return JSONResponse({"nodes": nodes, "edges": edges})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/breath-debug", methods=["GET"])
async def api_breath_debug(request):
    """Debug endpoint: simulate breath scoring and return per-bucket breakdown."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    query = request.query_params.get("q", "")
    q_valence = request.query_params.get("valence")
    q_arousal = request.query_params.get("arousal")
    q_valence = float(q_valence) if q_valence else None
    q_arousal = float(q_arousal) if q_arousal else None

    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        results = []

        for bucket in all_buckets:
            if _is_wander_only_bucket(bucket):
                continue
            meta = bucket.get("metadata", {})
            bid = bucket["id"]
            try:
                components = _breath_recall_components(bucket, query, q_valence, q_arousal)

                results.append({
                    "id": bid,
                    "name": meta.get("name", bid),
                    "domain": meta.get("domain", []),
                    "type": meta.get("type", "dynamic"),
                    "resolved": meta.get("resolved", False),
                    "pinned": meta.get("pinned", False),
                    "scores": components["scores"],
                    "weights": components["weights"],
                    "raw_total": components["raw_total"],
                    "normalized": components["normalized"],
                    "passed_threshold": components["normalized"] >= bucket_mgr.fuzzy_threshold,
                })
            except Exception:
                continue

        results.sort(key=lambda x: x["normalized"], reverse=True)
        passed = [r for r in results if r["passed_threshold"]]
        return JSONResponse({
            "query": query,
            "valence": q_valence,
            "arousal": q_arousal,
            "weights": {
                "topic": bucket_mgr.w_topic,
                "emotion": bucket_mgr.w_emotion,
                "time": bucket_mgr.w_time,
                "importance": bucket_mgr.w_importance,
            },
            "threshold": bucket_mgr.fuzzy_threshold,
            "total_candidates": len(results),
            "passed_count": len(passed),
            "results": results[:50],  # top 50 for debug
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/config", methods=["GET"])
async def api_config_get(request):
    """Get current runtime config (safe fields only, API key masked)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    dehy = config.get("dehydration", {})
    emb = config.get("embedding", {})
    api_key = dehy.get("api_key", "")
    masked_key = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else ("***" if api_key else "")
    return JSONResponse({
        "dehydration": {
            "model": dehy.get("model", ""),
            "base_url": dehy.get("base_url", ""),
            "api_key_masked": masked_key,
            "max_tokens": dehy.get("max_tokens", 1024),
            "temperature": dehy.get("temperature", 0.1),
        },
        "embedding": {
            "enabled": emb.get("enabled", False),
            "model": emb.get("model", ""),
        },
        "merge_threshold": config.get("merge_threshold", 75),
        "transport": config.get("transport", "stdio"),
        "buckets_dir": config.get("buckets_dir", ""),
    })


@mcp.custom_route("/api/config", methods=["POST"])
async def api_config_update(request):
    """Hot-update runtime config. Optionally persist to config.yaml."""
    from starlette.responses import JSONResponse
    import yaml
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    updated = []

    # --- Dehydration config ---
    if "dehydration" in body:
        d = body["dehydration"]
        dehy = config.setdefault("dehydration", {})
        for key in ("model", "base_url", "max_tokens", "temperature"):
            if key in d:
                dehy[key] = d[key]
                updated.append(f"dehydration.{key}")
        if "api_key" in d and d["api_key"]:
            dehy["api_key"] = d["api_key"]
            updated.append("dehydration.api_key")
        # Hot-reload dehydrator
        dehydrator.model = dehy.get("model", "deepseek-v4-flash")
        dehydrator.base_url = dehy.get("base_url", "")
        dehydrator.api_key = dehy.get("api_key", "")
        if hasattr(dehydrator, "client") and dehydrator.api_key:
            from openai import AsyncOpenAI
            dehydrator.client = AsyncOpenAI(
                api_key=dehydrator.api_key,
                base_url=dehydrator.base_url,
            )

    # --- Embedding config ---
    if "embedding" in body:
        e = body["embedding"]
        emb = config.setdefault("embedding", {})
        if "enabled" in e:
            emb["enabled"] = bool(e["enabled"])
            embedding_engine.enabled = emb["enabled"]
            updated.append("embedding.enabled")
        if "model" in e:
            emb["model"] = e["model"]
            embedding_engine.model = emb["model"]
            updated.append("embedding.model")

    # --- Merge threshold ---
    if "merge_threshold" in body:
        config["merge_threshold"] = int(body["merge_threshold"])
        updated.append("merge_threshold")

    # --- Persist to config.yaml if requested ---
    if body.get("persist", False):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
        try:
            save_config = {}
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    save_config = yaml.safe_load(f) or {}

            if "dehydration" in body:
                sc_dehy = save_config.setdefault("dehydration", {})
                for key in ("model", "base_url", "max_tokens", "temperature"):
                    if key in body["dehydration"]:
                        sc_dehy[key] = body["dehydration"][key]
                # Never persist api_key to yaml (use env var)

            if "embedding" in body:
                sc_emb = save_config.setdefault("embedding", {})
                for key in ("enabled", "model"):
                    if key in body["embedding"]:
                        sc_emb[key] = body["embedding"][key]

            if "merge_threshold" in body:
                save_config["merge_threshold"] = int(body["merge_threshold"])

            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(save_config, f, default_flow_style=False, allow_unicode=True)
            updated.append("persisted_to_yaml")
        except Exception as e:
            return JSONResponse({"error": f"persist failed: {e}", "updated": updated}, status_code=500)

    return JSONResponse({"updated": updated, "ok": True})


# =============================================================
# /api/marginalia — manual Shape Trace maintenance for breath.
# This is dashboard-only; the MCP marginalia tool is intentionally not exposed.
# =============================================================

@mcp.custom_route("/api/marginalia", methods=["GET"])
async def api_marginalia_get(request):
    """Read the manual Shape Trace text used by breath."""
    from starlette.responses import JSONResponse
    import json
    err = _require_auth(request)
    if err: return err
    content = ""
    ts = None
    try:
        if os.path.exists(MARGINALIA_PATH):
            with open(MARGINALIA_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            content = data.get("letter", "") or ""
            ts = data.get("ts")
    except Exception as e:
        return JSONResponse({"error": f"failed to read marginalia: {e}"}, status_code=500)
    return JSONResponse({
        "content": content,
        "ts": ts,
        "max_chars": MARGINALIA_MAX_CHARS,
    })


@mcp.custom_route("/api/marginalia", methods=["POST"])
async def api_marginalia_set(request):
    """Overwrite the manual Shape Trace text used by breath."""
    from starlette.responses import JSONResponse
    import json
    import time as _time
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    raw = body.get("content", "")
    if not isinstance(raw, str):
        return JSONResponse({"error": "content must be a string"}, status_code=400)
    content = raw[:MARGINALIA_MAX_CHARS]
    ts = _time.time()

    try:
        os.makedirs(os.path.dirname(MARGINALIA_PATH), exist_ok=True)
        with open(MARGINALIA_PATH, "w", encoding="utf-8") as f:
            json.dump({"letter": content, "ts": ts}, f, ensure_ascii=False)
    except Exception as e:
        return JSONResponse({"error": f"failed to write marginalia: {e}"}, status_code=500)

    return JSONResponse({
        "ok": True,
        "content": content,
        "chars": len(content),
        "ts": ts,
        "max_chars": MARGINALIA_MAX_CHARS,
    })


# =============================================================
# /api/host-vault — read/write the host-side OMBRE_HOST_VAULT_DIR
# 用于在 Dashboard 设置 docker-compose 挂载的宿主机记忆桶目录。
# 写入项目根目录的 .env 文件，需 docker compose down/up 才能生效。
# =============================================================

def _project_env_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def _read_env_var(name: str) -> str:
    """Return current value of `name` from process env first, then .env file (best-effort)."""
    val = os.environ.get(name, "").strip()
    if val:
        return val
    env_path = _project_env_path()
    if not os.path.exists(env_path):
        return ""
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                if k.strip() == name:
                    return v.strip().strip('"').strip("'")
    except Exception:
        pass
    return ""


def _write_env_var(name: str, value: str) -> None:
    """
    Idempotent upsert of `NAME=value` in project .env. Creates the file if missing.
    Preserves other entries verbatim. Quotes values containing spaces.
    """
    env_path = _project_env_path()
    quoted = f'"{value}"' if value and (" " in value or "#" in value) else value
    new_line = f"{name}={quoted}\n"

    lines: list[str] = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

    replaced = False
    for i, raw in enumerate(lines):
        stripped = raw.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        k, _, _v = stripped.partition("=")
        if k.strip() == name:
            lines[i] = new_line
            replaced = True
            break
    if not replaced:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines.append(new_line)

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(lines)


@mcp.custom_route("/api/host-vault", methods=["GET"])
async def api_host_vault_get(request):
    """Read the current OMBRE_HOST_VAULT_DIR (process env > project .env)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    value = _read_env_var("OMBRE_HOST_VAULT_DIR")
    return JSONResponse({
        "value": value,
        "source": "env" if os.environ.get("OMBRE_HOST_VAULT_DIR", "").strip() else ("file" if value else ""),
        "env_file": _project_env_path(),
    })


@mcp.custom_route("/api/host-vault", methods=["POST"])
async def api_host_vault_set(request):
    """
    Persist OMBRE_HOST_VAULT_DIR to the project .env file.
    Body: {"value": "/path/to/vault"}  (empty string clears the entry)
    Note: container restart is required for docker-compose to pick up the new mount.
    """
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    raw = body.get("value", "")
    if not isinstance(raw, str):
        return JSONResponse({"error": "value must be a string"}, status_code=400)
    value = raw.strip()

    # Reject characters that would break .env / shell parsing
    if "\n" in value or "\r" in value or '"' in value or "'" in value:
        return JSONResponse({"error": "value must not contain quotes or newlines"}, status_code=400)

    try:
        _write_env_var("OMBRE_HOST_VAULT_DIR", value)
    except Exception as e:
        return JSONResponse({"error": f"failed to write .env: {e}"}, status_code=500)

    return JSONResponse({
        "ok": True,
        "value": value,
        "env_file": _project_env_path(),
        "note": "已写入 .env；需在宿主机执行 `docker compose down && docker compose up -d` 让新挂载生效。",
    })


# =============================================================
# Import API — conversation history import
# 导入 API — 对话历史导入
# =============================================================

@mcp.custom_route("/api/import/upload", methods=["POST"])
async def api_import_upload(request):
    """Upload a conversation file and start import."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err

    if import_engine.is_running:
        return JSONResponse({"error": "Import already running"}, status_code=409)

    content_type = request.headers.get("content-type", "")
    filename = ""

    try:
        if "multipart/form-data" in content_type:
            form = await request.form()
            file_field = form.get("file")
            if not file_field:
                return JSONResponse({"error": "No file field"}, status_code=400)
            raw_bytes = await file_field.read()
            filename = getattr(file_field, "filename", "upload")
            raw_content = raw_bytes.decode("utf-8", errors="replace")
        else:
            body = await request.body()
            raw_content = body.decode("utf-8", errors="replace")
            # Try to get filename from query params
            filename = request.query_params.get("filename", "upload")

        if not raw_content.strip():
            return JSONResponse({"error": "Empty file"}, status_code=400)

        preserve_raw = request.query_params.get("preserve_raw", "").lower() in ("1", "true")
        resume = request.query_params.get("resume", "").lower() in ("1", "true")

    except Exception as e:
        return JSONResponse({"error": f"Failed to read upload: {e}"}, status_code=400)

    # Start import in background
    async def _run_import():
        try:
            await import_engine.start(raw_content, filename, preserve_raw, resume)
        except Exception as e:
            logger.error(f"Import failed: {e}")

    asyncio.create_task(_run_import())

    return JSONResponse({
        "status": "started",
        "filename": filename,
        "size_bytes": len(raw_content.encode()),
    })


@mcp.custom_route("/api/import/status", methods=["GET"])
async def api_import_status(request):
    """Get current import progress."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    return JSONResponse(import_engine.get_status())


@mcp.custom_route("/api/import/pause", methods=["POST"])
async def api_import_pause(request):
    """Pause the running import."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    if not import_engine.is_running:
        return JSONResponse({"error": "No import running"}, status_code=400)
    import_engine.pause()
    return JSONResponse({"status": "pause_requested"})


@mcp.custom_route("/api/import/patterns", methods=["GET"])
async def api_import_patterns(request):
    """Detect high-frequency patterns after import."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        patterns = await import_engine.detect_patterns()
        return JSONResponse({"patterns": patterns})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/import/results", methods=["GET"])
async def api_import_results(request):
    """List recently imported/created buckets for review."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        limit = int(request.query_params.get("limit", "50"))
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        # Sort by created time, newest first
        all_buckets.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
        results = []
        for b in all_buckets[:limit]:
            results.append({
                "id": b["id"],
                "name": b["metadata"].get("name", ""),
                "content": b["content"],
                "type": b["metadata"].get("type", ""),
                "domain": b["metadata"].get("domain", []),
                "tags": b["metadata"].get("tags", []),
                "chord": b["metadata"].get("chord", ""),
                "signal": b["metadata"].get("signal", ""),
                "signal_hints": b["metadata"].get("signal_hints", {}),
                "drive_tags": b["metadata"].get("drive_tags", {}),
                "importance": b["metadata"].get("importance", 5),
                "created": b["metadata"].get("created", ""),
            })
        return JSONResponse({"buckets": results, "total": len(all_buckets)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/import/review", methods=["POST"])
async def api_import_review(request):
    """Apply review decisions: mark buckets as important/noise/pinned."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    decisions = body.get("decisions", [])
    if not decisions:
        return JSONResponse({"error": "No decisions provided"}, status_code=400)

    applied = 0
    errors = 0
    for d in decisions:
        bid = d.get("bucket_id", "")
        action = d.get("action", "")
        if not bid or not action:
            continue
        try:
            if action == "important":
                await bucket_mgr.update(bid, importance=9)
            elif action == "pin":
                await bucket_mgr.update(bid, pinned=True)
            elif action == "noise":
                await bucket_mgr.update(bid, resolved=True, importance=1)
            elif action == "delete":
                file_path = bucket_mgr._find_bucket_file(bid)
                if file_path:
                    os.remove(file_path)
            applied += 1
        except Exception as e:
            logger.warning(f"Review action failed for {bid}: {e}")
            errors += 1

    return JSONResponse({"applied": applied, "errors": errors})


# =============================================================
# /api/speech-event — 每轮话的短时残影
# Hook 热路径只提交本地初判；DP 复判在后台异步跑，结果下一轮生效。
# 复核语义沿用 认 / 不认 / 悬置，不让 rubric 写死成某个具体 persona。
# =============================================================
@mcp.custom_route("/api/speech-event/submit", methods=["POST"])
async def api_speech_event_submit(request):
    from starlette.responses import JSONResponse
    return JSONResponse(
        {"ok": True, "retired": True, "reason": "dialogue_residue replaces speech_event"},
        headers={"Access-Control-Allow-Origin": "*"},
    )


@mcp.custom_route("/api/speech-event/state", methods=["GET"])
async def api_speech_event_state(request):
    from starlette.responses import JSONResponse
    event = load_speech_event_state(config["buckets_dir"])
    if event:
        event = dict(event)
        event["recent"] = is_recent_speech_event(event)
    return JSONResponse(event or {}, headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/speech-event/review", methods=["POST"])
async def api_speech_event_review(request):
    from starlette.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400,
                           headers={"Access-Control-Allow-Origin": "*"})
    try:
        result = apply_speech_event_review(
            config["buckets_dir"],
            body.get("event_id", ""),
            body.get("mark", ""),
            body.get("note", ""),
        )
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400,
                           headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500,
                           headers={"Access-Control-Allow-Origin": "*"})
    return JSONResponse(result, headers={"Access-Control-Allow-Origin": "*"})


# =============================================================
# /api/dialogue-residue — 2+2 当前对话残留
# companion 在 Stop 后拼出最近 2 条Human + 2 条 Agent。若该窗口已经调用过
# Nocturne 工具，直接跳过，避免和 CLI/nocturne 自存事件重复喂入。
# =============================================================
@mcp.custom_route("/api/dialogue-residue/submit", methods=["POST"])
async def api_dialogue_residue_submit(request):
    from starlette.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400,
                           headers={"Access-Control-Allow-Origin": "*"})

    window_id = str(body.get("window_id") or "").strip()[:120]
    messages = normalize_dialogue_messages(body.get("messages") or [])
    nocturne_called = bool(body.get("nocturne_called"))
    if not window_id:
        window_id = str(body.get("id") or "").strip()[:120]
    if nocturne_called:
        skipped = normalize_dialogue_residue_event(
            {"status": "skipped_nocturne_call", "confidence": 0.0, "intensity": 0.0},
            messages=messages,
            window_id=window_id,
        )
        save_dialogue_residue_state(config["buckets_dir"], skipped, ledger_stage="skipped_nocturne_call")
        return JSONResponse({"ok": True, "skipped": True, "reason": "nocturne_called", "window_id": skipped["window_id"]},
                           headers={"Access-Control-Allow-Origin": "*"})
    if len(messages) < 4:
        return JSONResponse({"ok": False, "error": "need 2 user + 2 assistant messages", "count": len(messages)},
                           status_code=400, headers={"Access-Control-Allow-Origin": "*"})

    dp_available = dialogue_residue_available()
    if dp_available:
        asyncio.create_task(_refine_dialogue_residue_background(messages, window_id))
    else:
        fallback = normalize_dialogue_residue_event(
            {"status": "dp_unavailable", "confidence": 0.0, "intensity": 0.0},
            messages=messages,
            window_id=window_id,
        )
        save_dialogue_residue_state(config["buckets_dir"], fallback, ledger_stage="dp_unavailable")

    return JSONResponse(
        {
            "ok": True,
            "dp_queued": dp_available,
            "window_id": window_id,
            "message_count": len(messages),
        },
        headers={"Access-Control-Allow-Origin": "*"},
    )


@mcp.custom_route("/api/dialogue-residue/state", methods=["GET"])
async def api_dialogue_residue_state(request):
    from starlette.responses import JSONResponse
    return JSONResponse(load_dialogue_residue_state(config["buckets_dir"]) or {},
                       headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/desire/state", methods=["GET"])
async def api_desire_state(request):
    """只读：当前drive/intent/pa_na等快照，不tick。
    tick的节奏完全交给_desire_heartbeat_loop(DESIRE_TICK_SECONDS)——
    否则dashboard刷新/打开页面会偷偷多走一拍，tick_count/escape_streak/grief
    的计数节奏会被"谁在看"污染。"""
    from starlette.responses import JSONResponse
    state = _desire.state()
    try:
        import json as _j, os as _o
        mood_path = _bucket_path("current_mood.json")
        live = {}
        if _o.path.exists(mood_path):
            with open(mood_path) as _f:
                live = _j.load(_f)
        thoughts = sorted(
            state.get("thoughts", []),
            key=lambda t: float(t.get("born_at", 0) or 0),
            reverse=True,
        )
        state["thoughts"] = thoughts
        # get_daily_mood缓存不命中时会同步调DeepSeek(最长10s)，helper会扔进线程池跑，
        # 不然这一个请求会卡住整个事件循环，拖累同时打过来的所有其他请求。
        mood_entry = await _weather_mood_entry()
        # The dashboard needs the full WeatherResidue readout. effective_pa_na has
        # only final PA/NA, so using it here hides base/residue as 0.00.
        weather = _desire.weather_state()
        warmth = float(weather.get("effective_PA", state.get("pa_na", {}).get("PA", 0.5)))
        shadow = float(weather.get("effective_NA", state.get("pa_na", {}).get("NA", 0.2)))
        latest_thought = _latest_thought_text(state)
        mood_trace, mood_trace_born_at = _fresh_mood_trace(state)
        top_drive, _, undertow_raw_value = _undertow_snapshot(state)
        _activations = (
            state.get("effective_activations")
            or state.get("drive_activations")
            or {}
        )
        undertow_value = _num(
            _activations.get(top_drive) if isinstance(_activations, dict) else None,
            undertow_raw_value,
        )
        state["latest_thought"] = latest_thought
        state["mood_trace"] = mood_trace
        state["synthesized_mood_trace"] = mood_entry[0]
        state["weather_residue"] = {
            "warmth": round(float(weather.get("warmth_residue", 0.0)), 3),
            "shadow": round(float(weather.get("shadow_residue", 0.0)), 3),
            "component_shadow": round(float(weather.get("component_shadow_residue", 0.0)), 3),
            "crystal_shadow": round(float(weather.get("crystal_shadow", 0.0)), 3),
            "shadow_crystal": weather.get("shadow_crystal"),
            "base_warmth": round(float(weather.get("base_PA", 0.0)), 3),
            "base_shadow": round(float(weather.get("base_NA", 0.0)), 3),
            "updated_at": weather.get("updated_at"),
            "active_chord": weather.get("active_chord", ""),
            "active_chord_source": weather.get("active_chord_source", ""),
            "active_chord_weight": weather.get("active_chord_weight", 0.0),
            "source_stack": weather.get("source_stack", []),
            "chord_chemistry": weather.get("chord_chemistry", {}),
        }
        state["pulse_weather"] = {
            "undertow": top_drive,
            "undertow_value": round(undertow_value, 3),
            "undertow_raw_value": round(undertow_raw_value, 3),
            "warmth": round(warmth, 3),
            "shadow": round(shadow, 3),
            "current_chord": weather.get("current_chord", ""),
            "active_chord": weather.get("active_chord", ""),
            "active_chord_source": weather.get("active_chord_source", ""),
            "active_chord_weight": weather.get("active_chord_weight", 0.0),
            "source_stack": weather.get("source_stack", []),
            "chord_display": _weather_chord_display(weather),
            "chord_chemistry": weather.get("chord_chemistry", {}),
            "chemistry_core": weather.get("chemistry_core", {}),
            "chemistry_route": weather.get("chemistry_route", {}),
                "warmth_residue": round(float(weather.get("warmth_residue", 0.0)), 3),
            "shadow_residue": round(float(weather.get("shadow_residue", 0.0)), 3),
            "component_shadow_residue": round(float(weather.get("component_shadow_residue", 0.0)), 3),
            "crystal_shadow": round(float(weather.get("crystal_shadow", 0.0)), 3),
            "shadow_crystal": weather.get("shadow_crystal"),
            "base_warmth": round(float(weather.get("base_PA", 0.0)), 3),
            "base_shadow": round(float(weather.get("base_NA", 0.0)), 3),
            "longing": round(float(state.get("longing", 0) or 0), 3),
            "longing_phase": state.get("longing_phase") or "",
            "hours_awake_absent": round(float(state.get("hours_awake_absent", 0) or 0), 3),
            "hours_since_last_message": round(float(state.get("hours_since_last_message", 0) or 0), 3),
            "attachment_gain_scale": round(float(state.get("attachment_gain_scale", 1) or 1), 3),
            "mood_trace": mood_trace,
            "mood_trace_born_at": mood_trace_born_at,
            "synthesized_mood_trace": mood_entry[0],
        }
        state["now_playing"] = _current_now_playing()
        state["weather_panel"] = _weather_panel_from_state(state)
        dialogue_residue = load_dialogue_residue_state(config["buckets_dir"])
        if dialogue_residue:
            state["dialogue_residue"] = dialogue_residue
    except Exception:
        pass
    full_requested = str(request.query_params.get("full", "")).strip().lower() in {"1", "true", "yes"}
    full_allowed = os.environ.get("OMBRE_DESIRE_STATE_FULL", "").strip().lower() in {"1", "true", "yes", "on"}
    payload = state if full_requested and full_allowed else _compact_desire_state(state)
    return JSONResponse(payload,
                       headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/desire/intent", methods=["GET"])
async def api_desire_intent(request):
    """只读：当前intent + 关联念头text，不触发satisfy/refractory。供heartbeat_bridge轮询。"""
    from starlette.responses import JSONResponse
    intent = _desire.intent_with_thought()
    return JSONResponse({"intent": intent},
                       headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/desire/intent/hint", methods=["GET"])
async def api_desire_intent_hint(request):
    """Retired: Drive v2 uses live thoughts and latent notes, not preset intent pools."""
    from starlette.responses import JSONResponse
    return JSONResponse({"hint": None, "retired": True},
                       headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/heartbeat/latent-note", methods=["GET"])
async def api_heartbeat_latent_note(request):
    """Free Roam Subcurrent：只从前端同一份 approved latent_notes 按 drive_tag 抽取。

    不再静默掉进 marks/old_memory 桶捞兜底——空就空，错口音比沉默更糟。
    """
    from starlette.responses import JSONResponse
    raw_exclude = request.query_params.get("exclude", "")
    exclude_ids = {x.strip() for x in raw_exclude.split(",") if x.strip()}
    drive_key = request.query_params.get("drive_key", "")
    approved_note = _select_approved_latent_note(exclude_ids, drive_key=drive_key)
    if not approved_note:
        return JSONResponse(
            {
                "note": None,
                "source": "approved_pool",
                "candidate_count": 0,
                "pool_match": "empty",
                "drive_key": normalize_drive_key(drive_key) or "",
            },
            headers={"Access-Control-Allow-Origin": "*"},
        )
    try:
        approved_note["continuity_bias"] = _desire.apply_subcurrent_bias(
            approved_note.get("drive_tag") or drive_key,
            latent_weight=float(approved_note.get("score", 1.0) or 1.0),
            confidence=0.7,
        )
    except Exception as e:
        logger.warning(f"approved latent continuity bias failed: {e}")
    return JSONResponse(
        {
            "note": approved_note,
            "source": "approved_pool",
            "candidate_count": 1,
            "pool_match": approved_note.get("pool_match") or "exact",
            "drive_key": normalize_drive_key(drive_key) or approved_note.get("drive_tag") or "",
        },
        headers={"Access-Control-Allow-Origin": "*"},
    )


@mcp.custom_route("/api/heartbeat/latent-note/ack", methods=["POST"])
async def api_heartbeat_latent_note_ack(request):
    """Heartbeat bridge 投递成功后确认消耗 approved 便签。"""
    from starlette.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        body = {}
    note_id = body.get("note_id") or body.get("id") or body.get("bucket_id") or ""
    try:
        note = _ack_approved_latent_note(note_id)
        return JSONResponse(
            {"ok": True, "note": note},
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except KeyError:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404,
                            headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500,
                            headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/trails", methods=["GET"])
async def api_trails(request):
    """折痕路径 JSON：query 必填。心跳不附带；前端/好奇时再拉。"""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err
    query = (request.query_params.get("query") or "").strip()
    try:
        limit = int(request.query_params.get("limit") or TRAIL_DEFAULT_LIMIT)
    except ValueError:
        limit = TRAIL_DEFAULT_LIMIT
    if not query:
        return JSONResponse(
            {"ok": False, "error": "query required", "trail": None},
            status_code=400,
            headers={"Access-Control-Allow-Origin": "*"},
        )
    try:
        trail = await _build_trail(query, limit=limit)
        return JSONResponse(
            {"ok": True, "trail": trail},
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as e:
        logger.error(f"api trails failed: {e}")
        return JSONResponse(
            {"ok": False, "error": str(e), "trail": None},
            status_code=500,
            headers={"Access-Control-Allow-Origin": "*"},
        )


@mcp.custom_route("/api/trails/curation", methods=["POST"])
async def api_trails_curation(request):
    """Non-destructive, exact-query display/hide overlay for one trail node."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err
    try:
        body = await request.json()
    except Exception:
        body = {}
    try:
        query, ref, action, display_anchor = _validate_trail_curation_payload(body)
    except ValueError as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=400,
            headers={"Access-Control-Allow-Origin": "*"},
        )
    try:
        result = _update_trail_curation(query, ref, action, display_anchor)
        return JSONResponse(
            {"ok": True, "curation": result},
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as e:
        logger.error(f"api trails curation failed: {e}")
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=500,
            headers={"Access-Control-Allow-Origin": "*"},
        )


@mcp.custom_route("/api/trails/delta", methods=["POST"])
async def api_trails_delta(request):
    """Claim or clear one manual, query-scoped delta without reading source buckets."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err
    try:
        body = await request.json()
    except Exception:
        body = {}
    try:
        query, ref, action, text, baseline_ref, basis_order_id = _validate_trail_delta_payload(body)
    except ValueError as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=400,
            headers={"Access-Control-Allow-Origin": "*"},
        )
    try:
        result = _update_trail_delta(
            query, ref, action, text, baseline_ref, basis_order_id
        )
        return JSONResponse(
            {"ok": True, "delta": result},
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as e:
        logger.error(f"api trails delta failed: {e}")
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=500,
            headers={"Access-Control-Allow-Origin": "*"},
        )


def _trail_family_error_response(error):
    from starlette.responses import JSONResponse
    if isinstance(error, _TrailFamilyError):
        return JSONResponse({"ok": False, "error": str(error)}, status_code=error.status)
    logger.error(f"trail families store failed: {error}")
    return JSONResponse({"ok": False, "error": "trail families store unavailable"}, status_code=500)


@mcp.custom_route("/api/trail-families", methods=["GET"])
async def api_trail_families_list(request):
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        data = _load_trail_families(strict=True)
        families = [{
            "id": family["id"],
            "title": family.get("title", ""),
            "core_question": family.get("core_question", ""),
            "revision": family["revision"],
            "query_entry_count": len(family["query_entries"]),
            "member_count": len(family["members"]),
            "created_at": family.get("created_at", ""),
            "updated_at": family.get("updated_at", ""),
        } for family in data["families"].values()]
        families.sort(key=lambda row: (row["created_at"], row["id"]))
        return JSONResponse({"ok": True, "families": families})
    except Exception as e:
        return _trail_family_error_response(e)


@mcp.custom_route("/api/trail-families/{family_id}", methods=["GET"])
async def api_trail_family_detail(request):
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    family_id = str(request.path_params.get("family_id") or "")
    try:
        family = _load_trail_families(strict=True)["families"].get(family_id)
        if not isinstance(family, dict):
            raise _TrailFamilyError(404, "family not found")
        return JSONResponse({"ok": True, "family": family})
    except Exception as e:
        return _trail_family_error_response(e)


async def _trail_family_mutation_request(request, mutator):
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        body = None
    try:
        result = mutator(body)
        return JSONResponse({"ok": True, "result": result})
    except Exception as e:
        return _trail_family_error_response(e)


@mcp.custom_route("/api/trail-families", methods=["POST"])
async def api_trail_families_mutate(request):
    return await _trail_family_mutation_request(request, _mutate_trail_family)


@mcp.custom_route("/api/trail-families/{family_id}/entries", methods=["POST"])
async def api_trail_family_entries_mutate(request):
    family_id = str(request.path_params.get("family_id") or "")
    return await _trail_family_mutation_request(
        request, lambda body: _mutate_trail_family_entry(family_id, body)
    )


@mcp.custom_route("/api/trail-families/{family_id}/members", methods=["POST"])
async def api_trail_family_members_mutate(request):
    family_id = str(request.path_params.get("family_id") or "")
    return await _trail_family_mutation_request(
        request, lambda body: _mutate_trail_family_member(family_id, body)
    )


@mcp.custom_route("/api/latent-notes", methods=["GET"])
async def api_latent_notes(request):
    """查看潜意识便签池。当前只用于草稿测试，不自动进入 heartbeat。"""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    status = (request.query_params.get("status") or "").strip()
    try:
        limit = int(request.query_params.get("limit") or 80)
    except ValueError:
        limit = 80
    limit = max(1, min(limit, 500))
    data = _load_latent_notes()
    if _prune_expired_latent_notes(data):
        _save_latent_notes(data)
    notes = latent_notes_for_display(data, status=status, limit=limit)
    return JSONResponse(
        {"version": data.get("version", LATENT_NOTE_POOL_VERSION), "count": len(notes), "notes": notes},
        headers={"Access-Control-Allow-Origin": "*"},
    )


@mcp.custom_route("/api/sanctum/reverie", methods=["GET"])
async def api_sanctum_reverie(request):
    """公开：Reverie 页数据 — Dream Veil + Latent Notes（只读）。"""
    from starlette.responses import JSONResponse
    import json as _json
    import os as _os

    status = (request.query_params.get("status") or "").strip()
    try:
        limit = int(request.query_params.get("limit") or 80)
    except ValueError:
        limit = 80
    limit = max(1, min(limit, 200))

    # Dream
    dream_text = ""
    dream_ts = 0
    try:
        dream_path = _bucket_path("latest_dream.json")
        if _os.path.exists(dream_path):
            with open(dream_path, encoding="utf-8") as f:
                data = _json.load(f)
            dream_text = str(data.get("dream") or "")
            dream_ts = data.get("ts") or 0
    except Exception:
        pass

    # Latent notes
    notes: list = []
    counts = {"draft": 0, "approved": 0, "used": 0}
    try:
        pool = _load_latent_notes()
        if _prune_expired_latent_notes(pool):
            _save_latent_notes(pool)
        all_notes = list(pool.get("notes") or [])
        for n in all_notes:
            if not isinstance(n, dict):
                continue
            st = str(n.get("status") or "draft")
            if st in counts:
                counts[st] += 1
        notes = latent_notes_for_display(pool, status=status, limit=limit)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=500,
            headers={"Access-Control-Allow-Origin": "*"},
        )

    return JSONResponse(
        {
            "ok": True,
            "dream": {
                "text": dream_text,
                "ts": dream_ts,
            },
            "latent_notes": {
                "count": len(notes),
                "counts": counts,
                "notes": notes,
            },
        },
        headers={"Access-Control-Allow-Origin": "*"},
    )


@mcp.custom_route("/api/latent-notes", methods=["POST"])
async def api_latent_notes_create(request):
    """手动添加一条潜意识便签草稿。"""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400,
                            headers={"Access-Control-Allow-Origin": "*"})
    dream_line = " ".join(str(body.get("dream_line") or body.get("line") or "").split())
    if not dream_line:
        return JSONResponse({"ok": False, "error": "dream_line required"}, status_code=400,
                            headers={"Access-Control-Allow-Origin": "*"})
    ts = _latent_note_ts()
    note_type = _normalize_latent_note_type(body.get("note_type"))
    note = {
        "id": "latent_manual_" + secrets.token_hex(8),
        "status": _normalize_latent_note_status(body.get("status"), "draft"),
        "pinned": bool(body.get("pinned", False)),
        "note_type": note_type,
        "drive_tag": _normalize_latent_drive_tag(body.get("drive_tag"), note_type),
        "source_bucket_id": "",
        "source_kind": _normalize_latent_source_kind(body.get("source_kind")),
        "source_title": str(body.get("source_title") or "手动便签").strip()[:80],
        "source_created": "",
        "source_fragment": str(body.get("source_fragment") or dream_line).strip(),
        "dream_line": dream_line,
        "model": "manual",
        "created_at": ts,
        "updated_at": ts,
    }
    data = _load_latent_notes()
    data["notes"] = [note] + data.get("notes", [])
    _touch_latent_note_data(data)
    _save_latent_notes(data)
    return JSONResponse({"ok": True, "note": note}, headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/latent-notes/{note_id}/update", methods=["POST"])
async def api_latent_notes_update(request):
    """编辑便签正文、类型或状态。"""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    note_id = request.path_params["note_id"]
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400,
                            headers={"Access-Control-Allow-Origin": "*"})
    data = _load_latent_notes()
    note = _find_latent_note(data, note_id)
    if not note:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404,
                            headers={"Access-Control-Allow-Origin": "*"})
    if "dream_line" in body or "line" in body:
        dream_line = " ".join(str(body.get("dream_line") or body.get("line") or "").split())
        if not dream_line:
            return JSONResponse({"ok": False, "error": "dream_line required"}, status_code=400,
                                headers={"Access-Control-Allow-Origin": "*"})
        note["dream_line"] = dream_line
    if "note_type" in body:
        note["note_type"] = _normalize_latent_note_type(body.get("note_type"))
        if "drive_tag" not in body:
            note["drive_tag"] = _normalize_latent_drive_tag(note.get("drive_tag"), note.get("note_type"))
    if "drive_tag" in body:
        note["drive_tag"] = _normalize_latent_drive_tag(body.get("drive_tag"), note.get("note_type"))
    if "status" in body:
        note["status"] = _normalize_latent_note_status(body.get("status"), note.get("status") or "draft")
    if "pinned" in body:
        note["pinned"] = bool(body.get("pinned"))
    note["updated_at"] = _latent_note_ts()
    _touch_latent_note_data(data)
    _save_latent_notes(data)
    return JSONResponse({"ok": True, "note": note}, headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/latent-notes/{note_id}/delete", methods=["POST"])
async def api_latent_notes_delete(request):
    """软删除一条潜意识便签。"""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    note_id = request.path_params["note_id"]
    data = _load_latent_notes()
    note = _find_latent_note(data, note_id)
    if not note:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404,
                            headers={"Access-Control-Allow-Origin": "*"})
    ts = _latent_note_ts()
    note["status"] = "deleted"
    note["pinned"] = False
    note["deleted_at"] = ts
    note["updated_at"] = ts
    _touch_latent_note_data(data)
    _save_latent_notes(data)
    return JSONResponse({"ok": True, "note": note}, headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/latent-notes/generate", methods=["POST"])
async def api_latent_notes_generate(request):
    """批量生成潜意识便签草稿。DP 慢路径，只进 draft 池，不直接喂 heartbeat。"""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        body = {}
    try:
        count = int(body.get("count") or request.query_params.get("count") or 10)
    except (TypeError, ValueError):
        count = 10
    count = max(1, min(count, 50))
    prefer_low = body.get("prefer_low_stock", True)
    if isinstance(prefer_low, str):
        prefer_low = prefer_low.strip().lower() not in {"0", "false", "no", "off"}
    try:
        result = await _generate_latent_note_drafts(count=count, prefer_low_stock=bool(prefer_low))
        generated = result.get("generated", [])
        data = _load_latent_notes()
        existing_ids = {n.get("id") for n in data.get("notes", [])}
        fresh = [n for n in generated if n.get("id") not in existing_ids]
        if fresh:
            data["notes"] = fresh + data.get("notes", [])
            data["version"] = LATENT_NOTE_POOL_VERSION
            data["updated_at"] = now_iso()
            _save_latent_notes(data)
        return JSONResponse(
            {
                "ok": True,
                "requested": count,
                "generated_count": len(generated),
                "saved_count": len(fresh),
                "source_count": result.get("source_count", 0),
                "inward_source_count": result.get("inward_source_count", 0),
                "outward_source_count": result.get("outward_source_count", 0),
                "inward_target": result.get("inward_target", 0),
                "outward_target": result.get("outward_target", 0),
                "model": result.get("model", ""),
                "drive_quota": result.get("drive_quota") or {},
                "stock_before": result.get("stock_before") or {},
                "low_stock": result.get("low_stock") or [],
                "notes": fresh,
            },
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as e:
        logger.warning(f"latent note generation failed: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500,
                            headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/desire/intent/ack", methods=["POST"])
async def api_desire_intent_ack(request):
    """本地投递成功后调用：挂号 pending + 短 refractory + 轻 bleed。
    不要求模型 settle/pass/break；真正的记忆写入会 engagement settle。
    POST JSON: {"drive_key": "attachment", "source": "heartbeat"}"""
    from starlette.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        body = {}
    drive_key = body.get("drive_key", "")
    if not drive_key:
        return JSONResponse({"error": "drive_key required"}, status_code=400)
    source = str(body.get("source") or "heartbeat")
    try:
        result = _desire.note_pulse_delivery(drive_key, source=source)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    return JSONResponse({"acked": drive_key, "result": result},
                       headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/desire/intent/pass", methods=["POST"])
async def api_desire_intent_pass(request):
    """轻轻放过当前intent：不改Drive，只降短期hook/intent优先级。
    POST JSON: {"drive_key": "...", "thought": "..."}
    thought 有字自动进念头池；旧字段 reason 仅作兼容别名。"""
    from starlette.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        body = {}
    drive_key = body.get("drive_key", "")
    if not drive_key:
        return JSONResponse({"error": "drive_key required"}, status_code=400)
    thought = (body.get("thought") or body.get("reason") or "").strip()
    try:
        result = pass_tool(drive_key, thought=thought)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    return JSONResponse({"passed": drive_key, "result": result},
                       headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/desire/ping", methods=["POST"])
async def api_desire_ping(request):
    """Human发消息时调用，重置longing计时器；可携带本地关键词天气轻推。"""
    from starlette.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        body = {}
    try:
        result = _desire.mark_user_signal()
        weather = None
        def _safe_delta(key: str) -> float:
            try:
                return max(0.0, float(body.get(key, 0.0) or 0.0))
            except (TypeError, ValueError):
                return 0.0
        warmth_delta = _safe_delta("warmth_delta")
        shadow_delta = _safe_delta("shadow_delta")
        soothe = bool(body.get("soothe", False))
        if warmth_delta > 0 or shadow_delta > 0 or soothe:
            weather = _desire.apply_weather_delta(
                warmth_delta=warmth_delta,
                shadow_delta=shadow_delta,
                source=body.get("source", "keyword"),
                soothe=soothe,
            )
        return JSONResponse({"ok": True, **result, "weather_residue": weather},
                           headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500,
                           headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/desire/thought/{tid}/update", methods=["POST"])
async def api_desire_thought_update(request):
    """Dashboard edit: update one thought's text/drive/strength."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    tid = request.path_params.get("tid", "")
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    text = body.get("text") if "text" in body else None
    drive = body.get("drive") if "drive" in body else None
    strength = body.get("strength") if "strength" in body else None
    if text is not None and not isinstance(text, str):
        return JSONResponse({"error": "text must be a string"}, status_code=400)
    if drive is not None:
        drive = normalize_drive_key(drive)
        if drive not in DRIVE_KEYS:
            return JSONResponse({"error": "invalid drive"}, status_code=400)
    if strength is not None:
        try:
            strength = max(0.0, min(1.0, float(strength)))
        except (TypeError, ValueError):
            return JSONResponse({"error": "strength must be a number"}, status_code=400)

    try:
        result = _desire.update_thought(tid, text=text, drive=drive, strength=strength)
        if not result.get("ok"):
            return JSONResponse({"error": "thought not found or unchanged"}, status_code=404)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/desire/thought/{tid}/delete", methods=["POST"])
async def api_desire_thought_delete(request):
    """Dashboard edit: remove one thought from Thought Pool."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    tid = request.path_params.get("tid", "")
    try:
        result = _desire.delete_thought(tid)
        if not result.get("ok"):
            return JSONResponse({"error": "thought not found"}, status_code=404)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/desire/feed", methods=["POST"])
async def api_desire_feed(request):
    """
    接收drive_event_v2或旧analyze_feel结果，写进念头池/Drive Event账本。
    v2: primary_drive + intensity + confidence + agency + brain + thoughts。
    legacy: drives/brain_signals会被折成一次drive_event_v2，不再三路重复pulse。
    """
    from starlette.responses import JSONResponse
    import json as _json
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    requested_source = str(body.get("source") or "").strip()
    requested_memory_mode = (
        "dp" if requested_source == "dp_memory"
        else "cli" if requested_source == "analyze_nocturne_entry"
        else ""
    )
    if requested_memory_mode and requested_memory_mode != MEMORY_ANALYZER_MODE:
        return JSONResponse({
            "error": "inactive memory analyzer",
            "active": MEMORY_ANALYZER_MODE,
            "rejected_source": requested_source,
        }, status_code=409)

    thoughts = body.get("thoughts", [])

    # --- 批量涌入节流 ---
    # trigger.js依赖claude.ai session活跃才跑，可能积压几天的feel一次性涌进来。
    # 一批塞太多flit会在接下来几次tick里集中冲过FLIT_UPGRADE_THRESHOLD同时升级成
    # fixation，造成drive脉冲尖峰——这是工程节奏问题，跟"久没见想得更浓"的基线
    # 漂移是两件事，不该混在一起。
    # 超过阈值时：strength最高的几条按原值入池，其余按名次递减打折——
    # 打折幅度不同→之后每次tick_thoughts衰减后到达升级阈值的时机自然错开，
    # 而不是同一拍集中升级。
    FEED_BATCH_THRESHOLD = 4   # 超过这个数量才节流
    FEED_KEEP_FULL = 3         # 保留几条按原strength入池
    FEED_DISCOUNT_STEP = 0.08  # 其余每条额外递减的折扣
    FEED_STRENGTH_FLOOR = 0.22 # 打折下限——flit 12h半衰期下，低于这个不到24h就fade没了

    def _add_feed_thoughts(items: list, source: str = "cli") -> int:
        if not isinstance(items, list):
            return 0
        if len(items) > FEED_BATCH_THRESHOLD:
            items = sorted(items, key=lambda t: float(t.get("strength", 0.45)), reverse=True)
        added_count = 0
        for i, t in enumerate(items):
            if not isinstance(t, dict):
                continue
            text = str(t.get("text", "")).strip()
            drive = normalize_drive_key(t.get("drive"), "unsourced")
            try:
                strength = float(t.get("strength", 0.45))
            except (TypeError, ValueError):
                strength = 0.45
            if len(items) > FEED_BATCH_THRESHOLD and i >= FEED_KEEP_FULL:
                rank = i - FEED_KEEP_FULL + 1
                strength = max(FEED_STRENGTH_FLOOR, strength * (1 - FEED_DISCOUNT_STEP * rank))
            if text:
                try:
                    thought_source = str(t.get("source") or source or "cli").strip()[:80]
                    source_bucket = str(t.get("source_bucket") or body.get("source_bucket") or "").strip()[:120]
                    source_type = str(t.get("source_type") or body.get("source_type") or "").strip()[:40]
                    source_created = str(t.get("source_created") or body.get("source_created") or "").strip()[:80]
                    _desire.add_thought(
                        text, drive, strength=strength, source=thought_source,
                        source_bucket=source_bucket, source_type=source_type,
                        source_created=source_created,
                    )
                    _desire.store.add_echo(text, drive)
                    if (t.get("chord") or "").strip():
                        _desire.apply_chord_echo(t.get("chord", "").strip(), source="thought")
                    added_count += 1
                except Exception as e:
                    logger.warning(f"desire/feed add_thought failed: {e}")
        return added_count

    schema_version = str(body.get("schema_version") or "")
    is_v2 = schema_version == DRIVE_EVENT_SCHEMA or bool(body.get("primary_drive"))
    event_result = None
    event_body = None
    if is_v2:
        event_body = body
    else:
        drives = body.get("drives", {})
        brain_signals = body.get("brain_signals", {})
        if isinstance(drives, dict) and drives or isinstance(brain_signals, dict) and brain_signals:
            event_body = _legacy_brain_to_event(brain_signals, drives)

    if event_body and event_body.get("primary_drive"):
        try:
            event_result = _desire.apply_drive_event(event_body)
        except Exception as e:
            logger.warning(f"desire/feed drive_event failed: {e}")
            event_result = {"ok": False, "error": str(e)}

    feed_source = str(body.get("source") or (event_body or {}).get("source") or "cli").strip()[:80] or "cli"
    # dp_memory 不入 Thought Pool（与 dialogue_residue 一致）；只走 Drive/Weather。
    add_thoughts = (
        feed_source not in {"dp_memory"}
        and (
            not (event_result and event_result.get("suppressed"))
            or feed_source == "analyze_nocturne_entry"
        )
    )
    added = _add_feed_thoughts(thoughts, source=feed_source) if add_thoughts else 0
    if event_result and event_result.get("suppressed"):
        logger.info(f"desire/feed suppressed event: {event_result.get('reason')}")

    try:
        import json as _bj, os as _bo
        mood_path = _bucket_path("current_mood.json")
        mood_data = {}
        if _bo.path.exists(mood_path):
            with open(mood_path) as _f:
                mood_data = _bj.load(_f)
        if event_body:
            mood_data["drive_event"] = {
                "schema_version": DRIVE_EVENT_SCHEMA,
                "primary_drive": normalize_drive_key(event_body.get("primary_drive"), ""),
                "event_label": event_body.get("event_label", ""),
                "brain": event_body.get("brain", {}),
                "evidence": event_body.get("evidence", []),
                "result": event_result or {},
            }
        if body.get("brain_signals"):
            mood_data["legacy_brain_signals"] = body.get("brain_signals")
        with open(mood_path, "w") as _f:
            _bj.dump(mood_data, _f)
    except Exception as e:
        logger.warning(f"desire/feed mood write failed: {e}")

    source = ""
    if isinstance(event_body, dict):
        brain = event_body.get("brain") if isinstance(event_body.get("brain"), dict) else {}
        source = str(event_body.get("source") or brain.get("source") or "")
    if body.get("mark_user_signal") or source in {"user_message", "speech_event"} or body.get("brain_signals"):
        try:
            _last_signal_ts[0] = time.time()
            _desire.mark_user_signal(_last_signal_ts[0])
        except Exception as e:
            logger.warning(f"desire/feed mark_user_signal failed: {e}")

    logger.info(f"desire/feed: +{added} thoughts, event={event_result}")

    return JSONResponse({
        "ok": True,
        "thoughts_added": added,
        "event": event_result,
    }, headers={"Access-Control-Allow-Origin": "*"})


# =============================================================
# /api/soma — Soma Trace上报/读取
# Soma Trace 可由外部 companion hook 计算并提交。
# big_cat_state.json这些本地文件)，后端本来不知道这东西存在。
# 本地hook每次算完，主动POST一份上来；dashboard用GET读最新的。
# 1小时没人上报就当过期，不强行维持一个早就不新鲜的状态。
# =============================================================
_SOMA_STATE_PATH = _bucket_path("soma_state.json")
_SOMA_STALE_SECONDS = 3600


@mcp.custom_route("/api/soma/report", methods=["POST"])
async def api_soma_report(request):
    from starlette.responses import JSONResponse
    import json
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    source = (body.get("source") or "").strip()[:40]
    if body.get("clear"):
        try:
            os.makedirs(os.path.dirname(_SOMA_STATE_PATH), exist_ok=True)
            with open(_SOMA_STATE_PATH, "w") as f:
                json.dump({"line": None, "chord": None, "source": source or "clear", "updated_at": time.time()}, f)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)
        return JSONResponse({"ok": True, "cleared": True}, headers={"Access-Control-Allow-Origin": "*"})

    line = (body.get("line") or "").strip()
    chord = (body.get("chord") or "").strip()
    if not line:
        return JSONResponse({"error": "line required"}, status_code=400)
    try:
        os.makedirs(os.path.dirname(_SOMA_STATE_PATH), exist_ok=True)
        with open(_SOMA_STATE_PATH, "w") as f:
            json.dump({"line": line, "chord": chord, "source": source, "updated_at": time.time()}, f)
        if chord:
            _desire.apply_chord_echo(chord, source="soma")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    return JSONResponse({"ok": True}, headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/soma/state", methods=["GET"])
async def api_soma_state(request):
    from starlette.responses import JSONResponse
    import json
    try:
        with open(_SOMA_STATE_PATH) as f:
            data = json.load(f)
        if time.time() - data.get("updated_at", 0) > _SOMA_STALE_SECONDS:
            return JSONResponse({"line": None, "chord": None, "source": None},
                               headers={"Access-Control-Allow-Origin": "*"})
        return JSONResponse(data, headers={"Access-Control-Allow-Origin": "*"})
    except Exception:
        return JSONResponse({"line": None, "chord": None, "source": None},
                           headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/analyzer/mode", methods=["GET"])
async def api_analyzer_mode(request):
    from starlette.responses import JSONResponse
    return JSONResponse({
        "mode": MEMORY_ANALYZER_MODE,
        "source": "dp_memory" if MEMORY_ANALYZER_MODE == "dp" else "analyze_nocturne_entry",
    }, headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/analyzer/entries", methods=["GET"])
async def api_analyzer_entries(request):
    """
    Analyzer-only read view for new Nocturne entries.
    Defaults to 2026-06-25 00:00 Asia/Shanghai (2026-06-24T16:00:00Z).
    """
    from starlette.responses import JSONResponse
    try:
        since = _parse_analyzer_since(request.query_params.get("since"))
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400,
                           headers={"Access-Control-Allow-Origin": "*"})

    try:
        all_buckets = await bucket_mgr.list_all(include_archive=True)
        marks_by_bucket = _load_all_marks()
        entries = []
        for b in all_buckets:
            bid = str(b.get("id") or "").strip()
            if not bid:
                continue
            mark_rows = marks_by_bucket.get(bid, [])
            created_dt = _bucket_created_utc(b)
            if not created_dt or created_dt < since:
                continue
            entry_type = _analyzer_entry_type(b, mark_rows)
            if not entry_type:
                continue
            meta = b.get("metadata", {})
            entries.append({
                "id": bid,
                "type": entry_type,
                "created": meta.get("created", ""),
                "content_preview": _analyzer_preview(b.get("content", "")),
                "chord": meta.get("chord", ""),
                "tags": meta.get("tags", []),
                "domain": meta.get("domain", []),
                "drive_tags": meta.get("drive_tags", {}),
                "signal_hints": meta.get("signal_hints", {}),
                "source": "dp_memory" if MEMORY_ANALYZER_MODE == "dp" else "analyze_nocturne_entry",
                "source_bucket": bid,
                "source_type": entry_type,
                "source_created": meta.get("created", ""),
                "_created_sort": created_dt.isoformat(),
            })
        entries.sort(key=lambda x: x["_created_sort"], reverse=True)
        for entry in entries:
            entry.pop("_created_sort", None)
        return JSONResponse(entries, headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500,
                           headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/analyzer/dp-memory", methods=["POST"])
async def api_analyzer_dp_memory(request):
    """
    DP memory analyzer line. Keeps the old CLI analyzer dormant and accepts
    the same entry shape from /api/analyzer/entries plus the old CLI preference.
    """
    from starlette.responses import JSONResponse
    if MEMORY_ANALYZER_MODE != "dp":
        return JSONResponse({"error": "dp memory analyzer disabled", "active": MEMORY_ANALYZER_MODE},
                           status_code=409, headers={"Access-Control-Allow-Origin": "*"})
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400,
                           headers={"Access-Control-Allow-Origin": "*"})

    entry = normalize_memory_entry(body.get("entry") if isinstance(body.get("entry"), dict) else body)
    preference = str(body.get("preference") or "").strip()
    post_feed = bool(body.get("post_feed", False))
    if not entry.get("content_preview"):
        return JSONResponse({"error": "entry.content_preview required"}, status_code=400,
                           headers={"Access-Control-Allow-Origin": "*"})
    if not memory_residue_available():
        return JSONResponse({"error": "dp_memory unavailable"}, status_code=503,
                           headers={"Access-Control-Allow-Origin": "*"})

    try:
        event = await classify_memory_residue_dp(
            entry,
            preference=preference,
            state_context=_dialogue_residue_context_snapshot(),
        )
        feed_result = None
        if post_feed:
            if event.get("primary_drive"):
                event_result = _desire.apply_drive_event(event)
            else:
                event_result = {"ok": False, "reason": "no_primary_drive"}
            # 只染 Drive/Weather/和弦；不再把 analyzer 代写念头塞进 Thought Pool。
            applied_chords = set()
            entry_chord = str(entry.get("chord") or "").strip()
            if entry_chord:
                _desire.apply_chord_echo(entry_chord, source="memory")
                applied_chords.add(entry_chord)
            feed_result = {
                "event": event_result,
                "thoughts_added": 0,
                "thoughts_disabled": "dp_memory_no_mint",
            }
        return JSONResponse({"ok": True, "event": event, "feed": feed_result},
                           headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        logger.warning(f"dp_memory analyzer failed: {e}")
        return JSONResponse({"error": str(e)}, status_code=500,
                           headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/feels", methods=["GET"])
async def api_feels_public(request):
    """公开接口：返回feel列表，供本地trigger按时间/checkpoint限流分析。无需auth。"""
    from starlette.responses import JSONResponse
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=True)
        feels = [
            {
                "id": b["id"],
                "content_preview": b["content"][:300],
                "created": b["metadata"].get("created", ""),
                "chord": b["metadata"].get("chord", ""),
                "drive_tags": b["metadata"].get("drive_tags", {}),
                "digested": bool(b["metadata"].get("digested", False)),
                "resolved": bool(b["metadata"].get("resolved", False)),
            }
            for b in all_buckets
            if b["metadata"].get("type") == "feel"
        ]
        feels.sort(key=lambda x: x["created"], reverse=True)
        return JSONResponse(feels, headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500,
                           headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/sanctum/traces", methods=["GET"])
async def api_sanctum_traces(request):
    """公开：Sanctum Memories / Trace 列表（与 dashboard Traces 同结构）。无需 auth。

    Query:
      filter — all|memory|feel|letter|writing|window|pinned|unresolved|digested|inner|archived
      q — 可选关键词（name / preview / tags / domain）
      limit — 默认 200，最大 1000
    """
    from starlette.responses import JSONResponse

    def _facets(meta: dict) -> set[str]:
        domains = meta.get("domain") or []
        if isinstance(domains, str):
            domains = [domains]
        tags = meta.get("tags") or []
        if isinstance(tags, str):
            tags = [tags]
        labels = {str(x).lower() for x in list(domains) + list(tags) if x}
        btype = str(meta.get("type") or "").lower()
        if btype:
            labels.add(btype)
        return labels

    def _kind(meta: dict) -> str:
        labels = _facets(meta)
        btype = str(meta.get("type") or "").lower()
        if btype == "feel" or "feel" in labels:
            return "feel"
        if "letter_human" in labels or "letter" in labels or btype in {"letter", "letter_human"}:
            return "letter"
        if "writing" in labels or btype == "writing":
            return "writing"
        if "window" in labels or btype == "window":
            return "window"
        if "inner" in labels or btype == "inner":
            return "inner"
        if btype == "archived":
            return "archived"
        return "memory"

    try:
        qs = request.query_params
        filt = (qs.get("filter") or "all").strip().lower()
        q = (qs.get("q") or "").strip().lower()
        try:
            limit = max(1, min(int(qs.get("limit") or 200), 1000))
        except Exception:
            limit = 200

        all_buckets = await bucket_mgr.list_all(include_archive=True)
        rows: list[dict] = []
        for b in all_buckets:
            if not isinstance(b, dict):
                continue
            meta = b.get("metadata") if isinstance(b.get("metadata"), dict) else {}
            kind = _kind(meta)
            preview = strip_wikilinks(b.get("content", "") or "")[:200]
            row = {
                "id": b.get("id"),
                "name": meta.get("name") or b.get("id") or "",
                "type": meta.get("type") or "dynamic",
                "kind": kind,
                "domain": meta.get("domain") or [],
                "tags": meta.get("tags") or [],
                "chord": meta.get("chord") or "",
                "pinned": bool(meta.get("pinned")),
                "resolved": bool(meta.get("resolved")),
                "digested": bool(meta.get("digested")),
                "unresolved": bool(meta.get("unresolved")) if "unresolved" in meta else False,
                "created": meta.get("created") or "",
                "last_active": meta.get("last_active") or meta.get("updated") or meta.get("created") or "",
                "content_preview": preview,
                "importance": meta.get("importance", 5),
            }
            # filter
            labels = _facets(meta)
            if filt == "all":
                if row["digested"] or row["resolved"]:
                    continue
            elif filt == "pinned":
                if not row["pinned"]:
                    continue
            elif filt == "feel":
                if kind != "feel":
                    continue
            elif filt == "letter":
                if kind != "letter":
                    continue
            elif filt == "writing":
                if kind != "writing":
                    continue
            elif filt == "window":
                if kind != "window":
                    continue
            elif filt == "inner":
                if kind != "inner" and "inner" not in labels:
                    continue
            elif filt == "memory":
                if kind != "memory":
                    continue
            elif filt == "digested":
                if not (row["digested"] or row["resolved"]):
                    continue
            elif filt == "archived":
                if kind != "archived" and str(meta.get("type") or "") != "archived":
                    continue
            elif filt == "unresolved":
                if not row["unresolved"] and "unresolved" not in labels:
                    continue
            if q:
                hay = " ".join(
                    [
                        str(row["name"]),
                        str(row["content_preview"]),
                        " ".join(str(x) for x in (row["tags"] or [])),
                        " ".join(str(x) for x in (row["domain"] or [])),
                        str(row["chord"]),
                    ]
                ).lower()
                if q not in hay:
                    continue
            rows.append(row)

        # timeline: pinned 置顶，其余按 created 倒序（与 dashboard Traces 一致）
        pinned = [r for r in rows if r.get("pinned")]
        rest = [r for r in rows if not r.get("pinned")]
        pinned.sort(key=lambda r: str(r.get("created") or ""), reverse=True)
        rest.sort(key=lambda r: str(r.get("created") or ""), reverse=True)
        rows = (pinned + rest)[:limit]
        return JSONResponse(
            {"ok": True, "records": rows, "count": len(rows)},
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=500,
            headers={"Access-Control-Allow-Origin": "*"},
        )


@mcp.custom_route("/api/sanctum/traces/{trace_id}", methods=["GET"])
async def api_sanctum_trace_detail(request):
    """公开：单条 Trace 全文（Sanctum Memories 二级详情）。"""
    from starlette.responses import JSONResponse
    try:
        trace_id = request.path_params.get("trace_id") or ""
        bucket = await bucket_mgr.get(trace_id)
        if not bucket:
            return JSONResponse(
                {"ok": False, "error": "not found"},
                status_code=404,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        meta = bucket.get("metadata") if isinstance(bucket.get("metadata"), dict) else {}
        return JSONResponse(
            {
                "ok": True,
                "id": bucket.get("id"),
                "metadata": meta,
                "content": strip_wikilinks(bucket.get("content", "") or ""),
            },
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=500,
            headers={"Access-Control-Allow-Origin": "*"},
        )


@mcp.custom_route("/api/sanctum/breath", methods=["GET"])
async def api_sanctum_breath(request):
    """公开：模拟 Breath 浮现条目（钉选 + 高权重未解决 + feel），默认 top 20。"""
    from starlette.responses import JSONResponse
    try:
        try:
            limit = max(1, min(int(request.query_params.get("limit") or 20), 40))
        except Exception:
            limit = 20
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        pinned = []
        scored = []
        feels = []
        for b in all_buckets:
            if not isinstance(b, dict):
                continue
            meta = b.get("metadata") if isinstance(b.get("metadata"), dict) else {}
            btype = str(meta.get("type") or "").lower()
            if btype in {"breath", "dream"}:
                continue
            preview = strip_wikilinks(b.get("content", "") or "")[:280]
            row = {
                "id": b.get("id"),
                "name": meta.get("name") or b.get("id") or "",
                "type": btype or "dynamic",
                "kind": "feel" if btype == "feel" else ("pinned" if (meta.get("pinned") or meta.get("protected")) else "memory"),
                "pinned": bool(meta.get("pinned") or meta.get("protected")),
                "created": meta.get("created") or "",
                "content_preview": preview,
                "score": float(decay_engine.calculate_score(meta) or 0),
                "section": "pinned" if (meta.get("pinned") or meta.get("protected")) else ("feel" if btype == "feel" else "memory"),
            }
            if row["pinned"]:
                pinned.append(row)
            elif btype == "feel":
                if not meta.get("resolved") and not meta.get("digested"):
                    feels.append(row)
            elif btype not in {"permanent", "archived"}:
                if not meta.get("resolved") and not meta.get("digested"):
                    # 排除 letter/writing 等 wander-only 若可检测
                    domains = meta.get("domain") or []
                    if isinstance(domains, str):
                        domains = [domains]
                    tags = meta.get("tags") or []
                    if isinstance(tags, str):
                        tags = [tags]
                    labels = {str(x).lower() for x in list(domains) + list(tags) if x}
                    if not labels.intersection({"letter", "letter_human", "writing", "window", "private"}):
                        scored.append(row)

        pinned.sort(key=lambda r: str(r.get("created") or ""), reverse=True)
        scored.sort(key=lambda r: float(r.get("score") or 0), reverse=True)
        feels.sort(key=lambda r: float(r.get("score") or 0), reverse=True)

        # 模拟 breath：钉选全收 + memory 前 N + feel 若干，合计不超过 limit
        mem_take = min(12, max(0, limit - len(pinned)))
        feel_take = min(8, max(0, limit - len(pinned) - mem_take))
        records = pinned[:limit] + scored[:mem_take] + feels[:feel_take]
        records = records[:limit]
        return JSONResponse(
            {
                "ok": True,
                "records": records,
                "count": len(records),
                "note": "simulated breath surface · top by weight",
            },
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=500,
            headers={"Access-Control-Allow-Origin": "*"},
        )


@mcp.custom_route("/api/sanctum/traces/{trace_id}/update", methods=["POST"])
async def api_sanctum_trace_update(request):
    """公开：Sanctum 详情操作（编辑 / 降权 / 沉底 / 消化）— 限字段。"""
    from starlette.responses import JSONResponse
    try:
        trace_id = request.path_params.get("trace_id") or ""
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(
                {"ok": False, "error": "Invalid JSON"},
                status_code=400,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        if not isinstance(body, dict):
            return JSONResponse(
                {"ok": False, "error": "body must be object"},
                status_code=400,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        bucket = await bucket_mgr.get(trace_id)
        if not bucket:
            return JSONResponse(
                {"ok": False, "error": "not found"},
                status_code=404,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        kwargs: dict = {}
        if "content" in body and isinstance(body["content"], str):
            kwargs["content"] = body["content"]
        if "name" in body and isinstance(body["name"], str):
            kwargs["name"] = body["name"].strip() or None
        if "resolved" in body:
            kwargs["resolved"] = bool(body["resolved"])
        if "digested" in body:
            kwargs["digested"] = bool(body["digested"])
        if "importance" in body:
            try:
                kwargs["importance"] = max(1, min(10, int(body["importance"])))
            except (TypeError, ValueError):
                return JSONResponse(
                    {"ok": False, "error": "importance 1-10"},
                    status_code=400,
                    headers={"Access-Control-Allow-Origin": "*"},
                )
        if "activation_count" in body:
            try:
                kwargs["activation_count"] = max(1, min(999, int(body["activation_count"])))
            except (TypeError, ValueError):
                return JSONResponse(
                    {"ok": False, "error": "activation_count invalid"},
                    status_code=400,
                    headers={"Access-Control-Allow-Origin": "*"},
                )
        if "arousal" in body:
            try:
                kwargs["arousal"] = max(0.0, min(1.0, float(body["arousal"])))
            except (TypeError, ValueError):
                return JSONResponse(
                    {"ok": False, "error": "arousal 0-1"},
                    status_code=400,
                    headers={"Access-Control-Allow-Origin": "*"},
                )
        if body.get("preserve_last_active"):
            kwargs["_preserve_last_active"] = True
        if not kwargs:
            return JSONResponse(
                {"ok": False, "error": "nothing to update"},
                status_code=400,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        # drop None name
        if kwargs.get("name") is None:
            kwargs.pop("name", None)
        updated = await bucket_mgr.update(trace_id, **kwargs)
        if not updated:
            return JSONResponse(
                {"ok": False, "error": "update failed"},
                status_code=500,
                headers={"Access-Control-Allow-Origin": "*"},
            )
        return JSONResponse(
            {"ok": True, "id": trace_id},
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=500,
            headers={"Access-Control-Allow-Origin": "*"},
        )


@mcp.custom_route("/api/sanctum/summary", methods=["GET"])
async def api_sanctum_summary(request):
    """公开：Sanctum 首页计数（总桶 / latent notes / letter / writing 最新日期）。无需 auth。"""
    from starlette.responses import JSONResponse

    def _bucket_kind(bucket: dict) -> str:
        meta = bucket.get("metadata") if isinstance(bucket.get("metadata"), dict) else {}
        domains = meta.get("domain") or []
        if isinstance(domains, str):
            domains = [domains]
        tags = meta.get("tags") or []
        if isinstance(tags, str):
            tags = [tags]
        labels = {str(x).lower() for x in list(domains) + list(tags) if x}
        btype = str(meta.get("type") or "").lower()
        if btype == "feel" or "feel" in labels:
            return "feel"
        if "letter_human" in labels or "letter" in labels or btype in {"letter", "letter_human"}:
            return "letter"
        if "writing" in labels or btype == "writing":
            return "writing"
        if "window" in labels or btype == "window":
            return "window"
        return "memory"

    def _created(bucket: dict) -> str:
        meta = bucket.get("metadata") if isinstance(bucket.get("metadata"), dict) else {}
        return str(meta.get("updated") or meta.get("created") or bucket.get("created") or "")

    try:
        all_buckets = await bucket_mgr.list_all(include_archive=True)
        tallies = {"memory": 0, "letter": 0, "writing": 0, "feel": 0, "window": 0, "other": 0}
        latest = {"letter": "", "writing": ""}
        for b in all_buckets:
            if not isinstance(b, dict):
                continue
            kind = _bucket_kind(b)
            if kind not in tallies:
                tallies["other"] += 1
            else:
                tallies[kind] += 1
            if kind in ("letter", "writing"):
                ts = _created(b)
                if ts and ts > str(latest.get(kind) or ""):
                    latest[kind] = ts

        # latent notes pool — 与 dashboard 对齐：draft + approved（不含 used / 过期）
        latent_count = 0
        try:
            data = _load_latent_notes()
            if _prune_expired_latent_notes(data):
                _save_latent_notes(data)
            notes = data.get("notes") or []
            latent_count = sum(
                1
                for n in notes
                if isinstance(n, dict) and str(n.get("status") or "") in {"draft", "approved"}
            )
        except Exception:
            latent_count = 0

        total_memories = (
            tallies["memory"] + tallies["letter"] + tallies["writing"]
            + tallies["feel"] + tallies["window"] + tallies["other"]
        )
        return JSONResponse(
            {
                "ok": True,
                "memories": total_memories,
                "latent_notes": latent_count,
                "feels": tallies["feel"],
                "letters": {"count": tallies["letter"], "latest": latest["letter"]},
                "writing": {"count": tallies["writing"], "latest": latest["writing"]},
                "breakdown": tallies,
            },
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=500,
            headers={"Access-Control-Allow-Origin": "*"},
        )


@mcp.custom_route("/api/sanctum/thought-to-latent", methods=["POST"])
async def api_sanctum_thought_to_latent(request):
    """公开：Thought Pool → Latent Notes draft（与 dashboard copyThoughtToLatent 同链路）。"""
    from starlette.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400,
                            headers={"Access-Control-Allow-Origin": "*"})
    text = " ".join(str(body.get("dream_line") or body.get("text") or body.get("line") or "").split())
    if not text:
        return JSONResponse({"ok": False, "error": "text required"}, status_code=400,
                            headers={"Access-Control-Allow-Origin": "*"})
    drive = str(body.get("drive") or body.get("drive_tag") or "").strip()
    note_type = "outward" if drive in {"social", "curiosity"} else "inward"
    if body.get("note_type"):
        note_type = _normalize_latent_note_type(body.get("note_type"))
    ts = _latent_note_ts()
    note = {
        "id": "latent_manual_" + secrets.token_hex(8),
        "status": "draft",
        "pinned": False,
        "note_type": note_type,
        "drive_tag": _normalize_latent_drive_tag(drive, note_type),
        "source_bucket_id": "",
        "source_kind": "thought_pool",
        "source_title": str(body.get("source_title") or "Thought Pool").strip()[:80],
        "source_created": "",
        "source_fragment": text,
        "dream_line": text,
        "model": "sanctum",
        "created_at": ts,
        "updated_at": ts,
        "source_tid": str(body.get("tid") or "").strip(),
    }
    data = _load_latent_notes()
    # 同一 tid 已在池里则直接返回，避免重复收藏
    tid = str(body.get("tid") or "").strip()
    if tid:
        for existing in data.get("notes") or []:
            if (
                isinstance(existing, dict)
                and str(existing.get("source_tid") or "") == tid
                and str(existing.get("source_kind") or "") == "thought_pool"
                and str(existing.get("status") or "") not in {"deleted", "used"}
            ):
                existing["dream_line"] = text
                existing["source_fragment"] = text
                existing["updated_at"] = ts
                _touch_latent_note_data(data)
                _save_latent_notes(data)
                return JSONResponse(
                    {"ok": True, "note": existing, "already": True},
                    headers={"Access-Control-Allow-Origin": "*"},
                )
    data["notes"] = [note] + data.get("notes", [])
    _touch_latent_note_data(data)
    _save_latent_notes(data)
    return JSONResponse({"ok": True, "note": note}, headers={"Access-Control-Allow-Origin": "*"})


@mcp.custom_route("/api/sanctum/thought-from-latent", methods=["POST"])
async def api_sanctum_thought_from_latent(request):
    """公开：取消 Thought Pool → Latent（按 note_id 或 source_tid 软删除）。"""
    from starlette.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400,
                            headers={"Access-Control-Allow-Origin": "*"})
    note_id = str(body.get("note_id") or body.get("id") or "").strip()
    tid = str(body.get("tid") or "").strip()
    if not note_id and not tid:
        return JSONResponse({"ok": False, "error": "note_id or tid required"}, status_code=400,
                            headers={"Access-Control-Allow-Origin": "*"})
    data = _load_latent_notes()
    notes = data.get("notes") or []
    removed: list[dict] = []
    ts = _latent_note_ts()
    for note in notes:
        if not isinstance(note, dict):
            continue
        if str(note.get("status") or "") in {"deleted"}:
            continue
        hit = False
        if note_id and str(note.get("id") or "") == note_id:
            hit = True
        elif (
            tid
            and str(note.get("source_tid") or "") == tid
            and str(note.get("source_kind") or "") == "thought_pool"
        ):
            hit = True
        if hit:
            note["status"] = "deleted"
            note["pinned"] = False
            note["deleted_at"] = ts
            note["updated_at"] = ts
            removed.append(note)
    if not removed:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404,
                            headers={"Access-Control-Allow-Origin": "*"})
    _touch_latent_note_data(data)
    _save_latent_notes(data)
    return JSONResponse(
        {"ok": True, "removed": len(removed), "notes": removed},
        headers={"Access-Control-Allow-Origin": "*"},
    )


# =============================================================
# /api/status — system status for Dashboard settings tab
# /api/status — Dashboard 设置页用系统状态
# =============================================================
@mcp.custom_route("/api/status", methods=["GET"])
async def api_system_status(request):
    """Return detailed system status for the settings panel."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        stats = await bucket_mgr.get_stats()
        return JSONResponse({
            "decay_engine": "running" if decay_engine.is_running else "stopped",
            "embedding_enabled": embedding_engine.enabled,
            "buckets": {
                "permanent": stats.get("permanent_count", 0),
                "dynamic": stats.get("dynamic_count", 0),
                "archive": stats.get("archive_count", 0),
                "total": stats.get("permanent_count", 0) + stats.get("dynamic_count", 0),
            },
            "using_env_password": bool(os.environ.get("OMBRE_DASHBOARD_PASSWORD", "")),
            "version": "1.3.0",
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# --- Entry point / 启动入口 ---
# ---- Liminal 扩展挂载（人格层+线索层+回执） ----
import liminal_ext
_liminal = liminal_ext.register(mcp, config["buckets_dir"])

if __name__ == "__main__":
    transport = config.get("transport", "stdio")
    logger.info(f"Ombre Brain starting | transport: {transport}")

    if transport in ("sse", "streamable-http"):
        import threading
        import uvicorn
        from starlette.middleware.cors import CORSMiddleware

        # --- Application-level keepalive: ping /health every 60s ---
        # --- 应用层保活：每 60 秒 ping 一次 /health，防止 Cloudflare Tunnel 空闲断连 ---
        async def _keepalive_loop():
            await asyncio.sleep(10)  # Wait for server to fully start
            async with httpx.AsyncClient() as client:
                while True:
                    try:
                        await client.get(f"http://localhost:{OMBRE_PORT}/health", timeout=5)
                        logger.debug("Keepalive ping OK / 保活 ping 成功")
                    except Exception as e:
                        logger.warning(f"Keepalive ping failed / 保活 ping 失败: {e}")
                    await asyncio.sleep(60)

        def _start_keepalive():
            loop = asyncio.new_event_loop()
            loop.run_until_complete(_keepalive_loop())

        async def _desire_heartbeat_loop():
            await asyncio.sleep(60)  # 等服务器完全启动
            try:
                from desire_engine import DESIRE_TICK_SECONDS as _tick_sec
            except Exception:
                _tick_sec = 900
            tick_sec = float(_tick_sec or 900)

            while True:
                try:
                    now = time.time()
                    # has_signal / idle 必须与 tick 间隔对齐，否则加速 tick 会 mag 漂移
                    has_signal = (now - _last_signal_ts[0]) < tick_sec
                    _desire.tick(idle_seconds=tick_sec, has_signal=has_signal)

                    # 节律状态日志
                    try:
                        rhythm = _desire.rhythm_state()
                        logger.info(f"Rhythm: {rhythm['label']} (val={rhythm['value']})")
                        grief = _desire.grief_state()
                        if grief["layer"] != "none":
                            logger.info(f"Grief layer: {grief['layer']} (protest_ticks={grief['protest_ticks']})")
                    except Exception as e:
                        logger.warning(f"Rhythm/grief state log failed: {e}")

                    # echo机制已关闭——念头池只靠新feel分析和手动pulse补充

                    intent = _desire.intent()
                    if intent:
                        asyncio.create_task(_execute_intent(intent))
                    logger.info("Desire heartbeat tick")
                except Exception as e:
                    logger.warning(f"Desire heartbeat failed: {e}")
                await asyncio.sleep(tick_sec)

        def _start_desire_heartbeat():
            loop = asyncio.new_event_loop()
            loop.run_until_complete(_desire_heartbeat_loop())

        t = threading.Thread(target=_start_keepalive, daemon=True)
        t.start()

        hb = threading.Thread(target=_start_desire_heartbeat, daemon=True)
        hb.start()

        # --- Add CORS middleware so remote clients (Cloudflare Tunnel / ngrok) can connect ---
        # --- 添加 CORS 中间件，让远程客户端（Cloudflare Tunnel / ngrok）能正常连接 ---
        if transport == "streamable-http":
            _app = mcp.streamable_http_app()
        else:
            _app = mcp.sse_app()

        async def _decay_background_loop():
            try:
                await decay_engine.ensure_started()
                while True:
                    await asyncio.sleep(3600)
            except Exception as e:
                logger.warning(f"Decay engine startup failed / 衰减引擎启动失败: {e}")

        def _start_decay_background():
            loop = asyncio.new_event_loop()
            loop.run_until_complete(_decay_background_loop())

        decay_thread = threading.Thread(target=_start_decay_background, daemon=True)
        decay_thread.start()

        _app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"],
        )
        logger.info("CORS middleware enabled for remote transport / 已启用 CORS 中间件")
        uvicorn.run(_app, host="0.0.0.0", port=OMBRE_PORT)
    else:
        mcp.run(transport=transport)
