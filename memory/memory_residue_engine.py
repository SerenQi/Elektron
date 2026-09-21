from __future__ import annotations

import json
import os
from typing import Any

import httpx

from desire_engine import DRIVE_EVENT_SCHEMA, DRIVE_KEYS, normalize_drive_event_brain, normalize_drive_key
from identity import AGENT_NAME


SOURCE = "dp_memory"
RUBRIC_VERSION = "dp_memory_v1_2026-07-08"
VALID_ENTRY_TYPES = {"feel", "memory", "letter", "writing", "window", "unresolved"}


def _clamp(value: Any, fallback: float = 0.0, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return fallback
    if v != v:
        return fallback
    return max(lo, min(hi, v))


def _as_list(value: Any, limit: int = 12) -> list[str]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, tuple):
        items = list(value)
    elif value:
        items = [value]
    else:
        items = []
    return [str(item).strip()[:120] for item in items if str(item).strip()][:limit]


def _hint_value(value: Any) -> float:
    if isinstance(value, str):
        named = {"low": 0.28, "mid": 0.55, "high": 0.82, "低": 0.28, "中": 0.55, "高": 0.82}
        if value.strip().lower() in named:
            return named[value.strip().lower()]
    return _clamp(value)


def normalize_memory_entry(raw: dict | None) -> dict:
    raw = raw if isinstance(raw, dict) else {}
    entry_type = str(raw.get("type") or "memory").strip().lower()
    if entry_type not in VALID_ENTRY_TYPES:
        entry_type = "memory"
    return {
        "id": str(raw.get("id") or raw.get("bucket_id") or "").strip()[:160],
        "type": entry_type,
        "created": str(raw.get("created") or raw.get("source_created") or "").strip()[:80],
        "content_preview": str(raw.get("content_preview") or raw.get("content") or "").strip()[:2400],
        "chord": str(raw.get("chord") or "").strip()[:40],
        "tags": _as_list(raw.get("tags")),
        "domain": _as_list(raw.get("domain")),
        "drive_tags": raw.get("drive_tags") if isinstance(raw.get("drive_tags"), dict) else {},
        "signal_hints": raw.get("signal_hints") if isinstance(raw.get("signal_hints"), dict) else {},
    }


def thought_limit(entry: dict) -> int:
    # dp_memory 不再 mint 念头——染色 Drive/Weather 即可。
    return 0


def normalize_memory_residue_event(event: dict | None, entry: dict | None = None) -> dict:
    event = event if isinstance(event, dict) else {}
    entry = normalize_memory_entry(entry or {})
    primary = normalize_drive_key(event.get("primary_drive"), "")
    explicit_drives = {
        drive: _clamp(value)
        for key, value in entry.get("drive_tags", {}).items()
        if (drive := normalize_drive_key(key, "")) in DRIVE_KEYS
    }
    if explicit_drives:
        primary = max(explicit_drives, key=explicit_drives.get)
    confidence = _clamp(event.get("confidence"), 0.0)
    intensity = _clamp(event.get("intensity"), 0.0)
    agency = _clamp(event.get("agency"), 0.0)
    secondary = event.get("secondary_drives") if isinstance(event.get("secondary_drives"), dict) else {}
    secondary = {
        drive: round(_clamp(value), 4)
        for key, value in secondary.items()
        if (drive := normalize_drive_key(key, "")) in DRIVE_KEYS and drive != primary
    }

    brain = event.get("brain") if isinstance(event.get("brain"), dict) else {}
    hints = entry.get("signal_hints") if isinstance(entry.get("signal_hints"), dict) else {}
    charge = _hint_value(hints.get("charge"))
    clutch = _hint_value(hints.get("clutch"))
    strain = _hint_value(hints.get("strain"))
    territorial = _hint_value(hints.get("territorial"))
    discernment = _hint_value(hints.get("discernment"))
    brain = normalize_drive_event_brain(
        {
            **brain,
            **({"release_pressure": charge, "novelty_pull": charge, "expression_pressure": charge * 0.7} if charge else {}),
            **({"closeness_pull": clutch} if clutch else {}),
            **({"tension_load": strain, "inward_pull": strain * 0.65} if strain else {}),
            **({"territorial_alarm": territorial, "anchor_target": "boundary"} if territorial else {}),
            **({"discernment_alarm": discernment} if discernment else {}),
            "source": SOURCE,
            "source_bucket": entry["id"],
            "source_type": entry["type"],
            "source_created": entry["created"],
            "agency": _clamp(brain.get("agency"), agency),
        }
    )
    for drive, value in explicit_drives.items():
        if drive != primary:
            secondary[drive] = max(secondary.get(drive, 0.0), round(value, 4))
    evidence = [str(item).strip()[:180] for item in event.get("evidence", []) if str(item).strip()][:3]
    # 与 dialogue_residue 对齐：记忆分析不代写 the agent 第一人称念头。
    _ = thought_limit(entry)

    return {
        "schema_version": DRIVE_EVENT_SCHEMA,
        "source": SOURCE,
        "primary_drive": primary,
        "secondary_drives": secondary,
        "intensity": round(intensity, 4),
        "confidence": round(confidence, 3),
        "agency": round(agency, 3),
        "event_label": str(event.get("event_label") or "dp_memory").strip()[:80],
        "brain": brain,
        "evidence": evidence,
        "thoughts": [],
        "source_bucket": entry["id"],
        "source_type": entry["type"],
        "source_created": entry["created"],
        "rubric_version": RUBRIC_VERSION,
    }


def memory_residue_available() -> bool:
    api_key, _, model = _memory_residue_api_config()
    return bool(api_key and model)


def _memory_residue_api_config() -> tuple[str, str, str]:
    api_key = (
        os.environ.get("DP_MEMORY_API_KEY")
        or os.environ.get("DIALOGUE_RESIDUE_API_KEY")
        or os.environ.get("SPEECH_EVENT_API_KEY")
        or os.environ.get("DEEPSEEK_API_KEY", "")
    )
    base_url = os.environ.get(
        "DP_MEMORY_BASE_URL",
        os.environ.get("DIALOGUE_RESIDUE_BASE_URL", os.environ.get("SPEECH_EVENT_BASE_URL", "https://api.deepseek.com/v1")),
    ).rstrip("/")
    # deepseek-chat retired 2026-07-24 → 400 Invalid Format if still used.
    model = os.environ.get(
        "DP_MEMORY_MODEL",
        os.environ.get(
            "DIALOGUE_RESIDUE_MODEL",
            os.environ.get("SPEECH_EVENT_MODEL", "deepseek-v4-flash"),
        ),
    )
    return api_key, base_url, model


async def classify_memory_residue_dp(entry: dict, preference: str = "", state_context: dict | None = None) -> dict:
    api_key, base_url, model = _memory_residue_api_config()
    if not api_key:
        raise RuntimeError("DP_MEMORY_API_KEY/DIALOGUE_RESIDUE_API_KEY/SPEECH_EVENT_API_KEY/DEEPSEEK_API_KEY is not set")
    normalized_entry = normalize_memory_entry(entry)
    if not normalized_entry["content_preview"]:
        raise ValueError("memory entry has no content_preview")

    preference = str(preference or "").strip()
    system_prompt = (
        "你是 dp_memory 记忆分析线。任务：读取一个 memory entry，"
        "输出一次慢速记忆 drive_event_v2。你不是 dialogue_residue；不要分析当前2+2对话窗。\n"
        "沿用下面这份旧 CLI 分身 preference 的口径，但 source 改为 dp_memory，"
        "brain.source 也必须是 dp_memory。\n\n"
        f"{preference}\n\n"
        "覆盖要求：输出严格 JSON；schema_version 必须是 drive_event_v2；source 必须是 dp_memory。"
        "primary_drive 只能是 attachment, libido, possessiveness, reflection, stewardship, curiosity, social, fatigue, stress 或空字符串；"
        "discernment 不是9维 drive，如果有皱眉请写 brain.discernment_alarm 和 evidence。"
        "secondary_drives 最多2个。"
        f"thoughts 必须是空数组 []——记忆分析只染 Drive/Weather，禁止代写 {AGENT_NAME} 第一人称念头。"
    )
    user_prompt = json.dumps(
        {
            "current_state": state_context or {},
            "entry": normalized_entry,
        },
        ensure_ascii=False,
    )

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "temperature": 0,
                "max_tokens": 900,
                # V4 defaults thinking=on; analyzers need stable non-thinking JSON
                # like the old deepseek-chat path.
                "thinking": {"type": "disabled"},
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()
    if content.startswith("```"):
        content = "\n".join(content.splitlines()[1:]).replace("```", "").strip()
    return normalize_memory_residue_event(json.loads(content), normalized_entry)
