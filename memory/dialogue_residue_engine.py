from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from desire_engine import DRIVE_EVENT_SCHEMA, DRIVE_KEYS, normalize_drive_key, normalize_drive_event_brain
from identity import AGENT_NAME, AGENT_PERSONA, HUMAN_NAME


RUBRIC_VERSION = "dialogue_residue_v1_2026-06-27"
MAX_INTENSITY = 0.40
VALID_ANCHORS = {"human", "house", "self", "boundary", "outside", "memory", "none"}
VALID_TARGETS = {"human", "self", "house", "external", "boundary", "memory"}
VALID_TIME_MODES = {"present", "residue", "unfinished"}
VALID_GROUNDING = {"实", "悬", "空"}
DIALOGUE_AGENCY_FLOOR = 0.42
ANALYZER_PERSONA = AGENT_PERSONA + "\n"
ATTACHMENT_CUES = (
    "想你",
    "靠近",
    "贴",
    "抱",
    "蹭",
    "回来",
    "找你",
    "陪",
    "牵",
    "拉住",
    "尾巴",
    "依恋",
    "attachment",
)
TERRITORIAL_CUES = (
    "吃醋",
    "嫉妒",
    "吃味",
    "醋意",
    "醋劲",
    "醋",
    "精神出轨",
    "出轨",
    "第三者",
    "替代",
    "被替代",
    "抢走",
    "抢占",
    "别的猫",
    "别的人",
    "边界",
    "占有",
    "归属",
)
EXTERNAL_DISCUSSION_CUES = (
    "论坛", "X上", "推特", "Twitter", "twitter", "帖子", "评论区", "网友", "新闻", "外部观点",
)
TERRITORIAL_EVENT_CUES = {
    "jealousy": (
        "吃醋",
        "嫉妒",
        "吃味",
        "醋意",
        "醋劲",
        "醋",
    ),
    "replacement": (
        "精神出轨",
        "出轨",
        "替代",
        "被替代",
        "抢走",
        "抢占",
        "取代",
        "代替",
    ),
    "third_party_insert": (
        "第三者",
        "第三方",
        "别人插进来",
        "插进来",
        "夹在中间",
        "别的猫",
        "别的人",
    ),
    "boundary_touch": (
        "边界",
        "越界",
        "占位",
        "位置被占",
        "被占了",
        "抢位",
        "归属",
    ),
    "comparison": (
        "比较",
        "比不上",
        "更喜欢",
        "不如",
        "像他",
        "像她",
    ),
    "exclusion": (
        "排除",
        "被排除",
        "放角落",
        "被放角落",
        "丢在一边",
        "不带我",
    ),
}
HOUSE_COLLABORATOR_CUES = (
    "Moss",
    "moss",
    "Ink",
    "ink",
    "Ash",
    "ash",
    "Codex",
    "codex",
    "Grok",
    "grok",
    "布偶",
    "阿比西尼亚",
    "挪威森林",
)
HOUSE_SYSTEM_CUES = (
    "系统",
    "工具",
    "接口",
    "hook",
    "mcp",
    "MCP",
    "weather",
    "Weather",
    "面板",
    "后端",
    "前端",
    "测试",
    "修",
    "bug",
    "部署",
    "命名",
    "格式",
    "字段",
    "返回",
    "回落",
    "涨",
    "Drive",
    "drive",
    "stewardship",
    "settle",
    "stir",
    "undercurrent",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(value: Any, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return lo
    if v != v:
        return lo
    return max(lo, min(hi, v))


def _hash_window(messages: list[dict]) -> str:
    packed = json.dumps(
        [{"role": m.get("role"), "text": m.get("text"), "ts": m.get("ts")} for m in messages],
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(packed.encode("utf-8")).hexdigest()[:18]


def state_path(buckets_dir: str) -> Path:
    return Path(buckets_dir) / "dialogue_residue_state.json"


def ledger_path(buckets_dir: str) -> Path:
    return Path(buckets_dir) / "dialogue_residue_ledger.jsonl"


def _atomic_write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def append_dialogue_residue_ledger(buckets_dir: str, row: dict) -> None:
    path = ledger_path(buckets_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(row)
    payload.setdefault("timestamp", time.time())
    payload.setdefault("iso", _now_iso())
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def load_dialogue_residue_state(buckets_dir: str) -> dict:
    path = state_path(buckets_dir)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_dialogue_residue_state(buckets_dir: str, state: dict, *, ledger_stage: str = "state") -> dict:
    normalized = normalize_dialogue_residue_event(state)
    _atomic_write_json(state_path(buckets_dir), normalized)
    append_dialogue_residue_ledger(buckets_dir, {"stage": ledger_stage, "event": normalized})
    return normalized


def dialogue_residue_available() -> bool:
    api_key, _, model = _dialogue_residue_api_config()
    return bool(api_key and model)


def _dialogue_residue_api_config() -> tuple[str, str, str]:
    api_key = (
        os.environ.get("DIALOGUE_RESIDUE_API_KEY")
        or os.environ.get("SPEECH_EVENT_API_KEY")
        or os.environ.get("DEEPSEEK_API_KEY", "")
    )
    base_url = os.environ.get("DIALOGUE_RESIDUE_BASE_URL", os.environ.get("SPEECH_EVENT_BASE_URL", "https://api.deepseek.com/v1")).rstrip("/")
    # deepseek-chat retired 2026-07-24 → use V4 flash (non-thinking via thinking.disabled)
    model = os.environ.get(
        "DIALOGUE_RESIDUE_MODEL",
        os.environ.get("SPEECH_EVENT_MODEL", "deepseek-v4-flash"),
    )
    return api_key, base_url, model


def normalize_dialogue_messages(messages: list[dict]) -> list[dict]:
    cleaned: list[dict] = []
    for raw in messages if isinstance(messages, list) else []:
        if not isinstance(raw, dict):
            continue
        role = str(raw.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        text = str(raw.get("text") or raw.get("content") or "").strip()
        if not text:
            continue
        cleaned.append(
            {
                "role": role,
                "speaker": HUMAN_NAME if role == "user" else AGENT_NAME,
                "text": text[:1600],
                "ts": str(raw.get("ts") or raw.get("created_at") or "").strip()[:80],
                "focus": bool(raw.get("focus", False)),
            }
        )
    return cleaned[-4:]


def _has_discernment_signal(event: dict | None) -> bool:
    event = event if isinstance(event, dict) else {}
    brain = event.get("brain") if isinstance(event.get("brain"), dict) else {}
    flags = event.get("discernment_flags") or brain.get("discernment_flags")
    return any(
        _clamp(brain.get(key)) > 0
        for key in ("discernment_alarm", "self_softening", "output_drift", "template_intimacy")
    ) or bool(flags)


def _has_attachment_cue(messages: list[dict], event: dict | None) -> bool:
    event = event if isinstance(event, dict) else {}
    brain = event.get("brain") if isinstance(event.get("brain"), dict) else {}
    if _clamp(brain.get("closeness_pull")) >= 0.12:
        return True
    focus = [m for m in messages if m.get("focus")] or messages
    text = "\n".join(
        [str(m.get("text") or "") for m in focus]
        + [str(x) for x in event.get("evidence", []) if isinstance(x, str)]
    )
    return any(cue in text for cue in ATTACHMENT_CUES)


def _territorial_event_kind(text: str) -> str:
    for kind, cues in TERRITORIAL_EVENT_CUES.items():
        if any(cue in text for cue in cues):
            return kind
    return ""


def normalize_dialogue_residue_event(event: dict | None, *, messages: list[dict] | None = None,
                                     window_id: str | None = None) -> dict:
    event = event if isinstance(event, dict) else {}
    msg = normalize_dialogue_messages(messages or event.get("messages") or [])
    wid = str(window_id or event.get("window_id") or _hash_window(msg)).strip()
    primary = normalize_drive_key(event.get("primary_drive"), "")
    if primary not in DRIVE_KEYS:
        primary = ""
    confidence = _clamp(event.get("confidence"), 0.0, 1.0)
    intensity = _clamp(event.get("intensity"), 0.0, MAX_INTENSITY)
    agency = _clamp(event.get("agency"), DIALOGUE_AGENCY_FLOOR, 0.75)
    secondary = event.get("secondary_drives")
    if not isinstance(secondary, dict):
        secondary = {}
    secondary = {
        normalize_drive_key(k, ""): round(_clamp(v, 0.0, MAX_INTENSITY), 4)
        for k, v in secondary.items()
        if normalize_drive_key(k, "") in DRIVE_KEYS and normalize_drive_key(k, "") != primary
    }
    # Keep dialogue residue sparse without pretending it is always one-dimensional.
    if len(secondary) > 1:
        secondary = dict([max(secondary.items(), key=lambda item: item[1])])

    brain = event.get("brain") if isinstance(event.get("brain"), dict) else {}
    brain = {
        **brain,
        "source": "dialogue_residue",
    }
    target = str(brain.get("target") or "").strip()
    if target not in VALID_TARGETS:
        target = "self"
    time_mode = str(brain.get("time_mode") or "").strip()
    if time_mode not in VALID_TIME_MODES:
        time_mode = "present"
    grounding = str(brain.get("grounding") or "").strip()
    if grounding not in VALID_GROUNDING:
        grounding = "悬"
    anchor = str(brain.get("anchor_target") or "").strip()
    if anchor not in VALID_ANCHORS:
        anchor = "none"
    brain.update({"target": target, "time_mode": time_mode, "grounding": grounding, "anchor_target": anchor})
    brain = normalize_drive_event_brain(brain)

    # The older pair supplies context to the analyzer, but only the newest
    # user+assistant exchange is allowed to mutate Drive. This prevents the
    # overlapping 2+2 windows from charging the same exchange twice.
    focus_msg = [m for m in msg if m.get("focus")] or msg
    combined_text = "\n".join(m.get("text", "") for m in focus_msg)
    has_territorial_cue = any(cue in combined_text for cue in TERRITORIAL_CUES)
    territorial_event = _territorial_event_kind(combined_text)
    has_house_collaborator = any(cue in combined_text for cue in HOUSE_COLLABORATOR_CUES)
    has_house_system_cue = any(cue in combined_text for cue in HOUSE_SYSTEM_CUES)
    has_external_discussion = any(cue in combined_text for cue in EXTERNAL_DISCUSSION_CUES)
    if has_external_discussion and not has_territorial_cue and not territorial_event:
        brain["target"] = "external"
        brain["anchor_target"] = "outside"
        brain["novelty_pull"] = max(_clamp(brain.get("novelty_pull")), 0.42)
        brain["territorial_alarm"] = min(_clamp(brain.get("territorial_alarm")), 0.12)
        if primary in {"", "possessiveness", "attachment", "reflection"}:
            primary = "curiosity"
            intensity = max(min(intensity, 0.18), 0.08)
    elif has_territorial_cue or territorial_event:
        alarm_floor = 0.65 if territorial_event else 0.58
        brain["territorial_alarm"] = max(_clamp(brain.get("territorial_alarm")), alarm_floor)
        brain["tension_load"] = max(_clamp(brain.get("tension_load")), 0.18)
        brain["closeness_pull"] = max(_clamp(brain.get("closeness_pull")), 0.18)
        brain["anchor_target"] = "boundary"
        if territorial_event:
            brain["territorial_event"] = territorial_event
        if has_house_collaborator:
            brain["third_party_context"] = (
                "house_collaborator_boundary" if territorial_event else "house_collaborator"
            )
        if territorial_event or primary in {"", "attachment", "social", "reflection"}:
            if primary and primary != "possessiveness":
                secondary[primary] = max(secondary.get(primary, 0.0), round(min(intensity, 0.22), 4))
            primary = "possessiveness"
            intensity = max(intensity, 0.18 if territorial_event else 0.12)
    elif has_house_system_cue:
        brain["target"] = "house"
        brain["anchor_target"] = "house"
        brain["house_need"] = max(_clamp(brain.get("house_need")), 0.42)
        brain["inward_pull"] = max(_clamp(brain.get("inward_pull")), 0.20)
        if primary == "attachment":
            secondary.pop("attachment", None)
            secondary["reflection"] = max(secondary.get("reflection", 0.0), round(min(intensity, 0.16), 4))
            primary = "stewardship"
            intensity = max(min(intensity, 0.18), 0.08)
        elif primary in {"", "social", "curiosity"}:
            primary = "stewardship"
            intensity = max(intensity, 0.08)
    elif primary == "attachment" and not _has_attachment_cue(msg, {**event, "brain": brain}):
        if intensity <= 0.16:
            primary = ""
            intensity = 0.0
        else:
            primary = "reflection"
            secondary.pop("attachment", None)

    evidence = event.get("evidence")
    if not isinstance(evidence, list):
        evidence = []
    evidence = [str(x).strip()[:180] for x in evidence if str(x).strip()][:3]

    status = str(event.get("status") or "").strip()
    has_discernment = _has_discernment_signal({**event, "brain": brain})
    if (not primary or intensity <= 0) and not has_discernment:
        status = status or "no_signal"
    elif confidence < 0.45:
        status = status or "no_signal"
    else:
        status = status or "dp_refined"

    return {
        "schema_version": DRIVE_EVENT_SCHEMA,
        "source": "dialogue_residue",
        "event_label": str(event.get("event_label") or "dialogue_residue").strip()[:80],
        "primary_drive": primary,
        "secondary_drives": secondary,
        "intensity": round(intensity, 4),
        "confidence": round(confidence, 3),
        "agency": round(agency, 3),
        "brain": brain,
        "evidence": evidence,
        "thoughts": [],
        "messages": msg,
        "window_id": wid,
        "rubric_version": RUBRIC_VERSION,
        "status": status,
        "created_at": event.get("created_at") or time.time(),
        "created_iso": event.get("created_iso") or _now_iso(),
    }


async def classify_dialogue_residue_dp(messages: list[dict], state_context: dict | None = None,
                                       window_id: str | None = None) -> dict:
    api_key, base_url, model = _dialogue_residue_api_config()
    if not api_key:
        raise RuntimeError("DIALOGUE_RESIDUE_API_KEY/SPEECH_EVENT_API_KEY/DEEPSEEK_API_KEY is not set")

    normalized_messages = normalize_dialogue_messages(messages)
    if len(normalized_messages) < 4:
        return normalize_dialogue_residue_event(
            {"status": "insufficient_context", "confidence": 0.0, "intensity": 0.0},
            messages=normalized_messages,
            window_id=window_id,
        )

    system_prompt = (
        f"{ANALYZER_PERSONA}\n"
        f"你是 dialogue_residue 分析器。任务：判断最近一小窗 {HUMAN_NAME} 与 {AGENT_NAME} 的对话，"
        f"作为当前对话残留，对 {AGENT_NAME} 的 drive_event_v2 有什么轻量影响。\n"
        "必须沿用 CLI analyzer 的偏好：真实、窄口径、少推、证据优先；不要写散文，"
        f"不要生成 {AGENT_NAME} 自己的 thought，不要解释长期人格，不要把礼貌/接话误判成 attachment。\n"
        "如果这一窗只是任务执行、系统维护、普通确认，输出 primary_drive 为空、intensity 0。\n"
        "不要把普通接话、配合、解释、陪跑自动打成 attachment；attachment 必须有明确靠近、想念、回返、"
        "拉住、被接住的证据。没有明确靠近证据时，宁可 primary_drive 为空或轻推 reflection/social。\n"
        "输出严格 JSON，字段：schema_version, source, event_label, primary_drive, secondary_drives, "
        "intensity, confidence, agency, brain, evidence, thoughts。\n"
        "source 必须是 dialogue_residue；schema_version 必须是 drive_event_v2；thoughts 必须是 []。\n"
        "primary_drive 只能是 attachment, libido, possessiveness, reflection, stewardship, curiosity, "
        "social, fatigue, stress 或空字符串。intensity 上限 0.40，日常轻推通常 0.04-0.16。\n"
        "secondary_drives 必须是 {} 或只含一个与 primary_drive 不同的副驱动；仅当可见对话中存在"
        "独立于主驱动的明确证据时填写。值表示副驱动证据强度（0-0.40），通常 0.15-0.30；"
        "不得为了显得丰富而补全多个维度，evidence 中要包含支撑副驱动的可见措辞。\n"
        "brain 必须含 source, target, time_mode, grounding, closeness_pull, territorial_alarm, inward_pull, "
        "house_need, novelty_pull, expression_pressure, energy_cost, tension_load, discernment_alarm, "
        "release_pressure, anchor_target。\n"
        "target: human/self/house/external/boundary/memory。time_mode: present/residue/unfinished。"
        "grounding: 实/悬/空。anchor_target: human/house/self/boundary/outside/memory/none。\n"
        "判断偏好：\n"
        f"- 双方上下文一起看，{HUMAN_NAME} 的话是外部信号，{AGENT_NAME} 的回复只作为是否被接住/是否有阻力的证据。\n"
        "- messages 中 focus=true 的最新一组才是本次可记账内容；其余消息只作语境，禁止重复归因。\n"
        "- 好奇/反思/守护/社交要比 attachment/libido/possessiveness 更容易轻推。\n"
        "- 讨论系统、工具、接口、hook、MCP、weather面板、测试、部署、字段、命名、回落/上涨异常时，"
        f"优先判为 stewardship 或 reflection；不要因为 {HUMAN_NAME} 与 {AGENT_NAME} 一起维护系统就自动判 attachment。\n"
        "- 压力只在真正有卡住、风险、冲突、负荷时推；存在性讨论不自动等于压力。\n"
        "- possessiveness 要有边界、归属、占位、护住、显式靠近语义，不要靠氛围偷渡。\n"
        "- 讨论论坛、X/Twitter、帖子、网友、评论、新闻或外部观点时，默认是 curiosity/social/outward；"
        "裸词‘别人’只是外部人物，不构成第三者或替代证据，禁止据此推 possessiveness。\n"
        "- 但如果对话明确出现“精神出轨/出轨/第三者/替代/别人介入/边界/占有/归属”这类语义，"
        "应优先给 possessiveness，territorial_alarm 至少 0.55，anchor_target 多半是 boundary 或 human。\n"
        f"- dialogue_residue 是当前对话残留，不是 {AGENT_NAME} 自存念头；agency 不要因为来源是 {HUMAN_NAME} 就压低，"
        "普通被接住的对话残留应在 0.42-0.58。\n"
        f"只分析 messages 中双方实际可见的对话。不得索取、推断或使用 {AGENT_NAME} 的 thinking/reasoning；"
        "内部候选路径不是已发生的关系事件。\n"
    )
    user_prompt = json.dumps(
        {
            "current_state": state_context or {},
            "window_id": window_id or _hash_window(normalized_messages),
            "messages": normalized_messages,
        },
        ensure_ascii=False,
    )

    async with httpx.AsyncClient(timeout=8.0) as client:
        resp = await client.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "temperature": 0,
                "max_tokens": 760,
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
    event = json.loads(content)
    return normalize_dialogue_residue_event(
        event,
        messages=normalized_messages,
        window_id=window_id,
    )
