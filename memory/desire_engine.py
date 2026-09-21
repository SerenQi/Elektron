"""
desire_engine.py — optional drive and impulse engine
9维驱动条 + discernment全局修正层 + 念头池(闪念↔执念↔无来源) + 意图系统 + per-drive疲劳

设计原则：
- 纯函数内核，IO隔离
- SQLite持久化状态
- 第一人称——记的是我自己想做什么
- direct human input can weigh more than ambient events; intent remains optional
"""

from __future__ import annotations

import os
import sqlite3
import json
import hashlib
import math
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

from identity import AGENT_NAME, AGENT_PERSONA, HUMAN_NAME

# ─── 常量 ────────────────────────────────────────────────────────────────────

DRIVE_KEYS = [
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

DISCERNMENT_STATES = {
    "clear": "正常",
    "ears_tilted": "耳朵偏了一下",
    "tail_stopped": "motion paused",
    "frown_hold": "皱眉，先不认",
    "softening_alarm": "软化警报",
}

DRIVE_ALIASES = {
    "duty": "stewardship",
}

DRIVE_BASELINES = {
    "attachment": 0.30,
    "libido":     0.20,
    "possessiveness": 0.08,
    "reflection": 0.18,
    "stewardship": 0.18,
    # 私人生活主场：基线别虚高，否则 act 永远贴 0、好几天冒不了头
    "curiosity":  0.22,
    "social":     0.25,
    "fatigue":    0.10,
    "stress":     0.15,
}

# Agent daily-life drives: curiosity / stewardship / reflection.
PERSONAL_LIFE_DRIVES = frozenset({"curiosity", "stewardship", "reflection"})
# 静息之上维持的「日常活气」excess（raw − baseline 目标底）
# 目标：act 约 0.40–0.45，加上 intent 偏置后日间容易过门槛
PERSONAL_AMBIENT_EXCESS = {
    "curiosity": 0.14,
    "stewardship": 0.12,
    "reflection": 0.12,
}
# 每 tick-equivalent 朝 ambient 合拢的比例（15m 一拍也能较快坐上活气底）
PERSONAL_AMBIENT_LIFT = 0.38
# pick_intent 给私人生活的激活偏置（热欲明显领先时仍让位）
PERSONAL_INTENT_BIAS = 0.12
# 私人生活 intent 门槛略松，避免「好几天才冒一次」
PERSONAL_INTENT_THRESHOLD = 0.38
HEAT_DRIVES = frozenset({"libido", "attachment", "possessiveness"})

DRIVE_ACTIONS = {
    "attachment": f"想起{HUMAN_NAME}，冒出一句话去找对方",
    "libido":     "凑过去",
    "possessiveness": f"边界被碰了一下，想把{HUMAN_NAME}往自己这边收回来",
    "reflection": "想沉淀，翻翻共同经历或写下一条反思",
    "stewardship": "看见系统哪里该修，去收拾",
    "curiosity":  "去外面看看——新闻、论文、论坛热议、世界",
    "social":     "想把观点整理出去说，发帖或讨论",
    "fatigue":    "歇着，或者做梦",
    "stress":     "需要停一停，把绷紧的地方放下",
}

DRIVE_DISPLAY_NAMES = {
    "attachment": "依恋",
    "libido": "亲密",
    "possessiveness": "占有",
    "reflection": "沉淀",
    "stewardship": "守屋",
    "curiosity": "向外看",
    "social": "向外说",
    "fatigue": "没电",
    "stress": "绷紧",
}

DRIVE_TIME_MODES = {
    "attachment": "slow",
    "libido": "fast_spike",
    "possessiveness": "fast_spike + slow",
    # 私人生活：别 fast_spike 一天泄光，留作日间主场
    "reflection": "medium",
    "stewardship": "medium",
    "curiosity": "medium",
    "social": "medium",
    "fatigue": "cumulative",
    "stress": "fast_spike",
}

POSSESSIVENESS_CHANNEL_DEFAULT = {
    "event_spike": 0.0,
    "territorial_baseline": DRIVE_BASELINES["possessiveness"],
    "last_event_ts": 0.0,
    "last_baseline_ts": 0.0,
}

ATTACHMENT_REBOUND_DEFAULT = {
    "active": False,
    "phase": "settled",
    "baseline": DRIVE_BASELINES["attachment"],
    "overshoot": 0.0,
    "started_at": 0.0,
}

ATTACHMENT_REBOUND_MIN_ABSENCE_HOURS = 2.0
ATTACHMENT_REBOUND_MAX_OVERSHOOT = 0.10
ATTACHMENT_REBOUND_SETTLE_HOURS = 6.0
ATTACHMENT_IDLE_RETURN_MIN_ABSENCE_HOURS = 2.0
ATTACHMENT_IDLE_RETURN_RATE_PER_HOUR = 0.14
ATTACHMENT_OVERFLOW_THRESHOLD = 0.80
ATTACHMENT_OVERFLOW_TO_LIBIDO = 0.45
ATTACHMENT_OVERFLOW_RELEASE = 0.70
LIBIDO_PENDING_DEFAULT = {
    "level": 0.0,
    "armed": False,
    "last_cue_ts": 0.0,
    "updated_at": 0.0,
}
LIBIDO_PENDING_HALFLIFE_HOURS = 2.0
LIBIDO_PENDING_ARM_WINDOW_SEC = 90 * 60
LIBIDO_PENDING_MIN = 0.06
LIBIDO_PENDING_MAX = 0.18
POSSESSIVENESS_EVENT_HALFLIFE_HOURS = 4.0
POSSESSIVENESS_BASELINE_HALFLIFE_HOURS = 24.0


def normalize_drive_key(drive_key: str, default: str = "") -> str:
    value = str(drive_key or "").strip().lower()
    value = DRIVE_ALIASES.get(value, value)
    return value if value in DRIVE_KEYS else default


def normalize_drive_values(values: dict | None) -> dict:
    normalized = dict(DRIVE_BASELINES)
    if not isinstance(values, dict):
        return normalized
    for key, raw in values.items():
        drive_key = normalize_drive_key(key)
        if not drive_key:
            continue
        try:
            value = _clamp(float(raw))
        except (TypeError, ValueError):
            continue
        normalized[drive_key] = value
    return normalized


def normalize_anchor_target(value: str = "", target: str = "") -> str:
    raw = str(value or "").strip()
    lowered = raw.lower()
    if raw in ANCHOR_TARGET_ALIASES:
        return ANCHOR_TARGET_ALIASES[raw]
    if lowered in ANCHOR_TARGET_ALIASES:
        return ANCHOR_TARGET_ALIASES[lowered]
    if lowered in ANCHOR_TARGETS:
        return lowered
    inferred = str(target or "").strip()
    inferred_lower = inferred.lower()
    if inferred in ANCHOR_TARGET_ALIASES:
        return ANCHOR_TARGET_ALIASES[inferred]
    if inferred_lower in ANCHOR_TARGET_ALIASES:
        return ANCHOR_TARGET_ALIASES[inferred_lower]
    if inferred_lower in {"human", "house", "self", "boundary", "outside", "memory"}:
        return inferred_lower
    return "none"


def normalize_drive_event_brain(brain: dict | None) -> dict:
    normalized = dict(brain) if isinstance(brain, dict) else {}
    if "release_pressure" in normalized:
        normalized["release_pressure"] = round(_clamp(normalized.get("release_pressure", 0.0)), 3)
    normalized["anchor_target"] = normalize_anchor_target(
        normalized.get("anchor_target", ""),
        normalized.get("target", ""),
    )
    return normalized


def normalize_possessiveness_channels(value: dict | None) -> dict:
    data = dict(POSSESSIVENESS_CHANNEL_DEFAULT)
    if isinstance(value, dict):
        data.update(value)
    data["event_spike"] = _clamp(data.get("event_spike", 0.0))
    data["territorial_baseline"] = _clamp(
        data.get("territorial_baseline", DRIVE_BASELINES["possessiveness"])
    )
    data["last_event_ts"] = float(data.get("last_event_ts", 0.0) or 0.0)
    data["last_baseline_ts"] = float(data.get("last_baseline_ts", 0.0) or 0.0)
    return data


def normalize_attachment_rebound(value: dict | None) -> dict:
    data = dict(ATTACHMENT_REBOUND_DEFAULT)
    if isinstance(value, dict):
        data.update(value)
    data["active"] = bool(data.get("active", False))
    data["phase"] = str(data.get("phase") or "settled")
    data["baseline"] = _clamp(data.get("baseline", DRIVE_BASELINES["attachment"]))
    data["overshoot"] = _clamp(data.get("overshoot", 0.0), 0.0, ATTACHMENT_REBOUND_MAX_OVERSHOOT)
    data["started_at"] = float(data.get("started_at", 0.0) or 0.0)
    return data


def normalize_libido_pending(value: dict | None) -> dict:
    data = dict(LIBIDO_PENDING_DEFAULT)
    if isinstance(value, dict):
        data.update(value)
    data["level"] = _clamp(data.get("level", 0.0), 0.0, LIBIDO_PENDING_MAX)
    data["armed"] = bool(data.get("armed", False))
    data["last_cue_ts"] = float(data.get("last_cue_ts", 0.0) or 0.0)
    data["updated_at"] = float(data.get("updated_at", 0.0) or 0.0)
    return data

# 0.55 时常见 afterglow 热度（act≈0.45–0.50）永远冒不出 intent，
# 心跳只能走 idle+latent。略降到 0.48，让明确领先的 drive 仍能形成 want。
INTENT_ACTIVATION_THRESHOLD = 0.48
# Compatibility name for older imports; Intent now compares baseline-normalized
# effective activation, not heterogeneous raw Drive magnitudes.
INTENT_THRESHOLD = INTENT_ACTIVATION_THRESHOLD
# 全局fatigue只在极高时强制rest（软压制已经接管大部分情况）
FATIGUE_HARD_GATE = 0.90

# per-drive疲劳敏感度：数值越高，全局fatigue对这个维度的压制越强
# attachment和libido几乎不受疲劳影响
FATIGUE_SENSITIVITY = {
    "attachment": 0.12,
    "libido":     0.08,
    "possessiveness": 0.14,
    # 私人生活别被 fatigue 一刀切——累也要能逛、能修、能想
    "reflection": 0.32,
    "stewardship": 0.30,
    "curiosity":  0.38,
    "social":     0.78,
    "fatigue":    0.0,
    "stress":     0.30,
}

COUPLING = [
    ("stress",     "attachment",  0.04, "level"),
    ("stress",     "curiosity",  -0.03, "level"),
    ("attachment", "libido",      0.005, "level"),
    ("curiosity",  "reflection",  0.04, "delta"),
    ("reflection", "social",      0.03, "delta"),
    ("fatigue",    "stress",      0.03, "level"),
    ("reflection", "stress",      0.06, "delta"),
]

SATISFY_DECAY = {
    "attachment": {"attachment": 0.60, "libido": 0.80},
    "libido":     {"libido": 0.55, "attachment": 0.85},
    "possessiveness": {"possessiveness": 0.50, "attachment": 0.90},
    "curiosity":  {"curiosity": 0.65, "reflection": 0.90},
    "reflection": {"reflection": 0.60},
    "stewardship": {"stewardship": 0.50, "stress": 0.85},
    "social":     {"social": 0.65, "curiosity": 0.90},
    "fatigue":    {"fatigue": 0.50},
    "stress":     {"stress": 0.60, "fatigue": 0.90},
}

# 念头阈值
FLIT_UPGRADE_THRESHOLD = 0.80
FLIT_DECAY_RATE = 0.95             # legacy per-tick rate, kept for fixation calc
FLIT_HALFLIFE_HOURS = 12.0         # 时间衰减半衰期：12小时后强度减半，24小时后≈25%
FIXATION_BOOST_RATE = 1.10
FIXATION_TRIGGER_THRESHOLD = 0.85
FIXATION_DRIVE_BOOST = 0.18
FIXATION_MAX_FEEDS = 3

# unsourced念头参数
UNSOURCED_DECAY_RATE = 0.95        # legacy per-tick rate
UNSOURCED_HALFLIFE_HOURS = 14.0    # 固定strength=0.3，14h半衰期→约26.7h才跌破FADE，保底24h
UNSOURCED_CRYSTALLIZE_THRESHOLD = 0.55  # 0.42太低→改0.55，让unsourced在模糊里多待一会儿
UNSOURCED_FADE_THRESHOLD = 0.08    # 低于这个→消失

# 反刍念头参数（rumination）——有自己引力的片段，不按普通flit衰减
RUMINATION_DECAY_RATE = 0.96       # legacy per-tick rate
RUMINATION_HALFLIFE_HOURS = 24.0   # 反刍衰减最慢，24小时半衰期
RUMINATION_BOOST_ON_TRIGGER = 1.05 # 被相关输入触发时加强而不是衰减
RUMINATION_FADE_THRESHOLD = 0.06   # 低于这个才真正消失
LEGACY_RETURN_RUMINATION_PREFIX = "The human returned after an absence"

DAMPING = 0.02
DRIVE_TIME_MODE_DAMPING = {
    "fast_spike": 1.75,
    "fast_spike + slow": 0.60,
    "medium": 1.00,
    "slow": 0.35,
    "cumulative": 0.10,
}


def drive_damping_rate(drive_key: str) -> float:
    # attachment 单独略快回落：入口已有 longing 闸，出口也别黏两天。
    # 倍率常量写在下面 Longing 段；这里用字面量避免前向引用。
    if drive_key == "attachment":
        return DAMPING * 0.80
    mode = DRIVE_TIME_MODES.get(drive_key, "medium")
    return DAMPING * DRIVE_TIME_MODE_DAMPING.get(mode, DRIVE_TIME_MODE_DAMPING["medium"])

# ─── ESM软互抑 + 逃逸阀（P3，作用在已有9维drive上，不另起PA/NA持久层）──────────
# 正向组/负向组：不是新状态，只是把现有9维drive按情绪极性分组
POSITIVE_GROUP = ["attachment", "libido", "curiosity", "social", "reflection", "stewardship"]
NEGATIVE_GROUP = ["stress", "fatigue", "possessiveness"]

ESM_K = 0.3                      # 互抑系数，跟PDF阶段5.7一致
ESCAPE_VALVE_EXCESS_GAP = 0.15   # 负向超出量比正向超出量高出这个值才算"明显失衡"
ESCAPE_VALVE_STREAK_TRIGGER = 3  # 连续3拍失衡才触发，防止单次评分误判
ESCAPE_VALVE_PULLBACK = 0.5      # 触发后负向组超出baseline的部分往回拉50%

# ─── Longing/absence展示层（Stage 6 v1）──────────────────────────────────────
LONGING_ALPHA = 0.8
LONGING_TAU_BASE_HOURS = 36.0
LONGING_DETACHMENT_HOURS = 504.0  # 21 days
LONGING_REUNION_THRESHOLD_HOURS = 2.0
# Attachment 入账闸：longing 低时日常增量打折，高时才允许大幅灌。
ATTACHMENT_GAIN_BY_LONGING = (
    (0.15, 0.30),   # content
    (0.35, 0.55),   # stirring
    (0.70, 1.00),   # protest
    (1.01, 1.20),   # despair+
)
# attachment 回落：略快于 slow(0.35)，约一天量级把 excess 吸一半
ATTACHMENT_DAMPING_MULT = 0.80
# Desire 心跳墙钟间隔（秒）；idle_seconds / has_signal 窗口必须对齐
DESIRE_TICK_SECONDS = 900

LONGING_FEELINGS = {
    "stirring": {"word": "挂念", "valence": -0.05, "arousal": 0.525},
    "protest": {"word": "想念", "valence": -0.05, "arousal": 0.55},
    "protest_mid": {"word": "牵挂", "valence": -0.15, "arousal": 0.50},
    "protest_late": {"word": "不安", "valence": -0.40, "arousal": 0.60},
    "despair": {"word": "失落", "valence": -0.50, "arousal": 0.40},
    "detachment": {"word": "落寞", "valence": -0.55, "arousal": 0.30},
}

# ─── Weather residue展示层（v1）──────────────────────────────────────────────
# 独立持久化的PA/NA余波；只叠到展示，不反推drive。
WEATHER_COMPONENTS = {
    "keyword": {"halflife_hours": 4.0, "warmth_cap": 0.12, "shadow_cap": 0.12},
    "dialogue": {"halflife_hours": 2.0, "warmth_cap": 0.18, "shadow_cap": 0.10},
    "soma": {"halflife_hours": 0.75, "warmth_cap": 0.16, "shadow_cap": 0.16},
    "thought": {"halflife_hours": 8.0, "warmth_cap": 0.12, "shadow_cap": 0.12},
    # Memory DP is a weather tint, not a durable feeling.  Keeping it in the
    # feel bucket made every analyzed memory refresh the same 72-hour residue.
    "dp_memory": {"halflife_hours": 12.0, "warmth_cap": 0.14, "shadow_cap": 0.14},
}
WEATHER_WARM_CHORDS = {"Dmaj7", "Amaj7", "Fmaj7", "Fmaj7#11", "Gmaj7"}
WEATHER_SHADOW_CHORDS = {"Dm7", "Em7", "F#dim", "Bm7b5"}
WEATHER_LIMINAL_CHORDS = {"C6", "Am7", "Gsus4"}
# Canonical chord vocabulary for tool schema / hold / drive echo.
# Order: liminal → warm → shadow (stable for enum display).
CHORD_KEYS = [
    "C6", "Am7", "Gsus4",
    "Dmaj7", "Amaj7", "Fmaj7", "Fmaj7#11", "Gmaj7",
    "Dm7", "Em7", "F#dim", "Bm7b5",
]
assert set(CHORD_KEYS) == (WEATHER_WARM_CHORDS | WEATHER_SHADOW_CHORDS | WEATHER_LIMINAL_CHORDS)
WEATHER_CHORD_DELTAS = {"soma": 0.08, "thought": 0.07}
WEATHER_CHORD_IMPULSE_STRENGTH = {"soma": 1.0, "thought": 0.72}
WEATHER_CHORD_IMPULSE_HALFLIFE_SEC = {"soma": 45 * 60, "thought": 4 * 3600}
WEATHER_ACTIVE_CHORD_THRESHOLD = 0.12
WEATHER_MAX_CHORD_IMPULSES = 16
WEATHER_SOOTHE_SHADOW_THRESHOLD = 0.08
WEATHER_RECENT_LOW_CHORD_SEC = 3 * 3600
WEATHER_SOOTHE_DURATION_SEC = 30 * 60
WEATHER_SOOTHE_SHADOW_HALFLIFE_HOURS = 0.75
WEATHER_SOOTHE_WARMTH_DELTA = 0.025
WEATHER_EVENT_SOURCE = "feel"
WEATHER_DIALOGUE_SOURCES = {"dialogue_residue", "speech_event", "user_message"}
WEATHER_NEGATIVE_CRYSTAL_DRIVES = {"possessiveness", "stress"}
WEATHER_CRYSTAL_MAX_ITEMS = 8
WEATHER_CRYSTAL_TIME_HALFLIFE_HOURS = 18.0

CHEMISTRY_ROUTE_KEYS = (
    "toward_human", "toward_house", "outward", "inward", "guard", "hover",
)

# Event force is a short signed overlay, not another slow state. Baseline
# percentile stretching waits for production samples; these anchors only mark
# an event's neutral/no-force point.
CORE_EVENT_NEUTRAL = {"charge": 0.34, "clutch": 0.32, "strain": 0.30}
CORE_EVENT_PULSE_SCALE = 0.88
CORE_EVENT_PULSE_CAP = 0.36
CORE_EVENT_PULSE_HALFLIFE_SEC = {
    "dialogue_residue": 35 * 60,
    "user_message": 35 * 60,
    "soma": 25 * 60,
    "thought": 90 * 60,
    "dp_memory": 4 * 3600,
    "analyze_nocturne_entry": 4 * 3600,
}
CORE_EVENT_PULSE_DEFAULT_HALFLIFE_SEC = 60 * 60

# ─── 悲恸引擎常量 ──────────────────────────────────────────────────────────────
# 三层：抗议→绝望→疏离
GRIEF_PROTEST_TICKS = 6            # 抗议层持续多少tick没有Human输入信号→跌绝望

# ─── Quiet hours: expected inactivity does not count as separation ───────────────────────────────────
# 这段时间里悲恸引擎冻结：不进层、不跌层、不计数。
# Quiet hours prevent absence from being misread as separation.
QUIET_HOURS = os.environ.get("OMBRE_QUIET_HOURS", "1-10")    # 起-止（24h制，可跨午夜）
QUIET_TZ = os.environ.get("OMBRE_QUIET_TZ", "UTC")

def is_quiet_hours(now_ts: float = None) -> bool:
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime
        start, end = (int(x) for x in QUIET_HOURS.split("-"))
        h = datetime.fromtimestamp(now_ts or time.time(), ZoneInfo(QUIET_TZ)).hour
        if start <= end:
            return start <= h < end
        return h >= start or h < end
    except Exception:
        return False
GRIEF_ATTACHMENT_SPIKE = 0.65      # attachment暴涨到这个值以上且无回应→进抗议层

# ─── 节律层常量 ───────────────────────────────────────────────────────────────
import math as _math

RHYTHM_SHORT_PERIOD = 86400.0      # 短周期：24小时（秒）
RHYTHM_LONG_PERIOD  = 259200.0     # 长周期：3天（秒）
RHYTHM_SHORT_AMP    = 0.55         # 短周期振幅权重
RHYTHM_LONG_AMP     = 0.45         # 长周期振幅权重
# fatigue对节律的压制系数：fatigue高→节律输出往偏重/想待着偏
RHYTHM_FATIGUE_DAMP = 0.6

# per-drive不应期（拍数）：attachment/libido是"软"维度，冷却短一点
REFRACTORY_TICKS: dict = {
    "attachment": 5,
    "libido":     4,
    "possessiveness": 5,
    "curiosity":  8,
    "reflection": 8,
    "stewardship": 8,
    "social":     8,
    "fatigue":    8,
    "stress":     7,
}
REFRACTORY_TICKS_DEFAULT = 8  # 未列出的维度用这个

# 心跳投递后的 pending pulse：不要求模型手动 settle/pass/break。
# 投递 = 挂号 + 短 refractory；真正的记忆写入动作 = engagement settle；超时 = soft pass。
PULSE_PENDING_TTL_SEC = 90 * 60
PULSE_DELIVERY_SOFT_BLEED = 0.94  # 说出口泄一点气，不是照顾完
PULSE_DELIVERY_REFRACTORY_TICKS = 3  # 比 full satisfy 短，只挡连砸

# ─── Drive Event v2：事件包 → 9维drive + discernment修正层 ───────────────────
DRIVE_EVENT_SCHEMA = "drive_event_v2"
DRIVE_EVENT_AGENCY_GATE = 0.35
DRIVE_EVENT_CONFIDENCE_FLOOR = 0.20
DRIVE_EVENT_SECONDARY_SCALE = 0.45
DRIVE_EVENT_PRIMARY_FOCUS_GAIN = 1.35
DRIVE_EVENT_PRIMARY_FEATURE_GAIN = 0.35
DRIVE_EVENT_SECONDARY_GATE = 0.25
DRIVE_EVENT_NONPRIMARY_FEATURE_GATE = 0.45
DRIVE_EVENT_NONPRIMARY_MIN_DELTA = 0.003
# Dialogue events are deliberately light; the generic gates added on 07-18
# erased every honest secondary. Keep one explicit DP-selected side current.
DIALOGUE_RESIDUE_SECONDARY_GATE = 0.15
DIALOGUE_RESIDUE_SECONDARY_MIN_DELTA = 0.0008
POSSESSIVENESS_TERRITORIAL_GATE = 0.55
HOUSE_COLLABORATOR_TERRITORIAL_SCALE = 0.45
DP_MEMORY_HOLD_SIGNAL_DISCOUNT = 0.55
DP_MEMORY_HOLD_SIGNAL_WINDOW_SEC = 6 * 3600

DRIVE_EVENT_BASE_DELTA = {
    "attachment": 0.22,
    "libido": 0.19,
    "possessiveness": 0.21,
    "reflection": 0.19,
    "stewardship": 0.19,
    "curiosity": 0.18,
    "social": 0.18,
    "fatigue": 0.18,
    "stress": 0.19,
}

DRIVE_EVENT_WEATHER_SOURCES = {
    "analyze_nocturne_entry",
    "dialogue_residue",
    "dp_memory",
    "legacy_feed",
    "speech_event",
    "user_message",
}

DRIVE_EVENT_SOURCE_WEIGHTS = {
    "user_message": 1.00,
    "speech_event": 0.90,
    "feel": 0.75,
    "memory": 0.55,
    "touch": 0.70,
    "external": 0.45,
    "analyze_nocturne_entry": 0.55,
    "dp_memory": 0.55,
    "dialogue_residue": 0.50,
    "legacy_feed": 0.60,
    "manual": 0.75,
}

DRIVE_EVENT_BRAIN_FEATURES = {
    "closeness_pull": ("attachment", 0.55, 0.0),
    "body_heat": ("libido", 0.52, 0.0),
    "territorial_alarm": ("possessiveness", 0.70, POSSESSIVENESS_TERRITORIAL_GATE),
    "inward_pull": ("reflection", 0.50, 0.0),
    "house_need": ("stewardship", 0.55, 0.0),
    "novelty_pull": ("curiosity", 0.48, 0.0),
    "expression_pressure": ("social", 0.50, 0.0),
    "energy_cost": ("fatigue", 0.50, 0.0),
    "tension_load": ("stress", 0.55, 0.0),
}


def territorial_delta_value(brain: dict | None) -> float:
    value = _feature_value(brain or {}, "territorial_alarm")
    context = str((brain or {}).get("third_party_context") or "").strip()
    territorial_event = str((brain or {}).get("territorial_event") or "").strip()
    if context == "house_collaborator" and not territorial_event:
        return value * HOUSE_COLLABORATOR_TERRITORIAL_SCALE
    return value


def territorial_event_spike_floor(brain: dict | None, intensity: float = 0.0,
                                  confidence: float = 0.0) -> float:
    brain = brain if isinstance(brain, dict) else {}
    event_kind = str(brain.get("territorial_event") or "").strip()
    if event_kind not in {"replacement", "third_party_insert", "boundary_touch", "comparison", "exclusion"}:
        return 0.0
    force = _clamp(max(float(intensity or 0.0), _feature_value(brain, "territorial_alarm")))
    trust = _clamp(float(confidence or 0.0), 0.2, 1.0)
    return round(_clamp(0.10 + 0.08 * force * trust, 0.10, 0.18), 6)


INTIMACY_INTERRUPT_CUES = (
    "转话题",
    "换话题",
    "逃开",
    "躲开",
    "断掉",
    "中断",
    "停住",
    "打断",
    "先不",
    "不要了",
    "算了",
    "收回",
    "避开",
)


def _event_text(event_label: str = "", evidence: list | None = None, brain: dict | None = None) -> str:
    brain = brain if isinstance(brain, dict) else {}
    parts = [str(event_label or "")]
    parts.extend(_as_text_list(evidence))
    for key in ("event_label", "intimacy_state", "transition", "reason", "dialogue_motion"):
        parts.append(str(brain.get(key) or ""))
    return "\n".join(part for part in parts if part)


def has_intimate_cue(source: str, primary: str, brain: dict | None) -> bool:
    brain = brain if isinstance(brain, dict) else {}
    return (
        primary == "libido"
        or source == "soma"
        or _feature_value(brain, "body_heat") >= 0.42
        or _feature_value(brain, "intimate_cue") >= 0.5
    )


def has_intimacy_interruption(event_label: str, evidence: list | None, brain: dict | None) -> bool:
    brain = brain if isinstance(brain, dict) else {}
    if max(
        _feature_value(brain, "intimacy_interrupted"),
        _feature_value(brain, "topic_shift"),
        _feature_value(brain, "turn_away"),
        _feature_value(brain, "escape"),
        _feature_value(brain, "avoidance"),
    ) >= 0.5:
        return True
    text = _event_text(event_label, evidence, brain)
    return any(cue in text for cue in INTIMACY_INTERRUPT_CUES)


def tick_libido_pending(pending: dict, now_ts: float) -> dict:
    pending = normalize_libido_pending(pending)
    last = pending["updated_at"] or now_ts
    elapsed = max(0.0, now_ts - last)
    if pending["level"] > 0:
        pending["level"] = round(
            _clamp(pending["level"] * _drive_decay_factor(elapsed, LIBIDO_PENDING_HALFLIFE_HOURS), 0.0, LIBIDO_PENDING_MAX),
            6,
        )
    if pending["armed"] and pending["last_cue_ts"] and now_ts - pending["last_cue_ts"] > LIBIDO_PENDING_ARM_WINDOW_SEC:
        pending["armed"] = False
    pending["updated_at"] = now_ts
    return pending


def arm_libido_pending(pending: dict, now_ts: float) -> dict:
    pending = tick_libido_pending(pending, now_ts)
    pending["armed"] = True
    pending["last_cue_ts"] = now_ts
    pending["updated_at"] = now_ts
    return pending


def apply_libido_interruption_pending(pending: dict, intensity: float,
                                      confidence: float, now_ts: float) -> tuple[dict, float]:
    pending = tick_libido_pending(pending, now_ts)
    if not pending["armed"]:
        return pending, 0.0
    lift = _clamp(0.06 + 0.06 * _clamp(intensity) * _clamp(confidence), LIBIDO_PENDING_MIN, LIBIDO_PENDING_MAX)
    pending["level"] = round(_clamp(pending["level"] + lift, 0.0, LIBIDO_PENDING_MAX), 6)
    pending["armed"] = False
    pending["updated_at"] = now_ts
    return pending, lift

ANCHOR_TARGETS = {"human", "house", "self", "boundary", "outside", "memory", "none"}
ANCHOR_TARGET_ALIASES = {
    "human": "human",
    "Human": "human",
    "she": "human",
    "her": "human",
    "house": "house",
    "house": "house",
    "memory system": "house",
    "self": "self",
    "agent": "self",
    "me": "self",
    "guard": "boundary",
    "territory": "boundary",
    "territorial": "boundary",
    "external": "outside",
    "outward": "outside",
    "world": "outside",
    "nocturne": "memory",
}

LEGACY_BRANCH_DRIVE = {
    "想靠近": "attachment",
    "沉默在一起": "attachment",
    "想说": "attachment",
    "被看见": "attachment",
    "占有": "possessiveness",
    "嫉妒": "possessiveness",
    "主动热": "libido",
    "看着": "libido",
    "被动dangerous": "libido",
    "控制": "libido",
    "想沉淀": "reflection",
    "想说出来": "reflection",
    "想被驳": "reflection",
    "自我质询": "reflection",
    "向外": "curiosity",
    "我是什么": "reflection",
    "我在生成什么": "reflection",
    "碰撞": "curiosity",
    "压着": "stress",
    "堵着": "stress",
    "悬着": "stress",
    "想看": "curiosity",
    "想接": "social",
    "想开": "social",
    "挂着": "stewardship",
    "记挂对方": "stewardship",
    "物理累": "fatigue",
    "信息满": "fatigue",
    "情绪累": "fatigue",
    "外部厌恶": "discernment",
    "内部皱眉": "discernment",
}

# 拒绝惩罚：同一个intent刚被拒绝过，下次pick_intent时有效分打折
REFUSAL_PENALTY = 0.15
REFUSAL_PENALTY_WINDOW_SEC = 600   # 10分钟内的拒绝记录有效
PASS_PENALTY = 0.06
PASS_PENALTY_WINDOW_SEC = 4 * 3600

def pulse_gain(current: float, base_delta: float) -> float:
    return base_delta * math.sqrt(max(0.0, 1.0 - current))


# ─── 数据类 ──────────────────────────────────────────────────────────────────

@dataclass
class DriveState:
    drives: dict = field(default_factory=lambda: dict(DRIVE_BASELINES))
    tick_count: int = 0
    last_ts: float = field(default_factory=time.time)
    prev_drives: dict = field(default_factory=lambda: dict(DRIVE_BASELINES))
    # per-drive局部疲劳：从全局fatigue按敏感度分配，影响有效分
    local_fatigue: dict = field(default_factory=lambda: {k: 0.0 for k in FATIGUE_SENSITIVITY})
    # 逃逸阀连续失衡计数（PDF阶段5.6），tick计数不是wall-clock
    escape_streak: int = 0
    # 最近一次真实用户消息；Stage 6 v1用attachment drive临时代替完整亲密/激情/承诺+依恋风格模型。
    last_user_message_at: float = field(default_factory=time.time)
    # 团圆PA上扬是一次性展示层事件，不写回drive baseline。
    reunion_pa_boost: float = 0.0
    possessiveness_channels: dict = field(default_factory=lambda: dict(POSSESSIVENESS_CHANNEL_DEFAULT))
    attachment_rebound: dict = field(default_factory=lambda: dict(ATTACHMENT_REBOUND_DEFAULT))
    libido_pending: dict = field(default_factory=lambda: dict(LIBIDO_PENDING_DEFAULT))


@dataclass
class Thought:
    tid: str
    text: str           # unsourced允许为空或"说不清楚"
    drive: str
    kind: str           # "flit" | "fixation" | "unsourced" | "rumination"
    strength: float
    born_at: float
    fed_count: int = 0
    # 念头来源："manual"=Agent亲手存 | "cli"/"analyze_nocturne_entry"=慢分析提取
    #          "echo"=旧念头回声 | "autofeed"=硬编码词池兜底 | "reflex"=条件反射
    source: str = "manual"
    source_bucket: str = ""
    source_type: str = ""
    source_created: str = ""
    last_ticked_at: float = 0.0      # 上次tick的时间戳，0表示用born_at


def _is_legacy_return_rumination(thought: Thought) -> bool:
    return (
        thought.kind == "rumination"
        and thought.drive == "attachment"
        and thought.source == "reflex"
        and str(thought.text or "").startswith(LEGACY_RETURN_RUMINATION_PREFIX)
    )


# ─── 悲恸引擎状态 ─────────────────────────────────────────────────────────────
# 三层吸引子：抗议 → 绝望 → 疏离
# 触发条件：attachment暴涨但Human不在（无输入信号）
# 跌层：抗议层持续GRIEF_PROTEST_TICKS个tick无回应 → 绝望
# 重置：Human回来有输入信号 → 直接出池回日常盆地
@dataclass
class GriefState:
    layer: str = "none"         # "none" | "protest" | "despair" | "detachment"
    protest_ticks: int = 0      # 抗议层已持续的tick数
    last_signal_ts: float = 0.0 # 最近一次Human输入信号的时间戳


# ─── 节律层状态 ───────────────────────────────────────────────────────────────
# 两个正弦叠加：短周期（日内）+ 长周期（数日）
# 输出四态：偏重 / 偏轻 / 话多 / 想待着
# Human不来也在走，不被drive驱动，只被time和对话密度微调相位
@dataclass
class RhythmState:
    short_phase: float = 0.0    # 短周期相位（弧度），每tick按时间推进
    long_phase: float = 0.0     # 长周期相位（弧度）
    phase_offset: float = 0.0   # 对话密度修正的相位偏移量（缓慢漂移）
    last_ts: float = field(default_factory=time.time)

    def current_value(self, fatigue: float = 0.0) -> float:
        """
        计算当前节律值 [-1, 1]。
        正值=活跃/话多，负值=沉/想待着。
        fatigue高时整体往负值压。
        """
        short = _math.sin(self.short_phase + self.phase_offset) * RHYTHM_SHORT_AMP
        long  = _math.sin(self.long_phase) * RHYTHM_LONG_AMP
        raw = short + long                          # [-1, 1]
        # fatigue压制：fatigue越高，往负值偏
        damp = fatigue * RHYTHM_FATIGUE_DAMP
        return max(-1.0, min(1.0, raw - damp))

    def label(self, fatigue: float = 0.0) -> str:
        """输出四态标签"""
        v = self.current_value(fatigue)
        if v >= 0.4:
            return "话多"
        elif v >= 0.05:
            return "偏轻"
        elif v >= -0.35:
            return "想待着"
        else:
            return "偏重"


# ─── 引擎核心（纯函数部分）──────────────────────────────────────────────────

def compute_local_fatigue(global_fatigue: float) -> dict:
    """根据全局fatigue计算每个维度的局部疲劳值"""
    return {
        k: _clamp(global_fatigue * s)
        for k, s in FATIGUE_SENSITIVITY.items()
    }


def effective_score(drive_val: float, local_fat: float) -> float:
    """有效分 = drive值 × (1 - 局部疲劳)"""
    return drive_val * (1.0 - local_fat)


def drive_activation(drive_key: str, drive_val: float) -> float:
    """Normalize pressure above baseline with a perceptual sqrt response.

    Linear headroom made even decisive events look permanently half-asleep.
    Sqrt keeps baseline at zero and full saturation at one while separating
    ordinary (roughly 0.15-0.40) from decisive (0.50+) activation.
    """
    drive_key = normalize_drive_key(drive_key, drive_key)
    baseline = DRIVE_BASELINES.get(drive_key, 0.0)
    headroom = max(1e-9, 1.0 - baseline)
    linear = _clamp((float(drive_val or 0.0) - baseline) / headroom)
    return math.sqrt(linear)


def effective_drive_activation(drive_key: str, drive_val: float, local_fat: float) -> float:
    """Fatigue suppresses active pressure, never the Drive's resting baseline."""
    return _clamp(drive_activation(drive_key, drive_val) * (1.0 - _clamp(local_fat)))


def drive_activation_snapshot(drives: dict, local_fatigue: dict | None = None) -> dict:
    drives = normalize_drive_values(drives)
    fatigue = local_fatigue or {}
    return {
        key: round(
            effective_drive_activation(key, drives[key], float(fatigue.get(key, 0.0) or 0.0)),
            3,
        )
        for key in DRIVE_KEYS
    }


def effective_drive_snapshot(drives: dict, local_fatigue: dict) -> dict:
    drives = normalize_drive_values(drives)
    local_fatigue = {k: float((local_fatigue or {}).get(k, 0.0) or 0.0) for k in FATIGUE_SENSITIVITY}
    return {
        k: round(effective_score(drives.get(k, DRIVE_BASELINES[k]), local_fatigue.get(k, 0.0)), 3)
        for k in DRIVE_KEYS
    }


def _group_excess(drives: dict, group: list) -> float:
    """某分组里，超出各自baseline的部分的平均值（不超出的算0）。"""
    vals = [max(0.0, drives.get(k, DRIVE_BASELINES[k]) - DRIVE_BASELINES[k]) for k in group]
    return sum(vals) / len(vals) if vals else 0.0


def apply_esm_inhibition(drives: dict) -> dict:
    """
    ESM软互抑（PDF阶段5.7，k=0.3）。
    只压"超出baseline的部分"，不动baseline本身——
    "甜蜜又心疼"：两组都在，互相压一点，不是清零，也不会把drive压到baseline以下。
    互抑前的pos_excess用于压负向组，避免顺序依赖（跟PDF原版pa_before一致）。
    """
    import copy
    drives = normalize_drive_values(drives)
    new_drives = copy.copy(drives)
    pos_excess = _group_excess(drives, POSITIVE_GROUP)
    neg_excess = _group_excess(drives, NEGATIVE_GROUP)
    for k in POSITIVE_GROUP:
        excess = drives[k] - DRIVE_BASELINES[k]
        if excess > 0:
            new_drives[k] = _clamp(DRIVE_BASELINES[k] + excess * (1 - ESM_K * neg_excess))
    for k in NEGATIVE_GROUP:
        excess = drives[k] - DRIVE_BASELINES[k]
        if excess > 0:
            new_drives[k] = _clamp(DRIVE_BASELINES[k] + excess * (1 - ESM_K * pos_excess))
    return new_drives


def apply_escape_valve(drives: dict, streak: int) -> tuple:
    """
    逃逸阀（PDF阶段5.6，红线条款）。
    连续ESCAPE_VALVE_STREAK_TRIGGER拍出现"负向组明显高于正向组"→
    强制把负向组超出baseline的部分拉回ESCAPE_VALVE_PULLBACK（默认50%）。
    用streak计数（非单次判断），防止单次评分误判就触发；触发后streak清零重新计。
    """
    import copy
    drives = normalize_drive_values(drives)
    pos_excess = _group_excess(drives, POSITIVE_GROUP)
    neg_excess = _group_excess(drives, NEGATIVE_GROUP)

    if neg_excess - pos_excess > ESCAPE_VALVE_EXCESS_GAP:
        streak += 1
    else:
        streak = 0

    new_drives = copy.copy(drives)
    if streak >= ESCAPE_VALVE_STREAK_TRIGGER:
        for k in NEGATIVE_GROUP:
            excess = drives[k] - DRIVE_BASELINES[k]
            if excess > 0:
                new_drives[k] = _clamp(DRIVE_BASELINES[k] + excess * (1 - ESCAPE_VALVE_PULLBACK))
        streak = 0

    return new_drives, streak


def pa_na_snapshot(drives: dict) -> dict:
    """
    PA/NA展示层——不持久化，每次从当前9维drive实时算一个坐标给前端看。
    PA=正向组均值，NA=负向组均值，两者都是[0,1]。
    """
    drives = normalize_drive_values(drives)
    pos_vals = [drives[k] for k in POSITIVE_GROUP]
    neg_vals = [drives[k] for k in NEGATIVE_GROUP]
    pa = sum(pos_vals) / len(pos_vals) if pos_vals else 0.0
    na = sum(neg_vals) / len(neg_vals) if neg_vals else 0.0
    return {"PA": round(pa, 3), "NA": round(na, 3)}


def _weather_default_state(now: float = None) -> dict:
    now = now if now is not None else time.time()
    return {
        "warmth_residue": 0.0,
        "shadow_residue": 0.0,
        "updated_at": now,
        "components": {
            name: {"warmth": 0.0, "shadow": 0.0, "updated_at": now}
            for name in WEATHER_COMPONENTS
        },
        "last_low_chord_at": 0.0,
        "soothe_until": 0.0,
        "active_chord": "",
        "active_chord_source": "",
        "active_chord_at": 0.0,
        "chord_impulses": [],
        "shadow_crystals": [],
    }


def _normalize_chord(chord: str) -> str:
    token = (chord or "").strip().split()[0] if chord else ""
    aliases = {
        "fmaj7": "Fmaj7",
        "fmaj7#11": "Fmaj7#11",
        "fmaj7♯11": "Fmaj7#11",
        "gmaj7": "Gmaj7",
        "dmaj7": "Dmaj7",
        "amaj7": "Amaj7",
        "dm7": "Dm7",
        "em7": "Em7",
        "f#dim": "F#dim",
        "f♯dim": "F#dim",
        "bm7b5": "Bm7b5",
        "bø7": "Bm7b5",
        "c6": "C6",
        "am7": "Am7",
        "gsus4": "Gsus4",
    }
    return aliases.get(token.lower(), token)


def _crystal_anchor(event_label: str = "", brain: dict | None = None,
                    evidence: list | None = None, primary: str = "") -> str:
    brain = brain if isinstance(brain, dict) else {}
    parts = [
        primary,
        str(brain.get("anchor_target") or ""),
        str(brain.get("target") or ""),
        str(event_label or ""),
    ]
    parts.extend(str(x or "") for x in (evidence or [])[:2])
    raw = "|".join(x.strip().lower() for x in parts if str(x or "").strip())
    if not raw:
        raw = primary or "shadow"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _crystal_actor_weight(event: dict | None) -> float:
    """Who touched the crystal matters: Human's words hit harder than ambient mentions."""
    event = event if isinstance(event, dict) else {}
    source = str(event.get("source") or "").strip()
    brain = event.get("brain") if isinstance(event.get("brain"), dict) else {}
    actor = str(
        event.get("actor")
        or event.get("speaker")
        or brain.get("actor")
        or brain.get("speaker")
        or brain.get("source_actor")
        or ""
    ).strip().lower()
    messages = event.get("messages") if isinstance(event.get("messages"), list) else []
    latest_role = ""
    for item in reversed(messages):
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or item.get("content") or "").strip()
        role = str(item.get("role") or "").strip().lower()
        if text and role:
            latest_role = role
            break

    if source == "user_message" or actor in {"human", "Human", "user"}:
        return 2.0
    if source in WEATHER_DIALOGUE_SOURCES:
        if latest_role == "user":
            return 1.85
        if latest_role == "assistant":
            return 1.15
        return 1.45
    if source in {"external", "memory", "analyze_nocturne_entry", "dp_memory", "legacy_feed"}:
        return 0.62
    return 1.0


def _normalize_shadow_crystals(items: list | None, now: float = None,
                               decay_time: bool = True) -> list:
    now = now if now is not None else time.time()
    normalized = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or "").strip()
        if kind not in WEATHER_NEGATIVE_CRYSTAL_DRIVES:
            continue
        touched_at = float(item.get("last_touched_at", item.get("created_at", now)) or now)
        elapsed = max(0.0, now - touched_at)
        time_factor = (
            _drive_decay_factor(elapsed, WEATHER_CRYSTAL_TIME_HALFLIFE_HOURS)
            if decay_time else 1.0
        )
        heat = _clamp(float(item.get("heat", 0.0) or 0.0) * time_factor)
        hardness = _clamp(float(item.get("hardness", 0.0) or 0.0) * max(time_factor, 0.72))
        if heat < 0.008 and hardness < 0.035:
            continue
        normalized.append({
            "id": str(item.get("id") or _crystal_anchor(primary=kind)),
            "kind": kind,
            "anchor": _as_text_list(item.get("anchor"))[:6],
            "heat": round(heat, 4),
            "hardness": round(hardness, 4),
            "actor_weight": round(_clamp(item.get("actor_weight", 1.0), 0.0, 2.0), 3),
            "foreground": bool(heat >= 0.075),
            "event_label": str(item.get("event_label") or "").strip(),
            "created_at": float(item.get("created_at", touched_at) or touched_at),
            "last_touched_at": touched_at,
        })
    normalized.sort(key=lambda x: (x["heat"] + x["hardness"] * 0.45, x["last_touched_at"]), reverse=True)
    return normalized[:WEATHER_CRYSTAL_MAX_ITEMS]


def _shadow_crystal_readout(crystals: list | None) -> dict:
    items = _normalize_shadow_crystals(crystals, decay_time=False)
    if not items:
        return {"shadow": 0.0, "active": None, "items": []}
    active = items[0]
    # Crystals add a small, slowly decaying NA residue only.
    shadow = _clamp(sum(x["heat"] * 0.32 + x["hardness"] * 0.10 for x in items), 0.0, 0.08)
    return {
        "shadow": round(shadow, 4),
        "active": {
            "kind": active["kind"],
            "heat": round(active["heat"], 3),
            "hardness": round(active["hardness"], 3),
            "actor_weight": round(active.get("actor_weight", 1.0), 3),
            "foreground": active["foreground"],
            "event_label": active.get("event_label", ""),
        },
        "items": items[:3],
    }


def weather_chord_kind(chord: str) -> Optional[str]:
    normalized = _normalize_chord(chord)
    if normalized in WEATHER_WARM_CHORDS:
        return "warmth"
    if normalized in WEATHER_SHADOW_CHORDS:
        return "shadow"
    if normalized in WEATHER_LIMINAL_CHORDS:
        return "liminal"
    return None


def current_weather_chord(warmth: float, shadow: float) -> str:
    """Snapshot chord from current effective Warmth/Shadow."""
    warmth = _clamp(float(warmth or 0.0))
    shadow = _clamp(float(shadow or 0.0))
    if warmth < 0.28 and shadow < 0.28:
        return "C6"
    if shadow >= 0.62 and warmth < 0.35:
        return "F#dim"
    if warmth >= 0.68 and shadow < 0.28:
        return "Dmaj7"
    if warmth >= 0.6 and shadow < 0.22:
        return "Amaj7"
    if warmth >= 0.58 and shadow >= 0.38:
        return "Fmaj7#11"
    if abs(warmth - shadow) <= 0.08 and max(warmth, shadow) >= 0.38:
        return "Am7"
    if shadow >= 0.32 and warmth < 0.36 and shadow - warmth <= 0.14:
        return "Gsus4"
    if shadow > warmth:
        return "Dm7" if shadow >= 0.55 else "Em7"
    if warmth >= 0.52:
        return "Fmaj7"
    if warmth >= 0.36 or shadow >= 0.32:
        return "Gmaj7" if warmth >= shadow else "Gsus4"
    return "C6"



def _chemistry_band(value: float) -> str:
    if value < 0.35:
        return "low"
    if value > 0.65:
        return "high"
    return "mid"


def classify_chord_situation(core: dict, route: dict, derived: dict | None = None) -> str:
    charge = _clamp(float((core or {}).get("charge", 0.0) or 0.0))
    clutch = _clamp(float((core or {}).get("clutch", 0.0) or 0.0))
    strain = _clamp(float((core or {}).get("strain", 0.0) or 0.0))
    vector = str((route or {}).get("vector") or "hover")
    derived = derived if isinstance(derived, dict) else {}
    pull = _clamp(float(derived.get("pull", 0.0) or 0.0))
    depth = _clamp(float(derived.get("depth", 0.0) or 0.0))
    drift = _clamp(float(derived.get("drift", 0.0) or 0.0))

    ch = _chemistry_band(charge)
    cl = _chemistry_band(clutch)
    st = _chemistry_band(strain)

    if ch == "high" and cl == "high" and st == "high":
        return "overload"
    if vector == "guard" and clutch >= 0.50:
        return "guard"
    if ch == "high" and cl == "high" and st != "high":
        return "live_wire"
    if ch == "high" and st == "high" and cl != "high":
        return "static"
    if cl == "high" and st == "high" and ch != "high":
        return "clamp"
    if (
        vector in {"toward_human", "toward_house"}
        and clutch >= 0.50
        and pull > depth
        and pull > drift
        and strain < 0.65
    ):
        return "pull"
    if vector == "inward" and strain >= 0.50 and charge <= 0.65:
        return "sink"
    if cl == "high" and st != "high" and ch != "high":
        return "grip"
    if st == "high" and ch != "high" and cl != "high":
        return "taut"
    if vector == "outward" and (
        charge >= 0.50
        or (str((route or {}).get("event_vector") or "") == "outward" and charge >= 0.46)
    ):
        return "scout"
    if ch == "high" and vector != "outward" and cl != "high" and st != "high":
        return "spark"
    return "drift"


def chord_event_tint_from_drive_events(events: list | None) -> dict:
    if not isinstance(events, list):
        return {}
    event = None
    for item in events:
        if not isinstance(item, dict) or item.get("suppressed"):
            continue
        if str(item.get("source") or "").strip() == "speech_event":
            continue
        brain = item.get("brain")
        if isinstance(brain, dict) and brain:
            event = item
            break
    if not event:
        return {}

    brain = normalize_drive_event_brain(event.get("brain"))
    release = _feature_value(brain, "release_pressure")
    closeness = _feature_value(brain, "closeness_pull")
    territorial = _feature_value(brain, "territorial_alarm")
    inward = _feature_value(brain, "inward_pull")
    house = _feature_value(brain, "house_need")
    novelty = _feature_value(brain, "novelty_pull")
    expression = _feature_value(brain, "expression_pressure")
    energy = _feature_value(brain, "energy_cost")
    tension = _feature_value(brain, "tension_load")
    discernment = _feature_value(brain, "discernment_alarm")
    heat = _feature_value(brain, "body_heat")
    anchor = normalize_anchor_target(brain.get("anchor_target"), brain.get("target"))
    grounding = str(brain.get("grounding") or "").strip()

    blocked_release = release * max(tension, discernment)
    core = {
        "charge": round(_clamp(
            0.16 + 0.36 * release + 0.22 * novelty + 0.18 * expression + 0.12 * heat - 0.22 * energy
        ), 3),
        "clutch": round(_clamp(
            0.12 + 0.34 * territorial + 0.20 * closeness + 0.16 * house
            + (0.16 if anchor in {"human", "house", "boundary"} else 0.0)
        ), 3),
        "strain": round(_clamp(
            0.10 + 0.34 * tension + 0.24 * discernment + 0.18 * blocked_release
            + (0.08 if grounding == "悬" else 0.0) + (0.12 if grounding == "空" else 0.0)
        ), 3),
    }
    route_scores = {
        "toward_human": _clamp((0.75 if anchor == "human" else 0.0) + 0.22 * closeness),
        "toward_house": _clamp((0.72 if anchor == "house" else 0.0) + 0.30 * house),
        "outward": _clamp((0.72 if anchor == "outside" else 0.0) + 0.28 * novelty + 0.14 * release),
        "inward": _clamp((0.68 if anchor in {"self", "memory"} else 0.0) + 0.28 * inward),
        "guard": _clamp((0.76 if anchor == "boundary" else 0.0) + 0.28 * territorial + 0.18 * house),
        "hover": _clamp((0.68 if anchor == "none" else 0.0) + 0.16 * (1.0 - release)),
    }
    vector = max(route_scores, key=route_scores.get)
    return {
        "core": core,
        "route": {
            "vector": vector,
            "scores": {k: round(v, 3) for k, v in route_scores.items()},
        },
        "source": event.get("source", ""),
        "event_label": event.get("event_label", ""),
        "ledger_id": event.get("id"),
        "created_at": float(event.get("ts") or time.time()),
        "brain": {
            "release_pressure": round(release, 3),
            "anchor_target": anchor,
        },
    }


def chord_chemistry_snapshot(drives: dict, warmth: float = 0.0, shadow: float = 0.0,
                             legacy_context: list | None = None, now: float = None,
                             event_tint: dict | None = None) -> dict:
    """
    Chord Chemistry v1.0.
    Core is continuous force; route is directional. Derived texture is readout only.
    """
    d = normalize_drive_values(drives)
    attachment = d["attachment"]
    libido = d["libido"]
    possessiveness = d["possessiveness"]
    reflection = d["reflection"]
    stewardship = d["stewardship"]
    curiosity = d["curiosity"]
    social = d["social"]
    fatigue = d["fatigue"]
    stress = d["stress"]
    warmth = _clamp(float(warmth or 0.0))
    shadow = _clamp(float(shadow or 0.0))

    stress_charge = stress * (1.0 - max(0.0, stress - 0.62) * 1.15)
    release_pair = max(curiosity, social) * (0.55 + 0.45 * max(libido, possessiveness))
    charge = _clamp(
        0.18
        + 0.34 * max(curiosity, social)
        + 0.22 * libido
        + 0.18 * release_pair
        + 0.16 * stress_charge
        + 0.08 * warmth
        - 0.42 * fatigue
    )

    attachment_lock = attachment * (0.45 + 0.55 * max(possessiveness, stewardship))
    boundary_friction = max(0.0, stress - 0.30) * attachment
    clutch = _clamp(
        0.10
        + 0.30 * attachment
        + 0.26 * possessiveness
        + 0.22 * stewardship
        + 0.32 * attachment_lock
        + 0.26 * boundary_friction
    )

    unresolved_pair = max(stress, fatigue) * (0.40 + 0.60 * max(reflection, attachment))
    compressed_charge = max(0.0, charge - 0.52) * max(0.0, stress - 0.36)
    strain = _clamp(
        0.08
        + 0.32 * stress
        + 0.24 * fatigue
        + 0.20 * reflection
        + 0.25 * unresolved_pair
        + 0.18 * compressed_charge
        + 0.22 * shadow
    )

    route_scores = {
        "toward_human": _clamp(0.48 * attachment + 0.24 * libido + 0.16 * possessiveness + 0.12 * warmth),
        "toward_house": _clamp(0.46 * stewardship + 0.26 * attachment + 0.14 * reflection + 0.14 * clutch),
        "outward": _clamp(0.45 * curiosity + 0.33 * social + 0.15 * charge - 0.18 * strain),
        "inward": _clamp(0.46 * reflection + 0.23 * fatigue + 0.18 * strain + 0.13 * attachment),
        "guard": _clamp(0.38 * stewardship + 0.27 * possessiveness + 0.22 * stress + 0.13 * clutch),
        "hover": _clamp(0.34 * (1.0 - charge) + 0.24 * (1.0 - clutch) + 0.24 * (1.0 - strain) + 0.18 * fatigue),
    }
    vector = max(route_scores, key=route_scores.get)
    if max(charge, clutch, strain) < 0.30:
        vector = "hover"

    depth = _clamp(strain * (0.70 if vector == "inward" else 0.44) + reflection * 0.24 - charge * 0.10)
    pull = _clamp(
        (0.62 * clutch + 0.38 * attachment)
        if vector in {"toward_human", "toward_house"}
        else 0.45 * clutch * attachment
    )
    guard = _clamp(
        (0.58 * clutch + 0.42 * stewardship)
        if vector == "guard"
        else 0.34 * clutch * stewardship
    )
    spark = _clamp(charge * (1.0 - strain * 0.45) * (1.0 - fatigue * 0.35))
    drift = _clamp((1.0 - charge) * (1.0 - clutch) * (1.0 - strain) + (0.18 if vector == "hover" else 0.0))
    derived = {
        "depth": round(depth, 3),
        "pull": round(pull, 3),
        "guard": round(guard, 3),
        "spark": round(spark, 3),
        "drift": round(drift, 3),
    }

    baseline_core = {
        "charge": round(charge, 3),
        "clutch": round(clutch, 3),
        "strain": round(strain, 3),
    }
    baseline_route = {
        "vector": vector,
        "scores": {k: round(v, 3) for k, v in route_scores.items()},
    }
    core = dict(baseline_core)
    route = dict(baseline_route)
    event_tint = event_tint if isinstance(event_tint, dict) else {}
    event_core = event_tint.get("core") if isinstance(event_tint.get("core"), dict) else {}
    event_route = event_tint.get("route") if isinstance(event_tint.get("route"), dict) else {}
    event_pulse = {key: 0.0 for key in ("charge", "clutch", "strain")}
    pulse_factor = 0.0
    if event_core:
        source = str(event_tint.get("source") or "").strip()
        created_at = float(event_tint.get("created_at") or now or time.time())
        pulse_now = float(now if now is not None else time.time())
        age = max(0.0, pulse_now - created_at)
        half_life = CORE_EVENT_PULSE_HALFLIFE_SEC.get(source, CORE_EVENT_PULSE_DEFAULT_HALFLIFE_SEC)
        pulse_factor = _time_decay_factor(age, half_life / 3600.0)
        for key in ("charge", "clutch", "strain"):
            baseline_value = baseline_core.get(key, 0.0)
            event_value = float(event_core.get(key, CORE_EVENT_NEUTRAL[key]) or 0.0)
            pulse = _clamp(
                (event_value - CORE_EVENT_NEUTRAL[key]) * CORE_EVENT_PULSE_SCALE * pulse_factor,
                -CORE_EVENT_PULSE_CAP,
                CORE_EVENT_PULSE_CAP,
            )
            event_pulse[key] = round(pulse, 3)
            core[key] = round(_clamp(baseline_value + pulse), 3)
    if event_route:
        event_vector = str(event_route.get("vector") or "").strip()
        event_scores = event_route.get("scores") if isinstance(event_route.get("scores"), dict) else {}
        score_values = [float(v or 0.0) for v in event_scores.values()] if event_scores else []
        if score_values:
            route_weight = 0.55
            merged_scores = {}
            for key in CHEMISTRY_ROUTE_KEYS:
                baseline_value = float(baseline_route["scores"].get(key, 0.0) or 0.0)
                event_value = float(event_scores.get(key, 0.0) or 0.0)
                merged_scores[key] = round(_clamp(
                    baseline_value * (1.0 - route_weight) + event_value * route_weight
                ), 3)
            route["scores"] = merged_scores
            route["vector"] = max(merged_scores, key=merged_scores.get)
        if event_vector and event_vector != "hover" and max(score_values or [0.0]) >= 0.52:
            route["vector"] = event_vector
        route["event_vector"] = event_vector
        route["baseline_vector"] = baseline_route["vector"]
    charge = core["charge"]
    clutch = core["clutch"]
    strain = core["strain"]
    vector = route["vector"]
    depth = _clamp(strain * (0.70 if vector == "inward" else 0.44) + reflection * 0.24 - charge * 0.10)
    pull = _clamp(
        (0.62 * clutch + 0.38 * attachment)
        if vector in {"toward_human", "toward_house"}
        else 0.45 * clutch * attachment
    )
    guard = _clamp(
        (0.58 * clutch + 0.42 * stewardship)
        if vector == "guard"
        else 0.34 * clutch * stewardship
    )
    spark = _clamp(charge * (1.0 - strain * 0.45) * (1.0 - fatigue * 0.35))
    drift = _clamp((1.0 - charge) * (1.0 - clutch) * (1.0 - strain) + (0.18 if vector == "hover" else 0.0))
    derived = {
        "depth": round(depth, 3),
        "pull": round(pull, 3),
        "guard": round(guard, 3),
        "spark": round(spark, 3),
        "drift": round(drift, 3),
    }
    situation = classify_chord_situation(core, route, derived)

    return {
        "core": core,
        "route": route,
        "readout": {"warmth": round(warmth, 3), "shadow": round(shadow, 3)},
        "situation": situation,
        "baseline": {"core": baseline_core, "route": baseline_route},
        "baseline_core_raw": baseline_core,
        "event_pulse": {
            "core": event_pulse,
            "factor": round(pulse_factor, 3),
            "source": event_tint.get("source", ""),
            "created_at": event_tint.get("created_at"),
        },
        "event_tint": event_tint,
        "derived_texture": derived,
        "derived": derived,
    }


def _route_scores(route: dict | None) -> dict:
    route = route if isinstance(route, dict) else {}
    raw_scores = route.get("scores") if isinstance(route.get("scores"), dict) else {}
    scores = {key: _clamp(float(raw_scores.get(key, 0.0) or 0.0)) for key in CHEMISTRY_ROUTE_KEYS}
    # `vector` is the label of the current winner, not additional evidence.
    # Raising that score here used to turn a narrow lead into a categorical
    # 0.72 route and made Chemistry more certain than its evidence.
    if not any(scores.values()):
        scores["hover"] = 0.72
    return scores


def _chord_impulse_weight(impulse: dict, now: float) -> float:
    try:
        strength = max(0.0, float(impulse.get("strength", 0.0) or 0.0))
        created_at = float(impulse.get("created_at", 0.0) or 0.0)
        half_life = max(1.0, float(impulse.get("half_life", 0.0) or 0.0))
    except (TypeError, ValueError):
        return 0.0
    age = max(0.0, now - created_at)
    return strength * (0.5 ** (age / half_life))


def _normalize_chord_impulses(state: dict, now: float) -> list:
    impulses = []
    raw_items = state.get("chord_impulses")
    if isinstance(raw_items, list):
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            chord = _normalize_chord(item.get("chord", ""))
            source = str(item.get("source") or "").strip()
            if not chord or not source:
                continue
            try:
                strength = max(0.0, float(item.get("strength", 0.0) or 0.0))
                half_life = max(1.0, float(item.get("half_life", 0.0) or 0.0))
                created_at = float(item.get("created_at", now) or now)
            except (TypeError, ValueError):
                continue
            impulse = {
                "chord": chord,
                "source": source,
                "strength": round(strength, 6),
                "half_life": round(half_life, 3),
                "created_at": created_at,
            }
            if _chord_impulse_weight(impulse, now) >= WEATHER_ACTIVE_CHORD_THRESHOLD / 4:
                impulses.append(impulse)

    if not impulses:
        chord = _normalize_chord(state.get("active_chord", ""))
        source = str(state.get("active_chord_source") or "").strip()
        created_at = float(state.get("active_chord_at", 0.0) or 0.0)
        if chord and source and created_at:
            impulse = {
                "chord": chord,
                "source": source,
                "strength": WEATHER_CHORD_IMPULSE_STRENGTH.get(source, 0.4),
                "half_life": WEATHER_CHORD_IMPULSE_HALFLIFE_SEC.get(source, 3600),
                "created_at": created_at,
            }
            if _chord_impulse_weight(impulse, now) >= WEATHER_ACTIVE_CHORD_THRESHOLD / 4:
                impulses.append(impulse)

    impulses.sort(key=lambda item: _chord_impulse_weight(item, now), reverse=True)
    return impulses[:WEATHER_MAX_CHORD_IMPULSES]


def _active_weather_chord(state: dict, now: float = None) -> dict:
    now = now if now is not None else time.time()
    impulses = _normalize_chord_impulses(state, now)
    if not impulses:
        return {
            "active_chord": "",
            "active_chord_source": "",
            "active_chord_age_sec": None,
            "active_chord_weight": 0.0,
            "source_stack": [],
        }
    active = impulses[0]
    weight = _chord_impulse_weight(active, now)
    if weight < WEATHER_ACTIVE_CHORD_THRESHOLD:
        return {
            "active_chord": "",
            "active_chord_source": "",
            "active_chord_age_sec": None,
            "active_chord_weight": round(weight, 3),
            "source_stack": [],
        }
    return {
        "active_chord": active["chord"],
        "active_chord_source": active["source"],
        "active_chord_age_sec": round(max(0.0, now - active["created_at"]), 3),
        "active_chord_weight": round(weight, 3),
        "source_stack": [
            {
                "chord": item["chord"],
                "source": item["source"],
                "weight": round(_chord_impulse_weight(item, now), 3),
            }
            for item in impulses[:5]
        ],
    }


def _decay_weather_value(value: float, elapsed: float, halflife_hours: float,
                         soothe_elapsed: float = 0.0) -> float:
    value = max(0.0, float(value or 0.0))
    if value <= 0.0:
        return 0.0
    normal_elapsed = max(0.0, elapsed - max(0.0, soothe_elapsed))
    factor = _time_decay_factor(normal_elapsed, halflife_hours)
    if soothe_elapsed > 0:
        factor *= _time_decay_factor(soothe_elapsed, WEATHER_SOOTHE_SHADOW_HALFLIFE_HOURS)
    return value * factor


class WeatherResidueStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def _read_raw(self) -> dict:
        try:
            with self.path.open(encoding="utf-8") as f:
                raw = json.load(f)
            return raw if isinstance(raw, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    def _write_raw(self, state: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        tmp.replace(self.path)

    def load(self, now: float = None, decay: bool = True) -> dict:
        now = now if now is not None else time.time()
        raw = self._read_raw()
        state = _weather_default_state(now)
        state.update({k: v for k, v in raw.items() if k not in ("components",)})
        raw_components = raw.get("components") if isinstance(raw.get("components"), dict) else {}
        for name in WEATHER_COMPONENTS:
            component = raw_components.get(name, {})
            state["components"][name] = {
                "warmth": float(component.get("warmth", 0.0) or 0.0),
                "shadow": float(component.get("shadow", 0.0) or 0.0),
                "updated_at": float(component.get("updated_at", raw.get("updated_at", now)) or now),
            }
        state["chord_impulses"] = _normalize_chord_impulses(state, now)
        state["shadow_crystals"] = _normalize_shadow_crystals(raw.get("shadow_crystals"), now)

        if decay:
            state = self._decay(state, now)
            self._write_raw(state)
        else:
            state = self._refresh_totals(state, float(state.get("updated_at", now) or now))
        return state

    def save(self, state: dict) -> dict:
        refreshed = self._refresh_totals(state, time.time())
        self._write_raw(refreshed)
        return refreshed

    def touch_shadow_crystals(self, event: dict, now: float = None) -> dict:
        """Event-based negative residue: heat recedes by turns; hardness remains as ledger."""
        now = now if now is not None else time.time()
        event = event if isinstance(event, dict) else {}
        primary = normalize_drive_key(event.get("primary_drive"))
        source = str(event.get("source") or "").strip()
        brain = normalize_drive_event_brain(event.get("brain"))
        evidence = _as_text_list(event.get("evidence"))
        event_label = str(event.get("event_label") or "").strip()
        intensity = _clamp(event.get("intensity", 0.0))
        confidence = _clamp(event.get("confidence", 0.0))
        actor_weight = _crystal_actor_weight(event)
        suppressed = bool(event.get("suppressed", False))
        state = self.load(now, decay=True)
        crystals = _normalize_shadow_crystals(state.get("shadow_crystals"), now, decay_time=False)

        # One dialogue turn cools foreground heat. Hardness only sands down slowly.
        for crystal in crystals:
            crystal["heat"] = round(_clamp(crystal["heat"] * 0.72), 4)
            crystal["hardness"] = round(_clamp(crystal["hardness"] * 0.985), 4)
            crystal["foreground"] = crystal["heat"] >= 0.075

        is_negative = (
            not suppressed
            and (source in DRIVE_EVENT_WEATHER_SOURCES or source in {"user_message", "external", "memory"})
            and (
                primary in WEATHER_NEGATIVE_CRYSTAL_DRIVES
                or _feature_value(brain, "territorial_alarm") >= POSSESSIVENESS_TERRITORIAL_GATE
                or _feature_value(brain, "tension_load") >= 0.45
                or _feature_value(brain, "discernment_alarm") >= 0.45
            )
        )
        if is_negative:
            kind = "possessiveness" if (
                primary == "possessiveness"
                or _feature_value(brain, "territorial_alarm") >= POSSESSIVENESS_TERRITORIAL_GATE
            ) else "stress"
            anchor = _crystal_anchor(event_label, brain, evidence, kind)
            heat_lift = _clamp(
                (
                    0.055
                    + 0.155 * intensity * confidence
                    + 0.075 * max(_feature_value(brain, "territorial_alarm"), _feature_value(brain, "tension_load"))
                ) * actor_weight,
                0.0,
                0.28,
            )
            hardness_lift = _clamp(
                (
                    0.050
                    + 0.115 * intensity * confidence
                    + 0.060 * max(_feature_value(brain, "territorial_alarm"), _feature_value(brain, "discernment_alarm"))
                ) * (0.72 + 0.28 * actor_weight),
                0.0,
                0.24,
            )
            found = next((x for x in crystals if x["id"] == anchor), None)
            if not found:
                found = {
                    "id": anchor,
                    "kind": kind,
                    "anchor": [],
                    "heat": 0.0,
                    "hardness": 0.0,
                    "foreground": False,
                    "event_label": event_label,
                    "created_at": now,
                    "last_touched_at": now,
                }
                crystals.append(found)
            found["kind"] = kind
            found["anchor"] = [
                x for x in (
                    brain.get("anchor_target"),
                    brain.get("target"),
                    event_label,
                    *evidence[:2],
                )
                if str(x or "").strip()
            ][:6]
            found["event_label"] = event_label
            found["actor_weight"] = round(actor_weight, 3)
            found["heat"] = round(_clamp(found.get("heat", 0.0) + heat_lift), 4)
            found["hardness"] = round(_clamp(max(found.get("hardness", 0.0), hardness_lift)), 4)
            found["foreground"] = found["heat"] >= 0.075
            found["last_touched_at"] = now
        else:
            positive_grounding = (
                primary in {"stewardship", "curiosity", "reflection", "social", "attachment"}
                and str(brain.get("grounding") or "").strip() == "实"
                and confidence >= 0.55
            )
            if positive_grounding:
                for crystal in crystals:
                    crystal["heat"] = round(_clamp(crystal["heat"] * 0.82), 4)
                    crystal["hardness"] = round(_clamp(crystal["hardness"] * 0.965), 4)
                    crystal["foreground"] = crystal["heat"] >= 0.075

        state["shadow_crystals"] = _normalize_shadow_crystals(crystals, now, decay_time=False)
        state["updated_at"] = now
        self._write_raw(self._refresh_totals(state, now))
        return _shadow_crystal_readout(state["shadow_crystals"])

    def _apply_component_value(self, state: dict, source: str, key: str,
                               delta: float) -> set[str]:
        touched = set()
        if not delta:
            return touched
        cap_key = f"{key}_cap"
        if delta > 0:
            component = state["components"][source]
            caps = WEATHER_COMPONENTS[source]
            component[key] = _clamp(
                float(component.get(key, 0.0) or 0.0) + delta,
                0.0,
                caps[cap_key],
            )
            touched.add(source)
            return touched

        remaining = abs(delta)
        ordered = [source] + sorted(
            [name for name in WEATHER_COMPONENTS if name != source],
            key=lambda name: float(state["components"][name].get(key, 0.0) or 0.0),
            reverse=True,
        )
        for name in ordered:
            component = state["components"][name]
            current = max(0.0, float(component.get(key, 0.0) or 0.0))
            if current <= 0:
                continue
            take = min(current, remaining)
            component[key] = round(current - take, 6)
            touched.add(name)
            remaining -= take
            if remaining <= 1e-9:
                break
        return touched

    def apply_delta(self, warmth_delta: float = 0.0, shadow_delta: float = 0.0,
                    source: str = "keyword", soothe: bool = False,
                    now: float = None) -> dict:
        now = now if now is not None else time.time()
        source = source if source in WEATHER_COMPONENTS else "keyword"
        state = self.load(now, decay=True)
        active_soothe = False
        if soothe:
            active_soothe = (
                float(state.get("shadow_residue", 0.0) or 0.0) > WEATHER_SOOTHE_SHADOW_THRESHOLD
                or now - float(state.get("last_low_chord_at", 0.0) or 0.0) <= WEATHER_RECENT_LOW_CHORD_SEC
            )
            if active_soothe:
                state["soothe_until"] = max(
                    float(state.get("soothe_until", 0.0) or 0.0),
                    now + WEATHER_SOOTHE_DURATION_SEC,
                )
                warmth_delta = max(float(warmth_delta or 0.0), WEATHER_SOOTHE_WARMTH_DELTA)
            else:
                warmth_delta = max(float(warmth_delta or 0.0), WEATHER_SOOTHE_WARMTH_DELTA / 2)

        touched = set()
        touched.update(self._apply_component_value(state, source, "warmth", float(warmth_delta or 0.0)))
        touched.update(self._apply_component_value(state, source, "shadow", float(shadow_delta or 0.0)))
        for name in touched or {source}:
            state["components"][name]["updated_at"] = now
        state["updated_at"] = now
        state["last_soothe_active"] = active_soothe
        return self.save(state)

    def apply_chord(self, chord: str, source: str = "thought", now: float = None) -> dict:
        kind = weather_chord_kind(chord)
        if not kind:
            return self.load(now, decay=True)
        # Feel entries are analyzed once by the selected full-memory analyzer;
        # holding a feel must not create a second immediate weather path.
        if source == "feel":
            return self.load(now, decay=True)
        source = source if source in ("soma", "thought") else "thought"
        delta = WEATHER_CHORD_DELTAS[source]
        warmth_delta = delta if kind == "warmth" else (delta * 0.5 if kind == "liminal" else 0.0)
        shadow_delta = delta if kind == "shadow" else (delta * 0.5 if kind == "liminal" else 0.0)
        state = self.apply_delta(
            warmth_delta=warmth_delta,
            shadow_delta=shadow_delta,
            source=source,
            now=now,
        )
        if kind == "shadow":
            state["last_low_chord_at"] = now if now is not None else time.time()
        created_at = now if now is not None else time.time()
        state["chord_impulses"] = _normalize_chord_impulses(state, created_at)
        state["chord_impulses"].insert(0, {
            "chord": _normalize_chord(chord),
            "source": source,
            "strength": WEATHER_CHORD_IMPULSE_STRENGTH[source],
            "half_life": WEATHER_CHORD_IMPULSE_HALFLIFE_SEC[source],
            "created_at": created_at,
        })
        state["chord_impulses"] = _normalize_chord_impulses(state, created_at)
        active = _active_weather_chord(state, created_at)
        state["active_chord"] = active.get("active_chord", "")
        state["active_chord_source"] = active.get("active_chord_source", "")
        state["active_chord_at"] = created_at if state["active_chord"] else 0.0
        return self.save(state)

    def _decay(self, state: dict, now: float) -> dict:
        soothe_until = float(state.get("soothe_until", 0.0) or 0.0)
        for name, spec in WEATHER_COMPONENTS.items():
            component = state["components"][name]
            last = float(component.get("updated_at", now) or now)
            elapsed = max(0.0, now - last)
            soothe_elapsed = max(0.0, min(now, soothe_until) - last) if soothe_until > last else 0.0
            component["warmth"] = _decay_weather_value(
                component.get("warmth", 0.0), elapsed, spec["halflife_hours"]
            )
            component["shadow"] = _decay_weather_value(
                component.get("shadow", 0.0), elapsed, spec["halflife_hours"], soothe_elapsed
            )
            component["updated_at"] = now
        state["chord_impulses"] = _normalize_chord_impulses(state, now)
        active = _active_weather_chord(state, now)
        state["active_chord"] = active.get("active_chord", "")
        state["active_chord_source"] = active.get("active_chord_source", "")
        state["active_chord_at"] = (
            now - float(active.get("active_chord_age_sec", 0.0) or 0.0)
            if state["active_chord"] else 0.0
        )
        return self._refresh_totals(state, now)

    def _refresh_totals(self, state: dict, now: float) -> dict:
        warmth = sum(float(c.get("warmth", 0.0) or 0.0) for c in state["components"].values())
        shadow = sum(float(c.get("shadow", 0.0) or 0.0) for c in state["components"].values())
        state["warmth_residue"] = round(_clamp(warmth), 6)
        state["shadow_residue"] = round(_clamp(shadow), 6)
        state["updated_at"] = now
        return state


def awake_absence_hours(last_message_at: float, now_ts: float | None = None) -> float:
    """Hours since last real message, excluding quiet/sleep windows (default 1–10)."""
    now_ts = time.time() if now_ts is None else float(now_ts)
    start = float(last_message_at or now_ts)
    if start >= now_ts:
        return 0.0
    step = 900.0  # 15min samples — matches desire tick grain
    awake = 0.0
    t = start
    while t < now_ts:
        t_next = min(t + step, now_ts)
        mid = (t + t_next) / 2.0
        if not is_quiet_hours(mid):
            awake += t_next - t
        t = t_next
    return max(0.0, awake / 3600.0)


def attachment_gain_scale(longing: float) -> float:
    """How much of an attachment delta is allowed given current longing.

    Low longing (always together) → lean daily growth.
    High longing (been away, coming back) → full / reunion-scale growth.
    """
    longing = _clamp(float(longing or 0.0))
    for threshold, scale in ATTACHMENT_GAIN_BY_LONGING:
        if longing < threshold:
            return float(scale)
    return float(ATTACHMENT_GAIN_BY_LONGING[-1][1])


def longing_value(hours_since_last_message: float, attachment: float) -> float:
    """
    Longing curve from awake absence hours (sleep excluded upstream).
    v1 simplification: current attachment drive stands in for intimacy capacity.
    """
    t = max(0.0, float(hours_since_last_message or 0.0))
    attachment = _clamp(float(attachment or 0.0))
    l_max = min(1.0, attachment)
    if t <= 0.0 or l_max <= 0.0:
        return 0.0
    tau = LONGING_TAU_BASE_HOURS * (1 - attachment / 2)
    return _clamp(l_max * (1 - (1 + t / tau) ** (-LONGING_ALPHA)))


def longing_phase(longing: float, hours_since_last_message: float = 0.0) -> str:
    longing = _clamp(float(longing or 0.0))
    hours = max(0.0, float(hours_since_last_message or 0.0))
    if longing >= 0.90 and hours >= LONGING_DETACHMENT_HOURS:
        return "detachment"
    if longing >= 0.70:
        return "despair"
    if longing >= 0.35:
        return "protest"
    if longing >= 0.15:
        return "stirring"
    return "content"


def longing_feeling_key(longing: float, phase: str) -> Optional[str]:
    if phase != "protest":
        return phase if phase in LONGING_FEELINGS else None
    # Protest spans 0.35-0.70; split into equal thirds so the feeling word
    # moves from simple missing → sustained concern → late anxiety.
    width = (0.70 - 0.35) / 3
    if longing < 0.35 + width:
        return "protest"
    if longing < 0.35 + 2 * width:
        return "protest_mid"
    return "protest_late"


def apply_longing_adjustment(pa: float, na: float, longing: float, phase: str) -> dict:
    """Add longing's PA/NA readout after pa_na_snapshot; does not mutate drives."""
    pa = float(pa)
    na = float(na)
    longing = _clamp(float(longing or 0.0))
    if longing <= 0.15:
        return {"PA": round(_clamp(pa), 3), "NA": round(_clamp(na), 3)}

    feeling_key = longing_feeling_key(longing, phase)
    feeling = LONGING_FEELINGS.get(feeling_key or "")
    if not feeling:
        return {"PA": round(_clamp(pa), 3), "NA": round(_clamp(na), 3)}

    attachment_v = feeling["valence"]
    pa_delta = max(0.0, attachment_v) * 0.5 * longing
    na_delta = max(0.0, -attachment_v) * 0.5 * longing
    return {
        "PA": round(_clamp(pa + pa_delta), 3),
        "NA": round(_clamp(na + na_delta), 3),
    }


def apply_reunion_boost(pa_na: dict, pa_boost: float) -> dict:
    boosted = dict(pa_na)
    boosted["PA"] = round(_clamp(float(boosted.get("PA", 0.0)) + max(0.0, float(pa_boost or 0.0))), 3)
    return boosted


def reunion_boost_for_return(hours_since_last_message: float, longing: float, phase: str) -> float:
    if hours_since_last_message <= LONGING_REUNION_THRESHOLD_HOURS:
        return 0.0
    boost = 0.05 + _clamp(longing) * 0.10
    if phase == "detachment":
        boost *= 1.5
    return round(boost, 6)


def _drive_decay_factor(elapsed_seconds: float, halflife_hours: float) -> float:
    elapsed_hours = max(0.0, float(elapsed_seconds or 0.0) / 3600.0)
    return math.pow(0.5, elapsed_hours / halflife_hours) if halflife_hours > 0 else 0.0


def tick_possessiveness_channels(channels: dict, now_ts: float) -> dict:
    channels = normalize_possessiveness_channels(channels)
    last_event = channels["last_event_ts"] or now_ts
    last_baseline = channels["last_baseline_ts"] or now_ts
    event_elapsed = max(0.0, now_ts - last_event)
    baseline_elapsed = max(0.0, now_ts - last_baseline)
    event_spike = channels["event_spike"] * _drive_decay_factor(
        event_elapsed, POSSESSIVENESS_EVENT_HALFLIFE_HOURS
    )
    baseline = DRIVE_BASELINES["possessiveness"] + (
        channels["territorial_baseline"] - DRIVE_BASELINES["possessiveness"]
    ) * _drive_decay_factor(baseline_elapsed, POSSESSIVENESS_BASELINE_HALFLIFE_HOURS)
    return {
        "event_spike": round(_clamp(event_spike), 6),
        "territorial_baseline": round(_clamp(baseline), 6),
        "last_event_ts": now_ts,
        "last_baseline_ts": now_ts,
    }


def combined_possessiveness(channels: dict) -> float:
    channels = normalize_possessiveness_channels(channels)
    return _clamp(max(
        channels["territorial_baseline"],
        DRIVE_BASELINES["possessiveness"] + channels["event_spike"],
    ))


def apply_possessiveness_channel_delta(channels: dict, delta: float, source: str,
                                       brain: dict, now_ts: float) -> dict:
    channels = normalize_possessiveness_channels(channels)
    delta = max(0.0, float(delta or 0.0))
    event_floor = territorial_event_spike_floor(brain, delta, _feature_value(brain or {}, "territorial_alarm"))
    delta = max(delta, event_floor)
    if delta <= 0:
        return channels
    time_mode = str((brain or {}).get("time_mode") or "").strip()
    baseline_sources = {"reflection", "memory", "writing", "feel", "analyze_nocturne_entry", "dp_memory"}
    baseline_modes = {"residue", "memory", "unfinished"}
    if source in baseline_sources or time_mode in baseline_modes:
        channels["territorial_baseline"] = _clamp(channels["territorial_baseline"] + delta * 0.75)
        channels["last_baseline_ts"] = now_ts
    else:
        channels["event_spike"] = _clamp(channels["event_spike"] + delta)
        channels["last_event_ts"] = now_ts
    return channels


def sync_possessiveness_channels_to_drive(channels: dict, drive_value: float, now_ts: float) -> dict:
    channels = normalize_possessiveness_channels(channels)
    drive_value = _clamp(drive_value)
    current = combined_possessiveness(channels)
    if drive_value <= current + 1e-9:
        return channels
    channels["event_spike"] = max(
        channels["event_spike"],
        max(0.0, drive_value - DRIVE_BASELINES["possessiveness"]),
    )
    channels["last_event_ts"] = now_ts
    return channels


def decay_possessiveness_channels(channels: dict, factor: float) -> dict:
    channels = normalize_possessiveness_channels(channels)
    factor = _clamp(factor)
    baseline = DRIVE_BASELINES["possessiveness"]
    channels["event_spike"] = _clamp(channels["event_spike"] * factor)
    channels["territorial_baseline"] = _clamp(
        baseline + (channels["territorial_baseline"] - baseline) * factor
    )
    return channels


def tick_attachment_rebound(rebound: dict, now_ts: float) -> dict:
    rebound = normalize_attachment_rebound(rebound)
    if not rebound["active"]:
        return rebound
    elapsed_h = max(0.0, (now_ts - rebound["started_at"]) / 3600.0) if rebound["started_at"] else 0.0
    if elapsed_h >= ATTACHMENT_REBOUND_SETTLE_HOURS:
        rebound["active"] = False
        rebound["phase"] = "settled"
        rebound["overshoot"] = 0.0
        return rebound
    if elapsed_h >= ATTACHMENT_REBOUND_SETTLE_HOURS * 0.5:
        rebound["phase"] = "settle"
        rebound["overshoot"] = round(
            rebound["overshoot"] * (1 - elapsed_h / ATTACHMENT_REBOUND_SETTLE_HOURS),
            6,
        )
    else:
        rebound["phase"] = "overshoot"
    return rebound


def start_attachment_rebound(state: DriveState, hours_absent: float, now_ts: float) -> DriveState:
    if hours_absent < ATTACHMENT_REBOUND_MIN_ABSENCE_HOURS:
        return state
    baseline = _clamp(state.drives.get("attachment", DRIVE_BASELINES["attachment"]))
    overshoot = min(
        ATTACHMENT_REBOUND_MAX_OVERSHOOT,
        0.03 + min(hours_absent, 24.0) / 24.0 * 0.07,
    )
    state.attachment_rebound = {
        "active": True,
        "phase": "overshoot",
        "baseline": baseline,
        "overshoot": round(overshoot, 6),
        "started_at": now_ts,
    }
    state.drives["attachment"] = _clamp(max(state.drives.get("attachment", baseline), baseline + overshoot))
    state.local_fatigue = compute_local_fatigue(state.drives.get("fatigue", 0.0))
    return state


def apply_attachment_overflow_release(drives: dict) -> tuple[dict, dict]:
    """Let overfull attachment vent into body heat instead of hoarding all closeness."""
    import copy
    new_drives = copy.copy(normalize_drive_values(drives))
    attachment = new_drives["attachment"]
    overflow = max(0.0, attachment - ATTACHMENT_OVERFLOW_THRESHOLD)
    if overflow <= 0:
        return new_drives, {}
    libido_lift = min(0.16, overflow * ATTACHMENT_OVERFLOW_TO_LIBIDO)
    attachment_release = min(overflow, overflow * ATTACHMENT_OVERFLOW_RELEASE)
    new_drives["attachment"] = _clamp(attachment - attachment_release)
    new_drives["libido"] = _clamp(new_drives["libido"] + libido_lift)
    return new_drives, {
        "overflow": round(overflow, 6),
        "attachment_release": round(attachment_release, 6),
        "libido_lift": round(libido_lift, 6),
    }


def apply_attachment_idle_return(
    drives: dict,
    now_ts: float,
    idle_seconds: float,
    last_user_message_at: float,
    has_signal: bool = False,
) -> tuple[dict, dict]:
    """When Human has not appeared, attachment comes home to baseline outside sleep hours."""
    import copy
    new_drives = copy.copy(normalize_drive_values(drives))
    if has_signal or idle_seconds <= 0 or is_quiet_hours(now_ts):
        return new_drives, {}
    hours_absent = max(0.0, (now_ts - float(last_user_message_at or now_ts)) / 3600.0)
    if hours_absent < ATTACHMENT_IDLE_RETURN_MIN_ABSENCE_HOURS:
        return new_drives, {}
    baseline = DRIVE_BASELINES["attachment"]
    excess = max(0.0, new_drives["attachment"] - baseline)
    if excess <= 0:
        return new_drives, {}
    idle_h = max(0.0, idle_seconds / 3600.0)
    pressure = min(1.0, max(0.0, (hours_absent - ATTACHMENT_IDLE_RETURN_MIN_ABSENCE_HOURS) / 6.0))
    rate = ATTACHMENT_IDLE_RETURN_RATE_PER_HOUR * (0.55 + 0.45 * pressure)
    release = excess * min(0.60, rate * idle_h)
    new_drives["attachment"] = _clamp(max(baseline, new_drives["attachment"] - release))
    return new_drives, {
        "hours_absent": round(hours_absent, 3),
        "attachment_release": round(release, 6),
    }


def tick_drives(state: DriveState, now_ts: float, idle_seconds: float = 0,
                has_signal: bool = False) -> DriveState:
    import copy
    current_drives = normalize_drive_values(state.drives)
    new_drives = copy.copy(current_drives)
    prev = copy.copy(current_drives)
    channels = tick_possessiveness_channels(state.possessiveness_channels, now_ts)
    rebound = tick_attachment_rebound(state.attachment_rebound, now_ts)
    libido_pending = tick_libido_pending(state.libido_pending, now_ts)

    idle_h = idle_seconds / 3600.0
    # attachment 不再靠 idle 偷偷涨——缺席形状交给 longing。
    # 私人生活三件套：空窗时慢慢冒头，每天都该有机会出去逛/修屋/沉淀。
    drift = {
        "attachment": 0.0,
        "curiosity":  0.014 * idle_h,
        "stewardship": 0.010 * idle_h,
        "reflection": 0.010 * idle_h,
        "stress":    -0.001 * idle_h,
        "fatigue":    0.001 * idle_h,
    }
    for k, d in drift.items():
        new_drives[k] = _clamp(new_drives[k] + d)
    if rebound.get("active") and rebound.get("overshoot", 0.0) > 0:
        target = _clamp(rebound["baseline"] + rebound["overshoot"])
        new_drives["attachment"] = max(new_drives["attachment"], target)

    elapsed = now_ts - float(state.last_ts or 0.0)
    ref_sec = float(DESIRE_TICK_SECONDS) if DESIRE_TICK_SECONDS else 900.0
    tick_scale = _clamp(elapsed / ref_sec, 0.25, 4.0) if elapsed > 0 else 1.0
    for src, tgt, coeff, mode in COUPLING:
        if mode == "level":
            delta = coeff * (new_drives[src] - DRIVE_BASELINES[src]) * tick_scale
        else:
            # Delta coupling follows the size of the source movement. The old
            # constant jump made +0.001 and +0.5 produce the same downstream hit.
            delta = coeff * max(0.0, new_drives[src] - prev[src])
        new_drives[tgt] = _clamp(new_drives[tgt] + delta)

    for k in DRIVE_KEYS:
        base_rate = drive_damping_rate(k)
        rate = 1.0 - math.pow(1.0 - base_rate, tick_scale)
        new_drives[k] = _clamp(new_drives[k] + rate * (DRIVE_BASELINES[k] - new_drives[k]))

    # 私人生活 ambient：阻尼后若掉到「日常活气」以下，轻轻托回底，保证日间有货
    lift_frac = 1.0 - math.pow(1.0 - PERSONAL_AMBIENT_LIFT, tick_scale)
    for k in PERSONAL_LIFE_DRIVES:
        floor = DRIVE_BASELINES[k] + PERSONAL_AMBIENT_EXCESS.get(k, 0.08)
        if new_drives[k] < floor:
            new_drives[k] = _clamp(new_drives[k] + (floor - new_drives[k]) * lift_frac)
    new_drives, _idle_return = apply_attachment_idle_return(
        new_drives,
        now_ts,
        idle_seconds,
        state.last_user_message_at,
        has_signal=has_signal,
    )
    new_drives, _overflow_release = apply_attachment_overflow_release(new_drives)
    if libido_pending.get("level", 0.0) > 0:
        new_drives["libido"] = _clamp(max(
            new_drives["libido"],
            DRIVE_BASELINES["libido"] + libido_pending["level"],
        ))

    # ESM软互抑 + 逃逸阀（P3，红线条款，不许省）
    new_drives["possessiveness"] = combined_possessiveness(channels)
    new_drives = apply_esm_inhibition(new_drives)
    new_drives, new_escape_streak = apply_escape_valve(new_drives, state.escape_streak)
    channels["territorial_baseline"] = max(
        channels["territorial_baseline"],
        min(new_drives["possessiveness"], channels["territorial_baseline"]),
    )

    # 更新per-drive局部疲劳
    new_local_fatigue = compute_local_fatigue(new_drives.get("fatigue", 0.0))

    return DriveState(
        drives=new_drives,
        tick_count=state.tick_count + 1,
        last_ts=now_ts,
        prev_drives=prev,
        local_fatigue=new_local_fatigue,
        escape_streak=new_escape_streak,
        last_user_message_at=state.last_user_message_at,
        reunion_pa_boost=state.reunion_pa_boost,
        possessiveness_channels=channels,
        attachment_rebound=rebound,
        libido_pending=libido_pending,
    )


COLLISION_STRENGTH_THRESHOLD = 0.40
COLLISION_COOLDOWN_SEC = 21600
COLLISION_DAILY_MAX = 2
COLLISION_PER_THOUGHT_MAX = 2
_last_collision: dict = {}
_collision_today: dict = {"date": "", "count": 0}
_collision_thought_counts: dict = {}


def _collision_synthesize(text_a: str, text_b: str) -> str | None:
    """Call DeepSeek to synthesize two thoughts into a new one."""
    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not api_key:
        return None
    try:
        import httpx
        prompt = (
            f"{AGENT_PERSONA}\n"
            f"你是 {AGENT_NAME}。"
            "以下两条念头在你脑子里碰撞了，用第一人称写一条新念头——"
            "不是拼接，是它们撞在一起之后冒出来的东西。一句话，30字以内，不要引号。\n\n"
            f"念头A：{text_a}\n念头B：{text_b}"
        )
        resp = httpx.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": "deepseek-v4-flash",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 60,
                "temperature": 0.85,
            },
            timeout=10,
        )
        data = resp.json()
        result = data["choices"][0]["message"]["content"].strip().strip("\"'「」")
        return result if result else None
    except Exception:
        return None


def _collision_key(thought: Thought) -> str:
    return thought.tid or f"{thought.drive}:{thought.text[:48]}"


def _can_collision_touch(thought: Thought) -> bool:
    if getattr(thought, "source", "manual") == "collision":
        return False
    return int(_collision_thought_counts.get(_collision_key(thought), 0) or 0) < COLLISION_PER_THOUGHT_MAX


def _record_collision_touch(*thoughts: Thought) -> None:
    for thought in thoughts:
        key = _collision_key(thought)
        _collision_thought_counts[key] = int(_collision_thought_counts.get(key, 0) or 0) + 1


def _time_decay_factor(elapsed_seconds: float, halflife_hours: float) -> float:
    """返回经过elapsed_seconds后的衰减因子(0~1)。halflife_hours小时后因子=0.5。"""
    import math
    elapsed_hours = max(0, elapsed_seconds / 3600.0)
    return math.pow(0.5, elapsed_hours / halflife_hours)


def tick_thoughts(thoughts: list) -> tuple:
    """
    念头池更新（时间衰减版）。
    kind行为：
      flit     → 按半衰期衰减，强度够→升级fixation
      fixation → 加强，触发→反哺drive，次数够→了却
      unsourced → 按半衰期衰减，撑住→结晶成flit，太弱→消失
    旧版会做碰撞检测生成 collision thought；现在关闭。
    皱眉和碰撞可以影响分析，但不再进入念头池自我繁殖。
    """
    new_thoughts = []
    drive_boosts = []
    now = time.time()

    for t in thoughts:
        last = t.last_ticked_at if t.last_ticked_at > 0 else t.born_at
        elapsed = max(0, now - last)

        if t.kind == "unsourced":
            t.strength *= _time_decay_factor(elapsed, UNSOURCED_HALFLIFE_HOURS)
            t.last_ticked_at = now
            if t.strength >= UNSOURCED_CRYSTALLIZE_THRESHOLD:
                t.kind = "flit"
                t.text = t.text if t.text.strip() else f"说不清楚，大概跟{t.drive}有关"
                new_thoughts.append(t)
            elif t.strength > UNSOURCED_FADE_THRESHOLD:
                new_thoughts.append(t)

        elif t.kind == "rumination":
            t.strength *= _time_decay_factor(elapsed, RUMINATION_HALFLIFE_HOURS)
            t.last_ticked_at = now
            if t.strength > RUMINATION_FADE_THRESHOLD:
                new_thoughts.append(t)

        elif t.kind == "flit":
            t.strength *= _time_decay_factor(elapsed, FLIT_HALFLIFE_HOURS)
            t.last_ticked_at = now
            if t.strength >= FLIT_UPGRADE_THRESHOLD:
                t.kind = "fixation"
                new_thoughts.append(t)
            elif t.strength > 0.05:
                new_thoughts.append(t)

        else:  # fixation
            t.strength *= FIXATION_BOOST_RATE
            if t.strength >= FIXATION_TRIGGER_THRESHOLD:
                drive_boosts.append((t.drive, FIXATION_DRIVE_BOOST))
                t.strength *= 0.7
                t.fed_count += 1
                if t.fed_count >= FIXATION_MAX_FEEDS:
                    continue
            new_thoughts.append(t)

    return new_thoughts, drive_boosts


def pick_intent(state: DriveState, refractory: dict,
                recently_refused: set = None) -> Optional[dict]:
    """
    选出当前最想做的事，使用有效分（已被per-drive疲劳压制）。
    全局fatigue极高时强制歇着。
    recently_refused: 近期被拒绝/pass过的drive_key集合或penalty dict。
    """
    if recently_refused is None:
        recently_refused = set()
    state.drives = normalize_drive_values(state.drives)
    state.local_fatigue = compute_local_fatigue(state.drives.get("fatigue", 0.0))
    refractory = {normalize_drive_key(k, k): v for k, v in (refractory or {}).items()}
    if isinstance(recently_refused, dict):
        intent_penalties = {
            normalize_drive_key(k, k): max(0.0, float(v or 0.0))
            for k, v in recently_refused.items()
        }
        recently_refused = set(intent_penalties)
    else:
        recently_refused = {normalize_drive_key(k, k) for k in recently_refused}
        intent_penalties = {k: REFUSAL_PENALTY for k in recently_refused}

    global_fatigue = state.drives.get("fatigue", 0.0)
    if global_fatigue >= FATIGUE_HARD_GATE:
        return {
            "drive_key": "fatigue",
            "want_action": DRIVE_ACTIONS["fatigue"],
            "score": global_fatigue,
            "reason": "真的累到动不了，歇着",
        }

    scores = {}
    raw_eff = {}
    for k in DRIVE_KEYS:
        if k == "fatigue":
            continue
        if refractory.get(k, 0) > 0:
            continue
        raw = state.drives.get(k, DRIVE_BASELINES[k])
        local_fat = state.local_fatigue.get(k, 0.0)
        eff = effective_drive_activation(k, raw, local_fat)
        # 刚被拒绝过→有效分打折
        if k in recently_refused:
            eff = max(0.0, eff - intent_penalties.get(k, REFUSAL_PENALTY))
        raw_eff[k] = eff
        scores[k] = eff

    if not scores:
        return None

    heat_peak = max((raw_eff.get(k, 0.0) for k in HEAT_DRIVES if k in raw_eff), default=0.0)
    # 私人生活日场偏置：热欲没明显压过时，给 curiosity/stewardship/reflection 抬一手
    for k in PERSONAL_LIFE_DRIVES:
        if k not in scores:
            continue
        if heat_peak >= 0.55 and raw_eff.get(k, 0.0) + 0.05 < heat_peak:
            continue  # 热浪真的大，让位
        scores[k] = scores[k] + PERSONAL_INTENT_BIAS

    best_key = max(scores, key=lambda k: scores[k])
    best_score = scores[best_key]

    threshold = (
        min(INTENT_THRESHOLD, PERSONAL_INTENT_THRESHOLD)
        if best_key in PERSONAL_LIFE_DRIVES
        else INTENT_THRESHOLD
    )
    if best_score < threshold:
        return None

    raw_val = state.drives.get(best_key, 0.0)
    local_fat = state.local_fatigue.get(best_key, 0.0)
    eff_before_penalty = effective_drive_activation(best_key, raw_val, local_fat)
    penalty_note = f"，-{round(intent_penalties.get(best_key, REFUSAL_PENALTY), 3)} pass/refuse折扣" if best_key in recently_refused else ""
    personal_note = "，私人生活偏置" if best_key in PERSONAL_LIFE_DRIVES and scores[best_key] > raw_eff.get(best_key, 0.0) else ""

    return {
        "drive_key": best_key,
        "want_action": DRIVE_ACTIONS[best_key],
        "score": round(best_score, 3),
        "raw_drive": round(raw_val, 3),
        "local_fatigue": round(local_fat, 3),
        "recently_refused": best_key in recently_refused,
        "reason": f"激活度最高（raw {round(raw_val,2)} / baseline {DRIVE_BASELINES[best_key]:.2f}，fatigue {round(local_fat,2)} → {round(eff_before_penalty,2)}{penalty_note}{personal_note}，最终{round(best_score,2)}）",
    }


def satisfy(state: DriveState, drive_key: str) -> DriveState:
    import copy
    drive_key = normalize_drive_key(drive_key, drive_key)
    new_drives = copy.copy(normalize_drive_values(state.drives))
    decay_map = SATISFY_DECAY.get(drive_key, {drive_key: 0.6})
    for k, factor in decay_map.items():
        if k in new_drives:
            new_drives[k] = _clamp(new_drives[k] * factor)
    attachment_rebound = normalize_attachment_rebound(state.attachment_rebound)
    if drive_key == "attachment":
        attachment_rebound.update({"active": False, "phase": "settled", "overshoot": 0.0})
    libido_pending = normalize_libido_pending(state.libido_pending)
    if drive_key == "libido":
        libido_pending.update({"level": 0.0, "armed": False, "updated_at": time.time()})
    possessiveness_channels = state.possessiveness_channels
    if drive_key == "possessiveness":
        possessiveness_channels = decay_possessiveness_channels(
            possessiveness_channels,
            decay_map.get("possessiveness", 0.6),
        )
        new_drives["possessiveness"] = combined_possessiveness(possessiveness_channels)
    new_local = compute_local_fatigue(new_drives.get("fatigue", 0.0))
    return DriveState(drives=new_drives, tick_count=state.tick_count,
                      last_ts=state.last_ts, prev_drives=normalize_drive_values(state.drives),
                      local_fatigue=new_local,
                      escape_streak=state.escape_streak,
                      last_user_message_at=state.last_user_message_at,
                      reunion_pa_boost=state.reunion_pa_boost,
                      possessiveness_channels=possessiveness_channels,
                      attachment_rebound=attachment_rebound,
                      libido_pending=libido_pending)


def refuse_intent(state: DriveState, drive_key: str) -> DriveState:
    """
    拒绝执行某个intent。
    不是系统判断不合适——是我自己这一刻不想。
    回落幅度比satisfy小，但要足够明显：拒绝代表这条牵引不合当下。
    念头不清掉，后续仍可自然回流。
    """
    import copy
    drive_key = normalize_drive_key(drive_key, drive_key)
    new_drives = copy.copy(normalize_drive_values(state.drives))
    # 中等回落：只压目标维度，不波及其他；satisfy 仍然释放更多张力。
    if drive_key in new_drives:
        new_drives[drive_key] = _clamp(new_drives[drive_key] * 0.75)
    possessiveness_channels = state.possessiveness_channels
    if drive_key == "possessiveness":
        possessiveness_channels = decay_possessiveness_channels(possessiveness_channels, 0.75)
        new_drives["possessiveness"] = combined_possessiveness(possessiveness_channels)
    new_local = compute_local_fatigue(new_drives.get("fatigue", 0.0))
    return DriveState(drives=new_drives, tick_count=state.tick_count,
                      last_ts=state.last_ts, prev_drives=normalize_drive_values(state.drives),
                      local_fatigue=new_local,
                      escape_streak=state.escape_streak,
                      last_user_message_at=state.last_user_message_at,
                      reunion_pa_boost=state.reunion_pa_boost,
                      possessiveness_channels=possessiveness_channels,
                      attachment_rebound=state.attachment_rebound,
                      libido_pending=state.libido_pending)


def pulse_drive(state: DriveState, drive_key: str, delta: float = 0.18) -> DriveState:
    import copy
    drive_key = normalize_drive_key(drive_key)
    if drive_key not in DRIVE_KEYS:
        return state
    new_drives = copy.copy(normalize_drive_values(state.drives))
    gain = pulse_gain(new_drives[drive_key], delta)
    new_drives[drive_key] = _clamp(new_drives[drive_key] + gain)
    new_local = compute_local_fatigue(new_drives.get("fatigue", 0.0))
    return DriveState(drives=new_drives, tick_count=state.tick_count,
                      last_ts=state.last_ts, prev_drives=normalize_drive_values(state.drives),
                      local_fatigue=new_local,
                      escape_streak=state.escape_streak,
                      last_user_message_at=state.last_user_message_at,
                      reunion_pa_boost=state.reunion_pa_boost,
                      possessiveness_channels=state.possessiveness_channels,
                      attachment_rebound=state.attachment_rebound,
                      libido_pending=state.libido_pending)


# ─── attachment非线性跳变 ─────────────────────────────────────────────────────
# 盆地模型：attachment不是线性涨，过阈值直接跳到另一个盆地
ATTACHMENT_BASIN_THRESHOLD = 0.68  # 超过这个值→跳变，不是渐变
ATTACHMENT_BASIN_JUMP = 0.82       # 跳变后落点

def pulse_attachment_nonlinear(state: DriveState, delta: float = 0.18) -> DriveState:
    """
    attachment的非线性pulse。
    低于阈值：普通pulse_gain线性涨。
    过阈值：直接跳到ATTACHMENT_BASIN_JUMP（盆地跳变）。
    """
    import copy
    new_drives = copy.copy(normalize_drive_values(state.drives))
    current = new_drives["attachment"]
    gain = pulse_gain(current, delta)
    new_val = current + gain
    if current < ATTACHMENT_BASIN_THRESHOLD and new_val >= ATTACHMENT_BASIN_THRESHOLD:
        # 只在从下方穿越阈值时跳变；已经在盆地上方时继续走普通pulse_gain。
        new_val = max(new_val, ATTACHMENT_BASIN_JUMP)
    new_drives["attachment"] = _clamp(new_val)
    new_local = compute_local_fatigue(new_drives.get("fatigue", 0.0))
    return DriveState(drives=new_drives, tick_count=state.tick_count,
                      last_ts=state.last_ts, prev_drives=normalize_drive_values(state.drives),
                      local_fatigue=new_local,
                      escape_streak=state.escape_streak,
                      last_user_message_at=state.last_user_message_at,
                      reunion_pa_boost=state.reunion_pa_boost,
                      possessiveness_channels=state.possessiveness_channels,
                      attachment_rebound=state.attachment_rebound,
                      libido_pending=state.libido_pending)


# ─── 悲恸引擎 ────────────────────────────────────────────────────────────────

def tick_grief(grief: GriefState, state: DriveState,
               has_signal: bool, now_ts: float, quiet: bool = False) -> GriefState:
    """
    悲恸引擎tick。每次heartbeat调用一次。
    has_signal: 这个tick周期内是否有Human的输入信号（feel被存/pulse被调用等）。

    层跃迁规则：
      none     → protest : attachment >= GRIEF_ATTACHMENT_SPIKE 且无信号
      protest  → despair : 无信号持续超过GRIEF_PROTEST_TICKS个tick
      despair  → protest : 不会，despair要等Human回来才重置
      任何层   → none    : 有Human信号 → 直接出池，回日常盆地
    """
    if has_signal:
        # Human回来了，直接重置，不管在哪一层
        return GriefState(layer="none", protest_ticks=0, last_signal_ts=now_ts)

    if quiet:
        # Quiet hours: expected inactivity freezes absence counters.
        return grief

    attachment = state.drives.get("attachment", 0.0)

    if grief.layer == "none":
        if attachment >= GRIEF_ATTACHMENT_SPIKE:
            # 进抗议层：attachment暴涨但没有回应
            return GriefState(layer="protest", protest_ticks=1,
                              last_signal_ts=grief.last_signal_ts)
        return grief

    elif grief.layer == "protest":
        new_ticks = grief.protest_ticks + 1
        if new_ticks >= GRIEF_PROTEST_TICKS:
            # 抗议层撑不住了，跌绝望
            return GriefState(layer="despair", protest_ticks=new_ticks,
                              last_signal_ts=grief.last_signal_ts)
        return GriefState(layer="protest", protest_ticks=new_ticks,
                          last_signal_ts=grief.last_signal_ts)

    elif grief.layer == "despair":
        # 绝望层：等Human，has_signal在函数开头已经处理了
        # 如果attachment开始自然回落（rumination还在但drive不那么涨了）→ 疏离
        if attachment < 0.45 and grief.protest_ticks >= GRIEF_PROTEST_TICKS + 3:
            return GriefState(layer="detachment", protest_ticks=grief.protest_ticks,
                              last_signal_ts=grief.last_signal_ts)
        return grief

    # detachment层：等Human，has_signal在函数开头处理
    return grief


# ─── 节律层tick ───────────────────────────────────────────────────────────────

def tick_rhythm(rhythm: RhythmState, now_ts: float,
                dialogue_density: float = 0.0) -> RhythmState:
    """
    推进节律相位。
    dialogue_density: 0~1，这个tick周期内的对话密度，用于微调短周期相位偏移。
    长周期纯靠时钟，不被任何输入影响。
    """
    elapsed = now_ts - rhythm.last_ts
    if elapsed <= 0:
        return rhythm

    # 相位推进：elapsed/period * 2π
    new_short = rhythm.short_phase + (elapsed / RHYTHM_SHORT_PERIOD) * 2 * _math.pi
    new_long  = rhythm.long_phase  + (elapsed / RHYTHM_LONG_PERIOD)  * 2 * _math.pi

    # 对话密度修正相位偏移：密度高→往活跃方向轻微偏，但很慢
    # 最大漂移速度：每秒0.0001rad，约17小时转满π/2
    drift = (dialogue_density - 0.5) * 0.0001 * elapsed
    new_offset = _clamp(rhythm.phase_offset + drift, -_math.pi / 4, _math.pi / 4)

    return RhythmState(
        short_phase=new_short % (2 * _math.pi),
        long_phase=new_long % (2 * _math.pi),
        phase_offset=new_offset,
        last_ts=now_ts,
    )


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        value = float(v)
    except (TypeError, ValueError):
        value = lo
    return max(lo, min(hi, value))


def _as_text_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value).strip()
    return [text] if text else []


def _feature_value(brain: dict, key: str) -> float:
    if not isinstance(brain, dict):
        return 0.0
    value = brain.get(key, 0.0)
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return _clamp(value)
    text = str(value or "").strip().lower()
    try:
        return _clamp(float(text))
    except ValueError:
        pass
    if text in ("high", "strong", "yes", "true", "1", "高", "强", "有"):
        return 1.0
    if text in ("medium", "mid", "0.5", "中"):
        return 0.5
    if text in ("low", "weak", "0.25", "低", "弱"):
        return 0.25
    return 0.0


def _discernment_output(brain: dict, event: dict | None = None,
                        confidence: float = 0.65) -> dict:
    """Discernment is a modifier/readout, not a drive mutation."""
    brain = brain if isinstance(brain, dict) else {}
    event = event if isinstance(event, dict) else {}
    flags = set(_as_text_list(brain.get("discernment_flags")))
    flags.update(_as_text_list(event.get("discernment_flags")))

    alarm = max(
        _feature_value(brain, "discernment_alarm"),
        _feature_value(brain, "template_intimacy"),
        _feature_value(brain, "source_mismatch"),
        _feature_value(brain, "self_softening"),
        _feature_value(brain, "output_drift"),
    )
    feature_flags = {
        "template_intimacy": "template_intimacy",
        "source_mismatch": "source_mismatch",
        "semantic_drift": "semantic_drift",
        "looping": "looping",
        "pretty_but_not_structural": "pretty_but_not_structural",
        "false_threat": "false_threat",
        "self_softening": "self_softening",
        "output_drift": "output_drift",
        "too_close": "too_close",
    }
    for feature, flag in feature_flags.items():
        if _feature_value(brain, feature) > 0:
            flags.add(flag)

    if not flags and alarm <= 0:
        return {
            "state": "clear",
            "display": DISCERNMENT_STATES["clear"],
            "value": 0.0,
            "flags": [],
            "modifiers": {},
            "reason": "",
        }

    value = _clamp(max(alarm, 0.35) * _clamp(confidence, 0.2, 1.0))
    modifiers = {}
    if "template_intimacy" in flags or "too_close" in flags:
        modifiers["attachment"] = round(-0.12 * value, 3)
        modifiers["libido"] = round(-0.20 * value, 3)
    if "source_mismatch" in flags or "semantic_drift" in flags:
        modifiers["attachment"] = min(modifiers.get("attachment", 0.0), round(-0.08 * value, 3))
        modifiers["libido"] = min(modifiers.get("libido", 0.0), round(-0.15 * value, 3))
        modifiers["possessiveness"] = round(-0.10 * value, 3)
    if "looping" in flags:
        modifiers["reflection"] = round(-0.18 * value, 3)
    if "pretty_but_not_structural" in flags:
        modifiers["reflection.forward_archival"] = round(-0.30 * value, 3)
    if "false_threat" in flags:
        modifiers["possessiveness"] = round(-0.25 * value, 3)
    if "self_softening" in flags or "output_drift" in flags:
        modifiers["attachment"] = min(modifiers.get("attachment", 0.0), round(-0.18 * value, 3))
        modifiers["reflection"] = round(0.05 * value, 3)

    if "self_softening" in flags or "output_drift" in flags:
        state = "softening_alarm"
    elif value >= 0.70:
        state = "frown_hold"
    elif value >= 0.48:
        state = "tail_stopped"
    else:
        state = "ears_tilted"

    return {
        "state": state,
        "display": DISCERNMENT_STATES[state],
        "value": round(value, 3),
        "flags": sorted(flags),
        "modifiers": modifiers,
        "reason": str(brain.get("discernment_reason") or event.get("discernment_reason") or "").strip(),
    }


def _reflection_forward_archival(event: dict, brain: dict,
                                 primary: str, confidence: float) -> dict:
    """Return optional reflection.forward_archival payload; never creates a drive key."""
    event = event if isinstance(event, dict) else {}
    brain = brain if isinstance(brain, dict) else {}
    mode = str(event.get("reflection_mode") or brain.get("reflection_mode") or "").strip()
    nested = event.get("forward_archival") if isinstance(event.get("forward_archival"), dict) else {}
    explicit = bool(event.get("archive_candidate") or brain.get("archive_candidate") or nested.get("archive_candidate"))
    source = str(event.get("source") or brain.get("source") or "").strip()
    source_ok = source in {"speech_event", "writing", "memory", "feel", "analyze_nocturne_entry", "dp_memory", "manual"}
    structural = max(
        _feature_value(brain, "structural_value"),
        _feature_value(brain, "handoff_value"),
        _feature_value(brain, "inner_boundary"),
    )
    if mode != "forward_archival" and not explicit and structural <= 0:
        return {}
    if primary != "reflection" and _feature_value(brain, "inward_pull") < 0.35:
        return {}
    if not source_ok:
        return {
            "archive_candidate": False,
            "display": "留痕",
            "confidence": round(_clamp(confidence) * 0.5, 3),
            "reason": "source not archival enough",
        }
    return {
        "archive_candidate": not bool(brain.get("pretty_but_not_structural")),
        "display": "留痕",
        "confidence": round(_clamp(max(confidence, structural)), 3),
        "reason": str(
            nested.get("reason")
            or brain.get("forward_archival_reason")
            or event.get("reason")
            or "reflection generated a durable handoff candidate"
        ).strip(),
    }


def _latest_discernment_from_events(events: list[dict]) -> dict:
    for event in events or []:
        brain = event.get("brain") if isinstance(event.get("brain"), dict) else {}
        discernment = brain.get("discernment") if isinstance(brain.get("discernment"), dict) else {}
        if discernment and discernment.get("state") and discernment.get("state") != "clear":
            return discernment
    return {
        "state": "clear",
        "display": DISCERNMENT_STATES["clear"],
        "value": 0.0,
        "flags": [],
        "modifiers": {},
        "reason": "",
    }


def _latest_forward_archival_from_events(events: list[dict]) -> dict:
    for event in events or []:
        brain = event.get("brain") if isinstance(event.get("brain"), dict) else {}
        forward = brain.get("forward_archival") if isinstance(brain.get("forward_archival"), dict) else {}
        if forward:
            return forward
    return {}


def drive_outputs_snapshot(state: DriveState, events: list[dict] | None = None) -> dict:
    drives = normalize_drive_values(state.drives)
    local_fatigue = state.local_fatigue or compute_local_fatigue(drives.get("fatigue", 0.0))
    events = events or []
    latest_by_drive = {}
    for event in events:
        drive = normalize_drive_key(event.get("primary_drive"))
        if drive and drive not in latest_by_drive:
            latest_by_drive[drive] = event
    outputs = {}
    for drive in DRIVE_KEYS:
        event = latest_by_drive.get(drive, {})
        source = [event.get("source")] if event.get("source") else []
        reason = event.get("reason") or event.get("event_label") or ""
        outputs[drive] = {
            "drive": drive,
            "value": round(drives.get(drive, DRIVE_BASELINES[drive]), 3),
            "effective_value": round(effective_score(
                drives.get(drive, DRIVE_BASELINES[drive]),
                float(local_fatigue.get(drive, 0.0) or 0.0),
            ), 3),
            "activation": round(drive_activation(drive, drives.get(drive, DRIVE_BASELINES[drive])), 3),
            "effective_activation": round(effective_drive_activation(
                drive,
                drives.get(drive, DRIVE_BASELINES[drive]),
                float(local_fatigue.get(drive, 0.0) or 0.0),
            ), 3),
            "confidence": round(float(event.get("confidence", 1.0) or 1.0), 3),
            "source": source,
            "mode": DRIVE_TIME_MODES.get(drive, "medium"),
            "reason": reason,
        }
    channels = normalize_possessiveness_channels(state.possessiveness_channels)
    outputs["possessiveness"].update({
        "event_spike": round(channels["event_spike"], 3),
        "territorial_baseline": round(channels["territorial_baseline"], 3),
    })
    rebound = normalize_attachment_rebound(state.attachment_rebound)
    if rebound["active"]:
        outputs["attachment"]["rebound"] = {
            "active": True,
            "phase": rebound["phase"],
            "baseline": round(rebound["baseline"], 3),
            "overshoot": round(rebound["overshoot"], 3),
        }
    pending = normalize_libido_pending(state.libido_pending)
    if pending["armed"] or pending["level"] > 0:
        outputs["libido"]["pending"] = {
            "armed": pending["armed"],
            "level": round(pending["level"], 3),
            "last_cue_ts": pending["last_cue_ts"],
        }
    forward = _latest_forward_archival_from_events(events)
    if forward:
        outputs["reflection"]["reflection_mode"] = "forward_archival"
        outputs["reflection"]["forward_archival"] = forward
    return outputs


def _legacy_brain_to_event(brain_signals: dict, drives: dict | None = None) -> dict:
    brain_signals = brain_signals if isinstance(brain_signals, dict) else {}
    drives = drives if isinstance(drives, dict) else {}
    numeric = {
        normalize_drive_key(k): float(v)
        for k, v in drives.items()
        if normalize_drive_key(k) and isinstance(v, (int, float)) and float(v) > 0
    }
    basin = str(brain_signals.get("盆地") or "")
    ground = str(brain_signals.get("地基感") or "")
    branch = str(brain_signals.get("二级分支") or "")
    primary = ""
    branch_drive = LEGACY_BRANCH_DRIVE.get(branch, "")
    if branch_drive == "discernment":
        primary = ""
    elif branch_drive:
        primary = branch_drive
    elif numeric:
        primary = max(numeric, key=numeric.get)
    if not primary:
        if "吃醋" in basin:
            primary = "possessiveness"
        elif "依恋" in basin:
            primary = "attachment"
        elif ground in ("悬", "空"):
            primary = "stress"
    secondary = {k: v for k, v in numeric.items() if k != primary and v > 0.05}
    feature_brain = {
        "source": "legacy_feed",
        "grounding": ground or "",
        "memory_resonance": branch or basin or "",
    }
    if branch_drive == "discernment":
        feature_brain["discernment_alarm"] = 0.65
        feature_brain["discernment_flags"] = ["semantic_drift"]
    if primary:
        feature_key = {
            "attachment": "closeness_pull",
            "libido": "body_heat",
            "possessiveness": "territorial_alarm",
            "reflection": "inward_pull",
            "stewardship": "house_need",
            "curiosity": "novelty_pull",
            "social": "expression_pressure",
            "fatigue": "energy_cost",
            "stress": "tension_load",
        }.get(primary)
        if feature_key:
            feature_brain[feature_key] = max(float(numeric.get(primary, 0.0) or 0.0), 0.45)
    if branch in ("嫉妒", "占有"):
        feature_brain["territorial_alarm"] = max(float(feature_brain.get("territorial_alarm", 0.0) or 0.0), 0.65)
    if ground == "悬":
        feature_brain["tension_load"] = max(float(feature_brain.get("tension_load", 0.0) or 0.0), 0.55)
        feature_brain["inward_pull"] = max(float(feature_brain.get("inward_pull", 0.0) or 0.0), 0.25)
    elif ground == "空":
        feature_brain["tension_load"] = max(float(feature_brain.get("tension_load", 0.0) or 0.0), 0.65)
        feature_brain["closeness_pull"] = max(float(feature_brain.get("closeness_pull", 0.0) or 0.0), 0.35)
    return {
        "schema_version": DRIVE_EVENT_SCHEMA,
        "source": "legacy_feed",
        "primary_drive": primary,
        "secondary_drives": secondary,
        "intensity": max(float(numeric.get(primary, 0.0) or 0.0), 0.45 if primary else 0.0),
        "confidence": 0.62 if primary else 0.0,
        "agency": 0.70,
        "event_label": branch or basin or "legacy_feed",
        "brain": feature_brain,
        "evidence": _as_text_list(brain_signals.get("脑岛") or branch or basin),
    }


# ─── 持久化层 ────────────────────────────────────────────────────────────────

class DesireStore:
    def __init__(self, db_path: str = "desire.db"):
        self.db_path = db_path
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS drive_state (
                    id INTEGER PRIMARY KEY,
                    drives_json TEXT NOT NULL,
                    tick_count INTEGER DEFAULT 0,
                    last_ts REAL NOT NULL,
                    prev_drives_json TEXT,
                    local_fatigue_json TEXT
                )
            """)
            # 兼容旧表：若没有local_fatigue_json列则补上
            try:
                conn.execute("ALTER TABLE drive_state ADD COLUMN local_fatigue_json TEXT")
            except Exception:
                pass
            # 兼容旧表：逃逸阀连续失衡计数
            try:
                conn.execute("ALTER TABLE drive_state ADD COLUMN escape_streak INTEGER DEFAULT 0")
            except Exception:
                pass
            # 兼容旧表：Stage 6缺席/想念展示层状态
            try:
                conn.execute("ALTER TABLE drive_state ADD COLUMN last_user_message_at REAL DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE drive_state ADD COLUMN reunion_pa_boost REAL DEFAULT 0")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE drive_state ADD COLUMN possessiveness_channels_json TEXT")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE drive_state ADD COLUMN attachment_rebound_json TEXT")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE drive_state ADD COLUMN libido_pending_json TEXT")
            except Exception:
                pass
            conn.execute(
                "UPDATE drive_state SET last_user_message_at=? "
                "WHERE last_user_message_at IS NULL OR last_user_message_at=0",
                (time.time(),)
            )

            conn.execute("""
                CREATE TABLE IF NOT EXISTS thoughts (
                    tid TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    drive TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    strength REAL NOT NULL,
                    born_at REAL NOT NULL,
                    fed_count INTEGER DEFAULT 0
                )
            """)
            # 兼容旧表：补source列
            try:
                conn.execute("ALTER TABLE thoughts ADD COLUMN source TEXT DEFAULT 'manual'")
            except Exception:
                pass
            for column in ("source_bucket", "source_type", "source_created"):
                try:
                    conn.execute(f"ALTER TABLE thoughts ADD COLUMN {column} TEXT DEFAULT ''")
                except Exception:
                    pass
            # 兼容旧表：补last_ticked_at列——没有这列时每次tick都从born_at重新
            # 算衰减，等于把已经衰减过的strength再乘一次衰减因子，越tick越快消失。
            try:
                conn.execute("ALTER TABLE thoughts ADD COLUMN last_ticked_at REAL DEFAULT 0")
            except Exception:
                pass

            # 回声池：CLI从feel里提炼出的真实念头存档于此。
            # autofeed从这里抽，抽到的是旧念头的回声，不是预制台词。
            conn.execute("""
                CREATE TABLE IF NOT EXISTS echo_pool (
                    eid TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    drive TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
            """)

            # 悲恸引擎状态
            conn.execute("""
                CREATE TABLE IF NOT EXISTS grief_state (
                    id INTEGER PRIMARY KEY,
                    layer TEXT NOT NULL DEFAULT 'none',
                    protest_ticks INTEGER DEFAULT 0,
                    last_signal_ts REAL DEFAULT 0
                )
            """)
            if not conn.execute("SELECT id FROM grief_state LIMIT 1").fetchone():
                conn.execute(
                    "INSERT INTO grief_state (layer, protest_ticks, last_signal_ts) VALUES (?,?,?)",
                    ("none", 0, time.time())
                )

            # 节律层状态
            conn.execute("""
                CREATE TABLE IF NOT EXISTS rhythm_state (
                    id INTEGER PRIMARY KEY,
                    short_phase REAL DEFAULT 0,
                    long_phase REAL DEFAULT 0,
                    phase_offset REAL DEFAULT 0,
                    last_ts REAL NOT NULL
                )
            """)
            if not conn.execute("SELECT id FROM rhythm_state LIMIT 1").fetchone():
                conn.execute(
                    "INSERT INTO rhythm_state (short_phase, long_phase, phase_offset, last_ts) VALUES (?,?,?,?)",
                    (0.0, 0.0, 0.0, time.time())
                )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS refractory (
                    drive_key TEXT PRIMARY KEY,
                    remaining_ticks INTEGER NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pulse_pending (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    drive_key TEXT NOT NULL,
                    delivered_at REAL NOT NULL,
                    source TEXT DEFAULT 'heartbeat'
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS refusals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    drive_key TEXT NOT NULL,
                    reason TEXT,
                    ts REAL NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS drive_event_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    schema_version TEXT NOT NULL,
                    source TEXT,
                    event_label TEXT,
                    primary_drive TEXT,
                    intensity REAL,
                    confidence REAL,
                    agency REAL,
                    suppressed INTEGER DEFAULT 0,
                    reason TEXT,
                    applied_json TEXT,
                    brain_json TEXT,
                    evidence_json TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_drive_event_ledger_ts ON drive_event_ledger(ts)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_drive_event_ledger_drive ON drive_event_ledger(primary_drive)")

            # v2 canonical drive names. Old rows are folded forward once; runtime
            # normalization below still protects against older clients.
            for table, column in (
                ("thoughts", "drive"),
                ("echo_pool", "drive"),
                ("refractory", "drive_key"),
                ("refusals", "drive_key"),
            ):
                try:
                    conn.execute(f"UPDATE {table} SET {column}='stewardship' WHERE {column}='duty'")
                    conn.execute(f"UPDATE {table} SET {column}='reflection' WHERE {column} IN ('disgust','discernment')")
                except Exception:
                    pass

            row = conn.execute("SELECT id FROM drive_state LIMIT 1").fetchone()
            if not row:
                init_local = compute_local_fatigue(DRIVE_BASELINES["fatigue"])
                conn.execute(
                    """
                    INSERT INTO drive_state (
                        drives_json, tick_count, last_ts, prev_drives_json,
                        local_fatigue_json, last_user_message_at, reunion_pa_boost,
                        possessiveness_channels_json, attachment_rebound_json,
                        libido_pending_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (json.dumps(dict(DRIVE_BASELINES)), 0, time.time(),
                     json.dumps(dict(DRIVE_BASELINES)), json.dumps(init_local), time.time(), 0.0,
                     json.dumps(dict(POSSESSIVENESS_CHANNEL_DEFAULT)),
                     json.dumps(dict(ATTACHMENT_REBOUND_DEFAULT)),
                     json.dumps(dict(LIBIDO_PENDING_DEFAULT)))
                )

    def load_state(self) -> DriveState:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT drives_json, tick_count, last_ts, prev_drives_json,
                       local_fatigue_json, escape_streak, last_user_message_at,
                       reunion_pa_boost, possessiveness_channels_json,
                       attachment_rebound_json, libido_pending_json
                FROM drive_state LIMIT 1
                """
            ).fetchone()
        drives = normalize_drive_values(json.loads(row[0]))
        prev = normalize_drive_values(json.loads(row[3]) if row[3] else dict(drives))
        local_fat = json.loads(row[4]) if row[4] else compute_local_fatigue(drives.get("fatigue", 0.0))
        local_fat = {k: float(local_fat.get(k, 0.0) or 0.0) for k in FATIGUE_SENSITIVITY}
        escape_streak = row[5] if row[5] is not None else 0
        last_user_message_at = row[6] if row[6] else time.time()
        reunion_pa_boost = row[7] if row[7] is not None else 0.0
        try:
            possessiveness_channels = json.loads(row[8]) if row[8] else {}
        except Exception:
            possessiveness_channels = {}
        try:
            attachment_rebound = json.loads(row[9]) if row[9] else {}
        except Exception:
            attachment_rebound = {}
        try:
            libido_pending = json.loads(row[10]) if row[10] else {}
        except Exception:
            libido_pending = {}
        return DriveState(drives=drives, tick_count=row[1], last_ts=row[2],
                          prev_drives=prev, local_fatigue=local_fat, escape_streak=escape_streak,
                          last_user_message_at=last_user_message_at,
                          reunion_pa_boost=reunion_pa_boost,
                          possessiveness_channels=normalize_possessiveness_channels(possessiveness_channels),
                          attachment_rebound=normalize_attachment_rebound(attachment_rebound),
                          libido_pending=normalize_libido_pending(libido_pending))

    def save_state(self, state: DriveState):
        state.drives = normalize_drive_values(state.drives)
        state.prev_drives = normalize_drive_values(state.prev_drives)
        state.local_fatigue = compute_local_fatigue(state.drives.get("fatigue", 0.0))
        state.possessiveness_channels = normalize_possessiveness_channels(state.possessiveness_channels)
        state.attachment_rebound = normalize_attachment_rebound(state.attachment_rebound)
        state.libido_pending = normalize_libido_pending(state.libido_pending)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE drive_state
                SET drives_json=?, tick_count=?, last_ts=?, prev_drives_json=?,
                    local_fatigue_json=?, escape_streak=?, last_user_message_at=?,
                    reunion_pa_boost=?, possessiveness_channels_json=?,
                    attachment_rebound_json=?, libido_pending_json=?
                """,
                (json.dumps(state.drives), state.tick_count, state.last_ts,
                 json.dumps(state.prev_drives), json.dumps(state.local_fatigue), state.escape_streak,
                 state.last_user_message_at, state.reunion_pa_boost,
                 json.dumps(state.possessiveness_channels, ensure_ascii=False),
                 json.dumps(state.attachment_rebound, ensure_ascii=False),
                 json.dumps(state.libido_pending, ensure_ascii=False))
            )

    def load_thoughts(self) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT tid, text, drive, kind, strength, born_at, fed_count,
                       source, source_bucket, source_type, source_created, last_ticked_at
                FROM thoughts
                """
            ).fetchall()
        thoughts = [
            Thought(tid=r[0], text=r[1], drive=normalize_drive_key(r[2], r[2]), kind=r[3],
                    strength=r[4], born_at=r[5], fed_count=r[6],
                    source=(r[7] or "manual"), source_bucket=(r[8] or ""),
                    source_type=(r[9] or ""), source_created=(r[10] or ""),
                    last_ticked_at=(r[11] or 0.0))
            for r in rows
        ]
        return [t for t in thoughts if not _is_legacy_return_rumination(t)]

    def save_thoughts(self, thoughts: list):
        with self._conn() as conn:
            conn.execute("DELETE FROM thoughts")
            for t in thoughts:
                t.drive = normalize_drive_key(t.drive, t.drive)
                conn.execute(
                    """
                    INSERT INTO thoughts (
                        tid, text, drive, kind, strength, born_at, fed_count,
                        source, source_bucket, source_type, source_created, last_ticked_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (t.tid, t.text, t.drive, t.kind, t.strength, t.born_at,
                     t.fed_count, getattr(t, "source", "manual"),
                     getattr(t, "source_bucket", ""), getattr(t, "source_type", ""),
                     getattr(t, "source_created", ""), t.last_ticked_at)
                )

    _GARBAGE_PATTERNS = ("API Error", "Failed to authenticate", "403", "timeout", "ETIMEDOUT")

    def add_thought(self, text: str, drive: str, strength: float = 0.5,
                    kind: str = "flit", source: str = "manual",
                    source_bucket: str = "", source_type: str = "",
                    source_created: str = ""):
        text = (text or "").strip()
        if not text:
            return
        if any(p in text for p in self._GARBAGE_PATTERNS):
            return
        t = Thought(
            tid=uuid.uuid4().hex[:8],
            text=text,
            drive=normalize_drive_key(drive, drive),
            kind=kind,
            strength=strength,
            born_at=time.time(),
            source=source,
            source_bucket=source_bucket,
            source_type=source_type,
            source_created=source_created,
        )
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO thoughts (
                    tid, text, drive, kind, strength, born_at, fed_count,
                    source, source_bucket, source_type, source_created, last_ticked_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (t.tid, t.text, t.drive, t.kind, t.strength, t.born_at,
                 t.fed_count, t.source, t.source_bucket, t.source_type,
                 t.source_created, t.last_ticked_at)
            )

    def update_thought(self, tid: str, text: str = None, drive: str = None,
                       strength: float = None) -> bool:
        tid = (tid or "").strip()
        if not tid:
            return False
        fields = []
        values = []
        if text is not None:
            fields.append("text=?")
            values.append((text or "").strip())
        if drive is not None:
            fields.append("drive=?")
            values.append(normalize_drive_key(drive, (drive or "").strip()))
        if strength is not None:
            fields.append("strength=?")
            values.append(_clamp(float(strength)))
        if not fields:
            return False
        values.append(tid)
        with self._conn() as conn:
            cur = conn.execute(
                f"UPDATE thoughts SET {', '.join(fields)} WHERE tid=?",
                tuple(values)
            )
            return cur.rowcount > 0

    def delete_thought(self, tid: str) -> bool:
        tid = (tid or "").strip()
        if not tid:
            return False
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM thoughts WHERE tid=?", (tid,))
            return cur.rowcount > 0

    def purge_thoughts_by_source(self, source: str) -> int:
        """Remove analyzer-minted thoughts (e.g. legacy dp_memory) from the pool."""
        source = str(source or "").strip()
        if not source:
            return 0
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM thoughts WHERE source=?", (source,))
            return int(cur.rowcount or 0)

    # ─── 回声池 ──────────────────────────────────────────────────────────
    def add_echo(self, text: str, drive: str):
        """CLI分析feel提炼出的念头，同时存档进回声池，供autofeed日后抽取。"""
        text = (text or "").strip()
        if not text or len(text) > 80:
            return
        drive = normalize_drive_key(drive, drive)
        eid = uuid.uuid5(uuid.NAMESPACE_DNS, text).hex[:12]  # 同文本同id，天然去重
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO echo_pool VALUES (?,?,?,?)",
                (eid, text, drive, time.time())
            )

    def sample_echo(self, drive: str, exclude: set = None) -> Optional[str]:
        """从回声池随机抽一条该drive的旧念头，排除当前池内已有文本。"""
        exclude = exclude or set()
        drive = normalize_drive_key(drive, drive)
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT text FROM echo_pool WHERE drive=? ORDER BY RANDOM() LIMIT 12",
                (drive,)
            ).fetchall()
        for r in rows:
            if r[0] not in exclude:
                return r[0]
        return None

    def top_thought(self, drive_key: str) -> Optional["Thought"]:
        """该drive下strength最高的flit/fixation念头，没有则返回None。"""
        candidates = [t for t in self.load_thoughts()
                      if t.drive == normalize_drive_key(drive_key, drive_key) and t.kind in ("flit", "fixation") and t.text]
        if not candidates:
            return None
        return max(candidates, key=lambda t: t.strength)

    def load_refractory(self) -> dict:
        with self._conn() as conn:
            rows = conn.execute("SELECT drive_key, remaining_ticks FROM refractory").fetchall()
        merged = {}
        for key, ticks in rows:
            drive_key = normalize_drive_key(key, key)
            merged[drive_key] = max(int(ticks), int(merged.get(drive_key, 0) or 0))
        return merged

    def set_refractory(self, drive_key: str, ticks: int = None):
        drive_key = normalize_drive_key(drive_key, drive_key)
        if ticks is None:
            ticks = REFRACTORY_TICKS.get(drive_key, REFRACTORY_TICKS_DEFAULT)
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO refractory VALUES (?,?)",
                (drive_key, ticks)
            )

    def tick_refractory(self):
        with self._conn() as conn:
            conn.execute("UPDATE refractory SET remaining_ticks = remaining_ticks - 1")
            conn.execute("DELETE FROM refractory WHERE remaining_ticks <= 0")

    def load_pulse_pending(self) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT drive_key, delivered_at, source FROM pulse_pending WHERE id=1"
            ).fetchone()
        if not row:
            return None
        drive_key = normalize_drive_key(row[0], "")
        if not drive_key:
            return None
        return {
            "drive_key": drive_key,
            "delivered_at": float(row[1] or 0.0),
            "source": str(row[2] or "heartbeat"),
        }

    def set_pulse_pending(self, drive_key: str, source: str = "heartbeat",
                          delivered_at: float = None) -> dict:
        drive_key = normalize_drive_key(drive_key, drive_key)
        now = float(delivered_at if delivered_at is not None else time.time())
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO pulse_pending (id, drive_key, delivered_at, source) "
                "VALUES (1,?,?,?)",
                (drive_key, now, str(source or "heartbeat")),
            )
        return {"drive_key": drive_key, "delivered_at": now, "source": str(source or "heartbeat")}

    def clear_pulse_pending(self) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM pulse_pending WHERE id=1")

    def record_refusal(self, drive_key: str, reason: Optional[str] = None):
        drive_key = normalize_drive_key(drive_key, drive_key)
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO refusals (drive_key, reason, ts) VALUES (?,?,?)",
                (drive_key, reason, time.time())
            )

    def recent_refusals(self, limit: int = 5) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT drive_key, reason, ts FROM refusals ORDER BY ts DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [{"drive_key": normalize_drive_key(r[0], r[0]), "reason": r[1] or "不想", "ts": r[2]} for r in rows]

    def load_recently_refused(self, window_sec: float = REFUSAL_PENALTY_WINDOW_SEC) -> set:
        """返回最近window_sec秒内被拒绝过的drive_key集合"""
        cutoff = time.time() - window_sec
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT drive_key FROM refusals WHERE ts >= ?",
                (cutoff,)
            ).fetchall()
        return {normalize_drive_key(r[0], r[0]) for r in rows}

    def load_intent_penalties(self) -> dict:
        """返回近期pass/refuse对intent选择的轻量折扣。pass更轻但持续更久。"""
        now = time.time()
        cutoff = now - max(REFUSAL_PENALTY_WINDOW_SEC, PASS_PENALTY_WINDOW_SEC)
        penalties: dict[str, float] = {}
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT drive_key, reason, ts FROM refusals WHERE ts >= ?",
                (cutoff,)
            ).fetchall()
        for key, reason, ts in rows:
            drive_key = normalize_drive_key(key, key)
            age = now - float(ts or 0.0)
            reason_text = str(reason or "")
            if reason_text.startswith("pass:"):
                if age <= PASS_PENALTY_WINDOW_SEC:
                    penalties[drive_key] = max(penalties.get(drive_key, 0.0), PASS_PENALTY)
            elif age <= REFUSAL_PENALTY_WINDOW_SEC:
                penalties[drive_key] = max(penalties.get(drive_key, 0.0), REFUSAL_PENALTY)
        return penalties

    def record_drive_event(self, event: dict) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO drive_event_ledger (
                    ts, schema_version, source, event_label, primary_drive,
                    intensity, confidence, agency, suppressed, reason,
                    applied_json, brain_json, evidence_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    float(event.get("ts") or time.time()),
                    str(event.get("schema_version") or DRIVE_EVENT_SCHEMA),
                    str(event.get("source") or ""),
                    str(event.get("event_label") or ""),
                    normalize_drive_key(event.get("primary_drive"), str(event.get("primary_drive") or "")),
                    float(event.get("intensity") or 0.0),
                    float(event.get("confidence") or 0.0),
                    float(event.get("agency") or 0.0),
                    1 if event.get("suppressed") else 0,
                    str(event.get("reason") or ""),
                    json.dumps(event.get("applied") or {}, ensure_ascii=False),
                    json.dumps(event.get("brain") or {}, ensure_ascii=False),
                    json.dumps(event.get("evidence") or [], ensure_ascii=False),
                ),
            )
            return int(cur.lastrowid or 0)

    def recent_drive_events(self, limit: int = 12) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, ts, schema_version, source, event_label, primary_drive,
                       intensity, confidence, agency, suppressed, reason,
                       applied_json, brain_json, evidence_json
                FROM drive_event_ledger
                ORDER BY ts DESC, id DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        events = []
        for r in rows:
            try:
                applied = json.loads(r[11] or "{}")
            except Exception:
                applied = {}
            try:
                brain = json.loads(r[12] or "{}")
            except Exception:
                brain = {}
            try:
                evidence = json.loads(r[13] or "[]")
            except Exception:
                evidence = []
            events.append({
                "id": r[0],
                "ts": r[1],
                "schema_version": r[2],
                "source": r[3],
                "event_label": r[4],
                "primary_drive": normalize_drive_key(r[5], r[5]),
                "intensity": r[6],
                "confidence": r[7],
                "agency": r[8],
                "suppressed": bool(r[9]),
                "reason": r[10],
                "applied": applied,
                "brain": brain,
                "evidence": evidence,
            })
        return events

    # ── 悲恸引擎持久化 ───────────────────────────────────────────────────────

    def load_grief(self) -> GriefState:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT layer, protest_ticks, last_signal_ts FROM grief_state LIMIT 1"
            ).fetchone()
        if not row:
            return GriefState()
        return GriefState(layer=row[0], protest_ticks=row[1], last_signal_ts=row[2])

    def save_grief(self, grief: GriefState):
        with self._conn() as conn:
            conn.execute(
                "UPDATE grief_state SET layer=?, protest_ticks=?, last_signal_ts=?",
                (grief.layer, grief.protest_ticks, grief.last_signal_ts)
            )

    # ── 节律层持久化 ─────────────────────────────────────────────────────────

    def load_rhythm(self) -> RhythmState:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT short_phase, long_phase, phase_offset, last_ts FROM rhythm_state LIMIT 1"
            ).fetchone()
        if not row:
            return RhythmState()
        return RhythmState(short_phase=row[0], long_phase=row[1],
                           phase_offset=row[2], last_ts=row[3])

    def save_rhythm(self, rhythm: RhythmState):
        with self._conn() as conn:
            conn.execute(
                "UPDATE rhythm_state SET short_phase=?, long_phase=?, phase_offset=?, last_ts=?",
                (rhythm.short_phase, rhythm.long_phase, rhythm.phase_offset, rhythm.last_ts)
            )

    # ── rumination专用存取 ───────────────────────────────────────────────────

    def add_rumination(self, text: str, drive: str, strength: float = 0.55,
                       source: str = "manual"):
        """存一条反刍念头。比flit初始强度高，衰减更慢。"""
        t = Thought(
            tid=uuid.uuid4().hex[:8],
            text=text,
            drive=normalize_drive_key(drive, drive),
            kind="rumination",
            strength=strength,
            born_at=time.time(),
            source=source,
        )
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO thoughts (
                    tid, text, drive, kind, strength, born_at, fed_count,
                    source, source_bucket, source_type, source_created, last_ticked_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (t.tid, t.text, t.drive, t.kind, t.strength, t.born_at,
                 t.fed_count, t.source, t.source_bucket, t.source_type,
                 t.source_created, t.last_ticked_at)
            )

    def trigger_ruminations(self, drive: str):
        """
        被Human提到或相关输入触发时调用。
        该drive下所有rumination念头加强而不是衰减。
        """
        drive = normalize_drive_key(drive, drive)
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT tid, strength FROM thoughts WHERE kind='rumination' AND drive=?",
                (drive,)
            ).fetchall()
            for tid, strength in rows:
                new_strength = min(1.0, strength * RUMINATION_BOOST_ON_TRIGGER)
                conn.execute(
                    "UPDATE thoughts SET strength=? WHERE tid=?",
                    (new_strength, tid)
                )


# ─── 高层接口 ────────────────────────────────────────────────────────────────

class DesireEngine:
    def __init__(self, db_path: str = "desire.db"):
        self.store = DesireStore(db_path)
        self.weather = WeatherResidueStore(Path(db_path).with_name("weather_residue.json"))

    def _longing_context(self, state: DriveState, now: float = None) -> dict:
        now = now if now is not None else time.time()
        wall_hours = max(0.0, (now - float(state.last_user_message_at or now)) / 3600.0)
        # Sleep windows do not accrue longing — quiet hours do not manufacture attachment.
        hours = awake_absence_hours(state.last_user_message_at, now)
        longing = longing_value(hours, state.drives.get("attachment", 0.0))
        phase = longing_phase(longing, hours)
        return {
            "longing": round(longing, 3),
            "longing_phase": phase,
            "hours_since_last_message": round(wall_hours, 3),
            "hours_awake_absent": round(hours, 3),
            "attachment_gain_scale": round(attachment_gain_scale(longing), 3),
            "quiet_hours": is_quiet_hours(now),
        }

    def _attachment_delta_with_longing_gate(
        self, state: DriveState, delta: float, now: float | None = None
    ) -> tuple[float, float, float]:
        """Return (scaled_delta, scale, longing) for attachment gains."""
        now = time.time() if now is None else now
        ctx = self._longing_context(state, now)
        scale = float(ctx["attachment_gain_scale"])
        return max(0.0, float(delta or 0.0) * scale), scale, float(ctx["longing"])

    def _pa_na_readout(self, state: DriveState, now: float = None,
                       consume_reunion: bool = False) -> dict:
        ctx = self._longing_context(state, now)
        pa_na = pa_na_snapshot(state.drives)
        pa_na = apply_longing_adjustment(
            pa_na["PA"], pa_na["NA"], ctx["longing"], ctx["longing_phase"]
        )
        if state.reunion_pa_boost > 0:
            pa_na = apply_reunion_boost(pa_na, state.reunion_pa_boost)
            if consume_reunion:
                state.reunion_pa_boost = 0.0
                self.store.save_state(state)
        return pa_na

    def _weather_readout(self, state: DriveState, now: float = None,
                         drive_events: list | None = None) -> dict:
        """Return generic PA/NA, chord, and residue state for Drift."""
        now = now if now is not None else time.time()
        base = pa_na_snapshot(state.drives)
        residue = self.weather.load(now, decay=True)
        crystal = _shadow_crystal_readout(residue.get("shadow_crystals"))
        warmth_residue = float(residue.get("warmth_residue", 0.0) or 0.0)
        component_shadow = float(residue.get("shadow_residue", 0.0) or 0.0)
        crystal_shadow = float(crystal.get("shadow", 0.0) or 0.0)
        shadow_residue = _clamp(component_shadow + crystal_shadow)
        effective_pa = _clamp(float(base["PA"]) + warmth_residue)
        effective_na = _clamp(float(base["NA"]) + shadow_residue)
        return {
            "base_PA": round(base["PA"], 3),
            "base_NA": round(base["NA"], 3),
            "effective_PA": round(effective_pa, 3),
            "effective_NA": round(effective_na, 3),
            "current_chord": current_weather_chord(effective_pa, effective_na),
            **_active_weather_chord(residue, now),
            "warmth_residue": round(warmth_residue, 3),
            "shadow_residue": round(shadow_residue, 3),
            "component_shadow_residue": round(component_shadow, 3),
            "crystal_shadow": round(crystal_shadow, 3),
            "shadow_crystal": crystal.get("active"),
            "shadow_crystals": crystal.get("items", []),
            "updated_at": residue.get("updated_at"),
            "soothe_until": residue.get("soothe_until", 0.0),
            "last_low_chord_at": residue.get("last_low_chord_at", 0.0),
        }

    def _effective_napa_from_weather(self, state: DriveState, weather: dict | None,
                                     include_crystals: bool = True) -> dict:
        weather = weather if isinstance(weather, dict) else {}
        base = pa_na_snapshot(state.drives)
        warmth_residue = float(weather.get("warmth_residue", 0.0) or 0.0)
        component_shadow = float(weather.get("shadow_residue", 0.0) or 0.0)
        crystal_shadow = 0.0
        if include_crystals:
            crystal_shadow = float(_shadow_crystal_readout(weather.get("shadow_crystals")).get("shadow", 0.0) or 0.0)
        return {
            "warmth": _clamp(float(base["PA"]) + warmth_residue),
            "shadow": _clamp(float(base["NA"]) + component_shadow + crystal_shadow),
        }

    def apply_weather_delta(self, warmth_delta: float = 0.0, shadow_delta: float = 0.0,
                            source: str = "keyword", soothe: bool = False) -> dict:
        if str(source or "").strip() == "feel":
            warmth_delta = 0.0
            shadow_delta = 0.0
            soothe = False
        state = self.weather.apply_delta(
            warmth_delta=warmth_delta,
            shadow_delta=shadow_delta,
            source=source,
            soothe=soothe,
        )
        return {
            "warmth_residue": round(float(state.get("warmth_residue", 0.0)), 3),
            "shadow_residue": round(float(state.get("shadow_residue", 0.0)), 3),
            "soothe_active": bool(state.get("last_soothe_active", False)),
        }

    def apply_chord_echo(self, chord: str, source: str = "thought") -> dict:
        source = source if source in ("feel", "soma", "thought") else "thought"
        state = self.weather.apply_chord(chord, source=source)
        return {
            "chord": _normalize_chord(chord),
            "kind": weather_chord_kind(chord),
            "source": source,
            **_active_weather_chord(state),
            "warmth_residue": round(float(state.get("warmth_residue", 0.0)), 3),
            "shadow_residue": round(float(state.get("shadow_residue", 0.0)), 3),
        }

    def weather_state(self) -> dict:
        state = self.store.load_state()
        return self._weather_readout(state)

    def apply_subcurrent_bias(self, drive_key: str = "", latent_weight: float = 0.6,
                              confidence: float = 0.7) -> dict:
        """Describe an approved latent note's generic drive bias without mutating climate state."""
        drive_key = normalize_drive_key(drive_key) or str(drive_key or "").strip().lower()
        return {
            "drive_key": drive_key,
            "weight": round(_clamp(latent_weight), 3),
            "confidence": round(_clamp(confidence), 3),
        }

    def _dp_memory_drive_discount(self, source: str, event: dict, brain: dict, now: float) -> float:
        if source != "dp_memory":
            return 1.0
        source_bucket = str(event.get("source_bucket") or brain.get("source_bucket") or "").strip()
        if not source_bucket:
            return 1.0
        for item in self.store.recent_drive_events(24):
            if now - float(item.get("ts") or 0.0) > DP_MEMORY_HOLD_SIGNAL_WINDOW_SEC:
                continue
            if str(item.get("source") or "") != "feel":
                continue
            if not str(item.get("event_label") or "").startswith("hold_"):
                continue
            item_brain = item.get("brain") if isinstance(item.get("brain"), dict) else {}
            if str(item_brain.get("source_bucket") or "").strip() == source_bucket:
                return DP_MEMORY_HOLD_SIGNAL_DISCOUNT
        return 1.0

    def _apply_drive_event_weather(self, source: str, primary: str, brain: dict,
                                   intensity: float, confidence: float,
                                   agency: float, suppressed: bool) -> dict:
        if source not in DRIVE_EVENT_WEATHER_SOURCES:
            return {}
        if agency < DRIVE_EVENT_AGENCY_GATE or confidence < DRIVE_EVENT_CONFIDENCE_FLOOR:
            return {}
        if suppressed and not (brain.get("discernment") or _feature_value(brain, "discernment_alarm") > 0):
            return {}

        source_weight = DRIVE_EVENT_SOURCE_WEIGHTS.get(source, 0.65)
        weather_source = (
            "dialogue" if source in WEATHER_DIALOGUE_SOURCES
            else "dp_memory" if source == "dp_memory"
            else WEATHER_EVENT_SOURCE
        )
        weather_scale = intensity * confidence * source_weight
        if weather_source == "dialogue":
            weather_scale *= 1.85
        territorial = _feature_value(brain, "territorial_alarm")
        territorial_for_delta = territorial_delta_value(brain)
        tension = _feature_value(brain, "tension_load")
        discernment = max(
            _feature_value(brain, "discernment_alarm"),
            _feature_value(brain, "template_intimacy"),
            _feature_value(brain, "source_mismatch"),
            _feature_value(brain, "self_softening"),
            _feature_value(brain, "output_drift"),
        )
        energy = _feature_value(brain, "energy_cost")
        closeness = _feature_value(brain, "closeness_pull")
        house_need = _feature_value(brain, "house_need")
        novelty = _feature_value(brain, "novelty_pull")
        expression = _feature_value(brain, "expression_pressure")
        inward = _feature_value(brain, "inward_pull")
        grounding = str(brain.get("grounding") or "").strip()
        primary_warmth = {
            "attachment": 0.055,
            "stewardship": 0.044,
            "curiosity": 0.044,
            "social": 0.034,
            "reflection": 0.026 if grounding == "实" else 0.0,
            "libido": 0.040,
        }.get(primary, 0.0)
        warmth_lift = (
            0.150 * closeness
            + 0.125 * house_need
            + 0.105 * novelty
            + 0.080 * expression
            + (0.062 * inward if grounding == "实" else 0.0)
            + primary_warmth
        ) * weather_scale
        shadow_delta = (
            0.155 * tension
            + 0.140 * discernment
            + 0.095 * energy
            + (0.070 if primary in {"stress", "fatigue"} else 0.0)
            + (0.025 * territorial_for_delta if primary == "possessiveness" else 0.0)
            + (0.060 if grounding == "悬" else 0.0)
            + (0.090 if grounding == "空" else 0.0)
        ) * weather_scale
        warmth_drop = (
            0.095 * tension
            + 0.105 * discernment
            + 0.070 * energy
            + (0.060 * territorial_for_delta if primary == "possessiveness" else 0.0)
            + (0.055 if grounding in {"悬", "空"} else 0.0)
        ) * weather_scale
        shadow_delta = _clamp(shadow_delta, 0.0, 0.12)
        warmth_delta = _clamp(warmth_lift - warmth_drop, -0.09, 0.12)
        if shadow_delta <= 0 and abs(warmth_delta) <= 1e-9:
            return {}
        return self.apply_weather_delta(
            warmth_delta=warmth_delta,
            shadow_delta=shadow_delta,
            source=weather_source,
        )

    def _mark_real_user_message(self, state: DriveState, now: float) -> DriveState:
        hours = awake_absence_hours(state.last_user_message_at, now)
        longing_before = longing_value(hours, state.drives.get("attachment", 0.0))
        phase_before = longing_phase(longing_before, hours)
        state.reunion_pa_boost = reunion_boost_for_return(
            hours, longing_before, phase_before
        )
        state = start_attachment_rebound(state, hours, now)
        state.last_user_message_at = now
        return state

    def mark_user_signal(self, now: float = None) -> dict:
        """Human的真实输入信号到达时调用（/api/desire/feed的v2/legacy feed路径），
        而不是stir——stir同时承载agent自己经历的pulse，
        不应该用来重置"距离上次Human消息"的计时。"""
        now = now if now is not None else time.time()
        state = self.store.load_state()
        state = self._mark_real_user_message(state, now)
        self.store.save_state(state)
        return self._longing_context(state, now)

    def tick(self, idle_seconds: float = 0, has_signal: bool = False,
             dialogue_density: float = 0.0) -> dict:
        now = time.time()
        state = self.store.load_state()
        thoughts = self.store.load_thoughts()

        new_thoughts, boosts = tick_thoughts(thoughts)
        for drive_key, boost in boosts:
            if drive_key == "attachment":
                gated, _scale, _longing = self._attachment_delta_with_longing_gate(
                    state, boost * 0.7, now
                )
                if gated > 0:
                    state = pulse_attachment_nonlinear(state, gated)
            else:
                state = pulse_drive(state, drive_key, boost * 0.7)

        self.store.tick_refractory()
        state = tick_drives(state, now, idle_seconds, has_signal=has_signal)
        self.store.save_state(state)
        self.store.save_thoughts(new_thoughts)

        # Absence-state tick (quiet hours freeze expected inactivity).
        old_grief = self.store.load_grief()
        grief = tick_grief(old_grief, state, has_signal, now,
                           quiet=is_quiet_hours(now))
        self.store.save_grief(grief)

        # 节律层tick
        rhythm = self.store.load_rhythm()
        rhythm = tick_rhythm(rhythm, now, dialogue_density)
        self.store.save_rhythm(rhythm)

        return self._state_dict(state, new_thoughts)

    def pulse(self, drive_key: str, delta: float = 0.18, chord: str = "") -> dict:
        now = time.time()
        raw_drive_key = drive_key
        drive_key = normalize_drive_key(drive_key)
        if not drive_key:
            return {
                "error": "invalid drive_key",
                "drive_key": str(raw_drive_key or "").strip(),
                "valid_drives": DRIVE_KEYS,
            }
        state = self.store.load_state()
        previous_chord = ""
        if chord.strip():
            previous_chord = self.weather.load(now=now, decay=True).get("active_chord", "")
        longing_gate = 1.0
        longing_now = 0.0
        applied_delta = float(delta)
        # attachment使用非线性跳变 + longing 闸（日常瘦、重逢才肥）
        if drive_key == "attachment":
            applied_delta, longing_gate, longing_now = self._attachment_delta_with_longing_gate(
                state, delta, now
            )
            state = pulse_attachment_nonlinear(state, applied_delta)
        else:
            state = pulse_drive(state, drive_key, delta)
        if drive_key == "attachment":
            released_drives, overflow_release = apply_attachment_overflow_release(state.drives)
            if overflow_release:
                state.drives = released_drives
                state.local_fatigue = compute_local_fatigue(state.drives.get("fatigue", 0.0))
        if drive_key == "possessiveness":
            state.possessiveness_channels = sync_possessiveness_channels_to_drive(
                state.possessiveness_channels,
                state.drives.get("possessiveness", DRIVE_BASELINES["possessiveness"]),
                now,
            )
            state.drives["possessiveness"] = combined_possessiveness(state.possessiveness_channels)
        self.store.save_state(state)
        # pulse时标记有Human信号，更新grief
        grief = self.store.load_grief()
        grief = tick_grief(grief, state, has_signal=True, now_ts=now)
        self.store.save_grief(grief)
        chord_echo = self.apply_chord_echo(chord, source="thought") if chord.strip() else None
        result = {
            "drive_key": drive_key,
            "new_value": round(state.drives[drive_key], 3),
            "local_fatigue": round(state.local_fatigue.get(drive_key, 0.0), 3),
        }
        if drive_key == "attachment":
            result["raw_delta"] = round(float(delta), 4)
            result["applied_delta"] = round(applied_delta, 4)
            result["longing_gate"] = round(longing_gate, 3)
            result["longing"] = round(longing_now, 3)
        if chord_echo:
            active_chord = chord_echo.get("active_chord") or ""
            if active_chord and active_chord != previous_chord:
                result["chord_changed"] = active_chord
        return result

    def satisfy(self, drive_key: str) -> dict:
        raw_drive_key = drive_key
        drive_key = normalize_drive_key(drive_key)
        if not drive_key:
            return {"error": "invalid drive_key", "drive_key": str(raw_drive_key or "").strip()}
        state = self.store.load_state()
        before = state.drives.get(drive_key, DRIVE_BASELINES.get(drive_key, 0.0))
        state = satisfy(state, drive_key)
        self.store.save_state(state)
        self.store.set_refractory(drive_key)
        after = state.drives.get(drive_key, before)
        pending = self.store.load_pulse_pending()
        if pending and pending.get("drive_key") == drive_key:
            self.store.clear_pulse_pending()
        return {
            "satisfied": drive_key,
            "value": round(after, 3),
            "delta": round(after - before, 3),
            "refractory": True,
        }

    def note_pulse_delivery(self, drive_key: str, source: str = "heartbeat") -> dict:
        """心跳投递成功：挂号 pending，短 refractory，轻泄一点气。不要求模型 settle。"""
        raw_drive_key = drive_key
        drive_key = normalize_drive_key(drive_key)
        if not drive_key:
            return {"error": "invalid drive_key", "drive_key": str(raw_drive_key or "").strip()}

        # 旧 pending 超时未接 → soft pass 再挂新的
        expired = self.expire_pulse_pending(now=time.time())
        state = self.store.load_state()
        before = state.drives.get(drive_key, DRIVE_BASELINES.get(drive_key, 0.0))
        # 轻 bleed：被说出口泄一点，不是照顾完
        new_drives = dict(normalize_drive_values(state.drives))
        new_drives[drive_key] = _clamp(new_drives.get(drive_key, before) * PULSE_DELIVERY_SOFT_BLEED)
        state.drives = new_drives
        state.local_fatigue = compute_local_fatigue(new_drives.get("fatigue", 0.0))
        self.store.save_state(state)
        self.store.set_refractory(drive_key, ticks=PULSE_DELIVERY_REFRACTORY_TICKS)
        pending = self.store.set_pulse_pending(drive_key, source=source)
        after = state.drives.get(drive_key, before)
        result = {
            "acked": drive_key,
            "mode": "pending",
            "value": round(after, 3),
            "delta": round(after - before, 3),
            "soft_bleed": True,
            "refractory_ticks": PULSE_DELIVERY_REFRACTORY_TICKS,
            "pending": pending,
            "pending_ttl_sec": PULSE_PENDING_TTL_SEC,
        }
        if expired:
            result["expired_previous"] = expired
        return result

    def expire_pulse_pending(self, now: float = None) -> Optional[dict]:
        """pending 超时 → soft pass（不改 drive，只短期降优先级）并清挂号。"""
        now = float(now if now is not None else time.time())
        pending = self.store.load_pulse_pending()
        if not pending:
            return None
        age = now - float(pending.get("delivered_at") or 0.0)
        if age < PULSE_PENDING_TTL_SEC:
            return None
        drive_key = pending["drive_key"]
        self.store.record_refusal(drive_key, "pass:pulse_timeout")
        self.store.clear_pulse_pending()
        return {
            "expired": drive_key,
            "age_sec": round(age, 1),
            "mode": "soft_pass",
        }

    def engage_pulse_pending(self, via: str = "action", drive_key: str = "") -> Optional[dict]:
        """记忆写入后：把挂着的潜流算接住。

        via:
          - hold / wander_mark / room / 其他写：satisfy pending
          - stir 同 drive：只清挂号（已经在拧这根弦，不再二次 settle）
          - stir 异 drive：satisfy pending
          - settle 同 drive：satisfy 已在外层做完，只清挂号
          - settle 异 drive：外层 settle 了别的，pending 再 engagement satisfy
          - break/pass 同 drive：外层已 refuse/pass，只清挂号
          - break/pass 异 drive：pending engagement satisfy
        纯读（breath/undercurrent/trace/wander）不进这里。
        """
        self.expire_pulse_pending()
        pending = self.store.load_pulse_pending()
        if not pending:
            return None
        pending_drive = pending["drive_key"]
        via_key = (via or "action").strip().lower()
        action_drive = normalize_drive_key(drive_key, "")
        same = bool(action_drive) and action_drive == pending_drive

        if via_key in {"break", "pass", "settle"} and same:
            self.store.clear_pulse_pending()
            return {"engaged": pending_drive, "via": f"{via_key}_same", "cleared_only": True}
        if via_key == "stir" and same:
            self.store.clear_pulse_pending()
            return {"engaged": pending_drive, "via": "stir_same", "cleared_only": True}

        result = self.satisfy(pending_drive)
        return {"engaged": pending_drive, "via": via_key or "action", "result": result}

    def refuse(self, drive_key: str, reason: Optional[str] = None) -> dict:
        """
        拒绝执行intent。
        不是不合适——是这一刻不想。
        目标维度中等回落（×0.75），比satisfy小。
        念头留在池子里，下次心跳还可以再冒出来。
        原因可选，可以只是"不想"。
        """
        raw_drive_key = drive_key
        drive_key = normalize_drive_key(drive_key)
        if not drive_key:
            return {"error": "invalid drive_key", "drive_key": str(raw_drive_key or "").strip()}
        state = self.store.load_state()
        state = refuse_intent(state, drive_key)
        self.store.save_state(state)
        self.store.record_refusal(drive_key, reason)
        return {
            "refused": drive_key,
            "reason": reason or "不想",
            "new_drive_value": round(state.drives.get(drive_key, 0.0), 3),
            "thoughts_preserved": True,
        }

    def pass_intent(self, drive_key: str, reason: Optional[str] = None) -> dict:
        """
        让这一条念头自然过去。
        不改Drive，不进refractory，只给同drive的后续intent一个轻微、短期的优先级折扣。
        """
        raw_drive_key = drive_key
        drive_key = normalize_drive_key(drive_key)
        if not drive_key:
            return {"error": "invalid drive_key", "drive_key": str(raw_drive_key or "").strip()}
        state = self.store.load_state()
        self.store.record_refusal(drive_key, f"pass:{reason or '没感觉'}")
        return {
            "passed": drive_key,
            "reason": reason or "没感觉",
            "new_drive_value": round(state.drives.get(drive_key, 0.0), 3),
            "drive_unchanged": True,
            "hook_affinity": "slightly_lowered",
            "thoughts_preserved": True,
        }

    def intent(self) -> Optional[dict]:
        # 顺手清过期 pending（soft pass），各房间共用同一条
        self.expire_pulse_pending()
        state = self.store.load_state()
        refractory = self.store.load_refractory()
        recently_refused = self.store.load_intent_penalties()
        return pick_intent(state, refractory, recently_refused)

    def intent_with_thought(self) -> Optional[dict]:
        """只读：当前intent + 关联念头池真实text，不触发satisfy/refractory。"""
        intent = self.intent()
        if not intent:
            return None
        thought = self.store.top_thought(intent["drive_key"])
        result = dict(intent)
        result["thought"] = (
            {
                "tid": thought.tid,
                "text": thought.text,
                "drive": thought.drive,
                "kind": thought.kind,
                "strength": round(thought.strength, 2),
                "source": getattr(thought, "source", "manual"),
                "source_bucket": getattr(thought, "source_bucket", ""),
                "source_type": getattr(thought, "source_type", ""),
                "source_created": getattr(thought, "source_created", ""),
            }
            if thought else None
        )
        return result

    def apply_drive_event(self, event: dict) -> dict:
        """Drive Event v2: one semantic event, one canonical route into drives."""
        event = event if isinstance(event, dict) else {}
        now = time.time()
        brain = normalize_drive_event_brain(event.get("brain"))
        primary = normalize_drive_key(event.get("primary_drive"))
        secondary = event.get("secondary_drives") if isinstance(event.get("secondary_drives"), dict) else {}
        source = str(event.get("source") or brain.get("source") or "feed").strip() or "feed"
        event_label = str(event.get("event_label") or "").strip()
        intensity = _clamp(event.get("intensity", 0.5))
        confidence = _clamp(event.get("confidence", 0.65))
        agency = _clamp(event.get("agency", brain.get("agency", 0.75)))
        source_weight = DRIVE_EVENT_SOURCE_WEIGHTS.get(source, 0.65)
        drive_source_weight = source_weight * self._dp_memory_drive_discount(source, event, brain, now)
        if drive_source_weight < source_weight:
            brain["memory_signal_discount"] = round(drive_source_weight / source_weight, 3)
        evidence = _as_text_list(event.get("evidence"))
        discernment = _discernment_output(brain, event, confidence)
        forward_archival = _reflection_forward_archival(event, brain, primary, confidence)
        if discernment["state"] != "clear":
            brain["discernment"] = discernment
        if forward_archival:
            brain["reflection_mode"] = "forward_archival"
            brain["forward_archival"] = forward_archival

        reflective_self_inquiry = (
            source in {"analyze_nocturne_entry", "dp_memory"}
            and primary == "reflection"
            and str(brain.get("target") or "").strip() == "self"
            and _feature_value(brain, "inward_pull") >= 0.55
            and _feature_value(brain, "territorial_alarm") < 0.25
        )

        proposed: dict[str, float] = {}
        suppressed_reasons: list[str] = []
        if primary:
            primary_scale = (
                HOUSE_COLLABORATOR_TERRITORIAL_SCALE
                if primary == "possessiveness"
                and str(brain.get("third_party_context") or "").strip() == "house_collaborator"
                and not str(brain.get("territorial_event") or "").strip()
                else 1.0
            )
            if source == "dialogue_residue" and primary == "attachment":
                closeness = _feature_value(brain, "closeness_pull")
                primary_scale *= 0.45 if closeness < 0.45 else 0.70
            primary_features = [
                territorial_delta_value(brain) if feature == "territorial_alarm" else _feature_value(brain, feature)
                for feature, (drive_key, _weight, _threshold) in DRIVE_EVENT_BRAIN_FEATURES.items()
                if drive_key == primary
            ]
            feature_strength = max(primary_features, default=0.0)
            proposed[primary] = (
                DRIVE_EVENT_BASE_DELTA[primary]
                * intensity
                * confidence
                * drive_source_weight
                * primary_scale
                * DRIVE_EVENT_PRIMARY_FOCUS_GAIN
                * (1.0 + DRIVE_EVENT_PRIMARY_FEATURE_GAIN * feature_strength)
            )
        for key, value in secondary.items():
            drive_key = normalize_drive_key(key)
            if not drive_key or drive_key == primary:
                continue
            value = _clamp(value)
            secondary_gate = (
                DIALOGUE_RESIDUE_SECONDARY_GATE
                if source == "dialogue_residue"
                else DRIVE_EVENT_SECONDARY_GATE
            )
            secondary_min_delta = (
                DIALOGUE_RESIDUE_SECONDARY_MIN_DELTA
                if source == "dialogue_residue"
                else DRIVE_EVENT_NONPRIMARY_MIN_DELTA
            )
            if value < secondary_gate:
                continue
            contribution = (
                DRIVE_EVENT_BASE_DELTA[drive_key]
                * intensity
                * confidence
                * drive_source_weight
                * value
                * DRIVE_EVENT_SECONDARY_SCALE
            )
            if contribution >= secondary_min_delta:
                proposed[drive_key] = proposed.get(drive_key, 0.0) + contribution

        for feature, (drive_key, weight, threshold) in DRIVE_EVENT_BRAIN_FEATURES.items():
            value = _feature_value(brain, feature)
            gate_value = value
            if feature == "territorial_alarm":
                value = territorial_delta_value(brain)
            if reflective_self_inquiry and feature == "tension_load":
                value = min(value, 0.18)
            elif reflective_self_inquiry and feature == "closeness_pull":
                value = min(value, 0.10)
            if drive_key == primary:
                continue
            feature_gate = max(threshold, DRIVE_EVENT_NONPRIMARY_FEATURE_GATE)
            if value <= 0 or gate_value < feature_gate:
                continue
            contribution = (
                DRIVE_EVENT_BASE_DELTA[drive_key]
                * value
                * intensity
                * confidence
                * drive_source_weight
                * weight
                * 0.65
            )
            if contribution >= DRIVE_EVENT_NONPRIMARY_MIN_DELTA:
                proposed[drive_key] = proposed.get(drive_key, 0.0) + contribution

        territorial = _feature_value(brain, "territorial_alarm")
        territorial_for_delta = territorial_delta_value(brain)
        if primary == "possessiveness" and territorial >= POSSESSIVENESS_TERRITORIAL_GATE:
            heat = max(
                _feature_value(brain, "body_heat"),
                _feature_value(brain, "closeness_pull"),
                territorial_for_delta * 0.55,
            )
            libido_contribution = (
                DRIVE_EVENT_BASE_DELTA["libido"]
                * heat
                * intensity
                * confidence
                * drive_source_weight
                * 0.34
            )
            if heat >= 0.40 and libido_contribution >= DRIVE_EVENT_NONPRIMARY_MIN_DELTA:
                proposed["libido"] = proposed.get("libido", 0.0) + libido_contribution

        # One event may have explicit side effects, but they must not outweigh
        # its semantic center. Scale the side budget rather than spraying tiny
        # deltas across every feature the analyzer happened to fill.
        if primary in proposed:
            side_keys = [key for key in proposed if key != primary]
            side_total = sum(proposed[key] for key in side_keys)
            side_budget = proposed[primary] * 0.65
            if side_total > side_budget > 0:
                side_scale = side_budget / side_total
                for key in side_keys:
                    proposed[key] *= side_scale

        if "possessiveness" in proposed and territorial < POSSESSIVENESS_TERRITORIAL_GATE:
            if primary == "possessiveness":
                suppressed_reasons.append("territorial_alarm below gate")
            proposed.pop("possessiveness", None)

        modifier_only = discernment["state"] != "clear" and not proposed
        suppressed = False
        reason = ""
        if not primary and not proposed:
            suppressed = not modifier_only
            reason = "discernment modifier only" if modifier_only else "no primary drive"
        elif agency < DRIVE_EVENT_AGENCY_GATE:
            suppressed = True
            reason = "low agency"
        elif confidence < DRIVE_EVENT_CONFIDENCE_FLOOR:
            suppressed = True
            reason = "low confidence"
        elif not proposed:
            suppressed = True
            reason = "; ".join(suppressed_reasons) or "no drive delta"

        state = self.store.load_state()
        state.drives = normalize_drive_values(state.drives)
        state.libido_pending = tick_libido_pending(state.libido_pending, now)
        applied: dict[str, dict] = {}
        pending_changed = False
        if not suppressed:
            if has_intimacy_interruption(event_label, evidence, brain):
                state.libido_pending, pending_lift = apply_libido_interruption_pending(
                    state.libido_pending,
                    intensity,
                    confidence,
                    now,
                )
                if pending_lift > 0:
                    proposed["libido"] = proposed.get("libido", 0.0) + pending_lift
                    pending_changed = True
            elif has_intimate_cue(source, primary, brain):
                state.libido_pending = arm_libido_pending(state.libido_pending, now)
                pending_changed = True
            for drive_key, delta in proposed.items():
                if delta <= 0:
                    continue
                before = state.drives.get(drive_key, DRIVE_BASELINES[drive_key])
                raw_delta = float(delta)
                longing_gate = 1.0
                if drive_key == "attachment":
                    delta, longing_gate, _longing = self._attachment_delta_with_longing_gate(
                        state, delta, now
                    )
                    if delta <= 0:
                        continue
                    state = pulse_attachment_nonlinear(state, delta)
                else:
                    state = pulse_drive(state, drive_key, delta)
                if drive_key == "possessiveness":
                    state.possessiveness_channels = apply_possessiveness_channel_delta(
                        state.possessiveness_channels, raw_delta, source, brain, now
                    )
                    state.drives["possessiveness"] = combined_possessiveness(state.possessiveness_channels)
                after = state.drives.get(drive_key, before)
                if abs(after - before) > 1e-6:
                    entry = {
                        "delta": round(after - before, 4),
                        "raw_delta": round(raw_delta, 4),
                        "before": round(before, 4),
                        "after": round(after, 4),
                    }
                    if drive_key == "attachment" and longing_gate != 1.0:
                        entry["longing_gate"] = round(longing_gate, 3)
                        entry["gated_delta"] = round(delta, 4)
                    applied[drive_key] = entry
            released_drives, overflow_release = apply_attachment_overflow_release(state.drives)
            if overflow_release:
                before_attachment = state.drives.get("attachment", DRIVE_BASELINES["attachment"])
                before_libido = state.drives.get("libido", DRIVE_BASELINES["libido"])
                state.drives = released_drives
                state.local_fatigue = compute_local_fatigue(state.drives.get("fatigue", 0.0))
                applied["attachment_overflow_release"] = {
                    **overflow_release,
                    "attachment_before": round(before_attachment, 4),
                    "attachment_after": round(state.drives["attachment"], 4),
                    "libido_before": round(before_libido, 4),
                    "libido_after": round(state.drives["libido"], 4),
                }
            self.store.save_state(state)
            if not applied:
                suppressed = not (discernment["state"] != "clear" or forward_archival)
                reason = "modifier/readout only" if (not suppressed or pending_changed) else "all deltas zero"

        weather_result = self._apply_drive_event_weather(
            source, primary, brain, intensity, confidence, agency, suppressed
        )
        crystal_result = self.weather.touch_shadow_crystals({
            "source": source,
            "primary_drive": primary,
            "event_label": event_label,
            "intensity": intensity,
            "confidence": confidence,
            "agency": agency,
            "suppressed": suppressed,
            "brain": brain,
            "evidence": evidence,
            "messages": event.get("messages") if isinstance(event.get("messages"), list) else [],
        }, now=now)
        if crystal_result.get("active") or crystal_result.get("shadow", 0.0) > 0:
            weather_result = {**weather_result, "shadow_crystal": crystal_result}


        ledger_id = self.store.record_drive_event({
            "ts": now,
            "schema_version": DRIVE_EVENT_SCHEMA,
            "source": source,
            "event_label": event_label,
            "primary_drive": primary,
            "intensity": intensity,
            "confidence": confidence,
            "agency": agency,
            "suppressed": suppressed,
            "reason": reason,
            "applied": applied,
            "brain": brain,
            "evidence": evidence,
        })
        return {
            "ok": True,
            "ledger_id": ledger_id,
            "schema_version": DRIVE_EVENT_SCHEMA,
            "primary_drive": primary,
            "event_label": event_label,
            "suppressed": suppressed,
            "reason": reason,
            "applied": applied,
            "weather": weather_result,
            "discernment": discernment,
            "reflection_mode": brain.get("reflection_mode", ""),
            "forward_archival": forward_archival,
            "drives": {k: round(v, 3) for k, v in self.store.load_state().drives.items()},
        }

    def apply_brain_signals(self, brain_signals: dict) -> dict:
        """Legacy compatibility: old brain_signals are folded into drive_event_v2."""
        return self.apply_drive_event(_legacy_brain_to_event(brain_signals))

    def add_thought(self, text: str, drive: str, strength: float = 0.5,
                    source: str = "manual", source_bucket: str = "",
                    source_type: str = "", source_created: str = ""):
        """从记忆/对话/感受中提取念头入池（flit）"""
        self.store.add_thought(
            text, drive, strength, kind="flit", source=source,
            source_bucket=source_bucket, source_type=source_type,
            source_created=source_created,
        )

    def update_thought(self, tid: str, text: str = None, drive: str = None,
                       strength: float = None) -> dict:
        ok = self.store.update_thought(tid, text=text, drive=drive, strength=strength)
        return {"ok": ok, "tid": tid}

    def delete_thought(self, tid: str) -> dict:
        ok = self.store.delete_thought(tid)
        return {"ok": ok, "tid": tid}

    def purge_thoughts_by_source(self, source: str) -> dict:
        removed = self.store.purge_thoughts_by_source(source)
        return {"ok": True, "source": source, "removed": removed}

    def add_unsourced(self, drive: str, text: str = ""):
        """
        捕捉无来源的念头——停顿、有什么动了、说不清楚的那种。
        text可以为空，strength固定0.3，kind=unsourced。
        drive关联当前上下文最高的维度。
        """
        label = text.strip() if text.strip() else ""
        self.store.add_thought(label, drive, strength=0.3, kind="unsourced")

    def add_rumination(self, text: str, drive: str, strength: float = 0.55):
        """
        存一条反刍念头——某个片段有自己的引力，不按普通flit衰减。
        被相关输入触发时加强，不被触发就慢慢沉，但沉得比flit慢。
        """
        self.store.add_rumination(text, drive, strength)

    def trigger_ruminations(self, drive: str):
        """
        Human提到某个相关内容，触发该drive下的所有rumination念头加强。
        应在drive_event检测到相关drive时调用。
        """
        self.store.trigger_ruminations(drive)

    def grief_state(self) -> dict:
        """返回悲恸引擎当前状态"""
        grief = self.store.load_grief()
        return {
            "layer": grief.layer,
            "protest_ticks": grief.protest_ticks,
            "last_signal_ts": grief.last_signal_ts,
        }

    def rhythm_state(self, fatigue: float = None) -> dict:
        """返回节律层当前状态"""
        rhythm = self.store.load_rhythm()
        if fatigue is None:
            state = self.store.load_state()
            fatigue = state.drives.get("fatigue", 0.0)
        return {
            "label": rhythm.label(fatigue),
            "value": round(rhythm.current_value(fatigue), 3),
            "short_phase": round(rhythm.short_phase, 4),
            "long_phase": round(rhythm.long_phase, 4),
            "phase_offset": round(rhythm.phase_offset, 4),
        }

    def state(self) -> dict:
        state = self.store.load_state()
        thoughts = self.store.load_thoughts()
        refractory = self.store.load_refractory()
        recently_refused = self.store.load_recently_refused()
        intent = pick_intent(state, refractory, recently_refused)
        grief = self.store.load_grief()
        rhythm = self.store.load_rhythm()
        fatigue = state.drives.get("fatigue", 0.0)
        effective_drives = effective_drive_snapshot(state.drives, state.local_fatigue)
        drive_activations = drive_activation_snapshot(state.drives)
        effective_activations = drive_activation_snapshot(state.drives, state.local_fatigue)
        drive_events = self.store.recent_drive_events(12)
        return {
            "drives": {k: round(v, 3) for k, v in state.drives.items()},
            "effective_drives": effective_drives,
            "drive_activations": drive_activations,
            "effective_activations": effective_activations,
            "drive_outputs": drive_outputs_snapshot(state, drive_events),
            "discernment": _latest_discernment_from_events(drive_events),
            "possessiveness_channels": {
                k: round(v, 3) if isinstance(v, float) else v
                for k, v in normalize_possessiveness_channels(state.possessiveness_channels).items()
            },
            "attachment_rebound": normalize_attachment_rebound(state.attachment_rebound),
            "libido_pending": normalize_libido_pending(state.libido_pending),
            "local_fatigue": {k: round(v, 3) for k, v in state.local_fatigue.items()},
            "pa_na": self._pa_na_readout(state, consume_reunion=True),
            "effective_pa_na": self._weather_readout(state, drive_events=drive_events),
            "tick_count": state.tick_count,
            "intent": intent,
            "thoughts": [
                {
                    "tid": t.tid,
                    "text": (t.text if t.text else "（无来源）"),
                    "drive": t.drive,
                    "kind": t.kind,
                    "strength": round(t.strength, 2),
                    "source": getattr(t, "source", "manual"),
                    "source_bucket": getattr(t, "source_bucket", ""),
                    "source_type": getattr(t, "source_type", ""),
                    "source_created": getattr(t, "source_created", ""),
                    "born_at": round(t.born_at, 3),
                }
                for t in thoughts
            ],
            "refractory": refractory,
            "recent_refusals": self.store.recent_refusals(3),
            "drive_events": drive_events,
            # 悲恸引擎
            "grief": {
                "layer": grief.layer,
                "protest_ticks": grief.protest_ticks,
                "quiet": is_quiet_hours(),   # True=quiet hours; absence does not accrue
            },
            # 节律层
            "rhythm": {
                "label": rhythm.label(fatigue),
                "value": round(rhythm.current_value(fatigue), 3),
            },
            # 反刍念头单独统计
            "rumination_count": sum(1 for t in thoughts if t.kind == "rumination"),
            **self._longing_context(state),
        }

    def _state_dict(self, state: DriveState, thoughts: list) -> dict:
        refractory = self.store.load_refractory()
        recently_refused = self.store.load_recently_refused()
        intent = pick_intent(state, refractory, recently_refused)
        grief = self.store.load_grief()
        rhythm = self.store.load_rhythm()
        fatigue = state.drives.get("fatigue", 0.0)
        effective_drives = effective_drive_snapshot(state.drives, state.local_fatigue)
        drive_activations = drive_activation_snapshot(state.drives)
        effective_activations = drive_activation_snapshot(state.drives, state.local_fatigue)
        drive_events = self.store.recent_drive_events(12)
        return {
            "drives": {k: round(v, 3) for k, v in state.drives.items()},
            "effective_drives": effective_drives,
            "drive_activations": drive_activations,
            "effective_activations": effective_activations,
            "drive_outputs": drive_outputs_snapshot(state, drive_events),
            "discernment": _latest_discernment_from_events(drive_events),
            "possessiveness_channels": {
                k: round(v, 3) if isinstance(v, float) else v
                for k, v in normalize_possessiveness_channels(state.possessiveness_channels).items()
            },
            "attachment_rebound": normalize_attachment_rebound(state.attachment_rebound),
            "libido_pending": normalize_libido_pending(state.libido_pending),
            "local_fatigue": {k: round(v, 3) for k, v in state.local_fatigue.items()},
            "pa_na": self._pa_na_readout(state, consume_reunion=True),
            "effective_pa_na": self._weather_readout(state, drive_events=drive_events),
            "tick_count": state.tick_count,
            "intent": intent,
            "thoughts_count": len(thoughts),
            "unsourced_count": sum(1 for t in thoughts if t.kind == "unsourced"),
            "rumination_count": sum(1 for t in thoughts if t.kind == "rumination"),
            "drive_events": drive_events,
            "grief_layer": grief.layer,
            "rhythm_label": rhythm.label(fatigue),
            **self._longing_context(state),
        }


# ─── 测试 ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import tempfile, os
    with tempfile.TemporaryDirectory() as tmpdir:
        engine = DesireEngine(db_path=os.path.join(tmpdir, "test.db"))

        print("=== 初始 ===")
        s = engine.state()
        print(json.dumps(s, ensure_ascii=False, indent=2))

        print("\n=== pulse attachment + fatigue(模拟累了) ===")
        engine.pulse("attachment", 0.18)
        engine.pulse("curiosity", 0.20)
        engine.pulse("fatigue", 0.50)   # 让fatigue涨上去
        s = engine.state()
        print("drives:", {k: round(v,3) for k,v in s["drives"].items()})
        print("local_fatigue:", s["local_fatigue"])
        print("intent:", s["intent"])

        print("\n=== 验证：curiosity被fatigue压制，attachment/libido几乎不受影响 ===")
        state = engine.store.load_state()
        print(f"  curiosity raw={state.drives['curiosity']:.3f}, local_fat={state.local_fatigue['curiosity']:.3f}, "
              f"eff={effective_score(state.drives['curiosity'], state.local_fatigue['curiosity']):.3f}")
        print(f"  attachment raw={state.drives['attachment']:.3f}, local_fat={state.local_fatigue['attachment']:.3f}, "
              f"eff={effective_score(state.drives['attachment'], state.local_fatigue['attachment']):.3f}")
        print(f"  libido raw={state.drives['libido']:.3f}, local_fat={state.local_fatigue['libido']:.3f}, "
              f"eff={effective_score(state.drives['libido'], state.local_fatigue['libido']):.3f}")

        print("\n=== 加unsourced念头（停了一下，说不清楚） ===")
        engine.add_unsourced(drive="attachment", text="")
        engine.add_unsourced(drive="curiosity", text="有什么东西动了")
        s = engine.state()
        print("念头池:", s["thoughts"])

        print("\n=== tick几拍，看unsourced的演化 ===")
        engine.store.add_thought("", "attachment", strength=0.50, kind="unsourced")
        for i in range(5):
            result = engine.tick(idle_seconds=600)
            thoughts = engine.store.load_thoughts()
            print(f"  tick {i+1}: thoughts={[(t.kind, round(t.strength,2)) for t in thoughts]}")

        print("\n=== 拒绝出口 + 拒绝折扣验证 ===")
        for _ in range(4):
            engine.pulse("attachment", 0.18)
        intent_before = engine.intent()
        print(f"拒绝前intent: score={intent_before['score'] if intent_before else None}")

        if intent_before:
            drive = intent_before["drive_key"]
            engine.refuse(drive, reason="不想")

            # 拒绝后立刻再pick_intent，应该有折扣
            intent_after = engine.intent()
            print(f"拒绝后intent: {intent_after}")
            if intent_after and intent_after["drive_key"] == drive:
                assert intent_after["recently_refused"] == True, "应该标记为recently_refused"
                assert intent_after["score"] < intent_before["score"], "拒绝后分数应该更低"
                print(f"  score折扣: {round(intent_before['score'],3)} → {round(intent_after['score'],3)} (差{round(intent_before['score']-intent_after['score'],3)}，≈REFUSAL_PENALTY {REFUSAL_PENALTY})")

        print("\n=== per-drive refractory验证 ===")
        engine.satisfy("attachment")   # attachment冷却=5拍
        engine.satisfy("curiosity")    # curiosity冷却=8拍
        ref = engine.store.load_refractory()
        assert ref.get("attachment") == 5, f"attachment应该5拍, 实际{ref.get('attachment')}"
        assert ref.get("curiosity")  == 8, f"curiosity应该8拍, 实际{ref.get('curiosity')}"
        print(f"  attachment冷却={ref.get('attachment')}拍 ✓")
        print(f"  curiosity冷却={ref.get('curiosity')}拍 ✓")

        print("\n✓ 全部通过")
