"""Configurable identity labels for the public memory core.

The private household deployment has its own persona and relationship layer.
That layer is intentionally absent here. Operators may provide a short
first-person persona and display names through environment variables.
"""

from __future__ import annotations

import os


AGENT_NAME = os.environ.get("OMBRE_AGENT_NAME", "Agent").strip() or "Agent"
HUMAN_NAME = os.environ.get("OMBRE_HUMAN_NAME", "Human").strip() or "Human"
AGENT_PERSONA = (
    os.environ.get("OMBRE_AGENT_PERSONA", "").strip()
    or (
        f"You are {AGENT_NAME}, an AI using a private long-term memory system. "
        "Write in first person, preserve uncertainty, and do not invent "
        "relationship history absent from the supplied evidence."
    )
)


def pair_label() -> str:
    return f"{HUMAN_NAME} and {AGENT_NAME}"
