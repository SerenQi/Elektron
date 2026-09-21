"""Pure display selection for the Latent Notes pool."""
from __future__ import annotations


def latent_notes_for_display(
    data: dict,
    *,
    status: str = "",
    limit: int = 80,
) -> list[dict]:
    """Return live notes without letting tombstones crowd out bounded lists.

    Pin toggles update ``updated_at``. Sorting by that timestamp keeps a
    just-unpinned note in the response instead of making it look deleted merely
    because it fell behind old pinned/used rows.
    """
    notes = [note for note in data.get("notes", []) if isinstance(note, dict)]
    if status:
        notes = [note for note in notes if note.get("status") == status]
    else:
        notes = [note for note in notes if note.get("status") != "deleted"]
    notes.sort(
        key=lambda note: str(note.get("updated_at") or note.get("created_at") or ""),
        reverse=True,
    )
    notes.sort(key=lambda note: not bool(note.get("pinned")))
    return notes[:max(1, limit)]
