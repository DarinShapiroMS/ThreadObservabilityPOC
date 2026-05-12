"""Tier 4: playbook corpus loader and lookup.

Loads a curated set of Thread/Matter failure-mode playbooks from a
file-backed JSON corpus and exposes a small query API used by the
``lookup_playbook`` / ``list_playbooks`` MCP tools and by the
``analyze_node`` consultant tool.

Each playbook entry has:

* ``id`` (stable identifier, machine usable)
* ``title`` (human-readable)
* ``applies_to`` (list of issue ``kind`` values this entry covers)
* ``summary`` (short description)
* ``evidence_to_collect`` (list of strings — what to gather first)
* ``remediation_steps`` (list of strings — ordered)
* ``references`` (list of strings — links or spec citations)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

# Resolved at import time. The corpus JSON is shipped as package data next
# to this module so it works equally under editable installs (dev/tests)
# and under a regular ``pip install`` inside the addon container.
_THIS_DIR = Path(__file__).resolve().parent
_DEFAULT_CORPUS = _THIS_DIR / "playbooks.json"

_cache: dict[str, Any] = {}


def _load_corpus(path: Path | None = None) -> dict[str, Any]:
    """Read the JSON corpus from disk, memoized by absolute path."""
    p = (path or _DEFAULT_CORPUS).resolve()
    key = str(p)
    if key in _cache:
        return _cache[key]
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # Defensive: ensure shape.
    if not isinstance(data, dict) or not isinstance(data.get("playbooks"), list):
        raise ValueError(f"playbook corpus malformed: {p}")
    _cache[key] = data
    return data


def reset_cache_for_tests() -> None:
    _cache.clear()


def list_playbooks(*, path: Path | None = None) -> dict[str, Any]:
    """Return all playbook summaries (id, title, applies_to)."""
    data = _load_corpus(path)
    out = [
        {
            "id": p.get("id"),
            "title": p.get("title"),
            "applies_to": list(p.get("applies_to") or []),
        }
        for p in data["playbooks"]
    ]
    return {"version": data.get("version", 1), "count": len(out), "playbooks": out}


def get_playbook(playbook_id: str, *, path: Path | None = None) -> dict[str, Any] | None:
    """Return the full playbook entry by id, or None."""
    data = _load_corpus(path)
    for p in data["playbooks"]:
        if p.get("id") == playbook_id:
            return dict(p)
    return None


def lookup_playbook(
    *,
    kind: str | None = None,
    playbook_id: str | None = None,
    query: str | None = None,
    path: Path | None = None,
) -> dict[str, Any]:
    """Return playbook entries matching the given criteria.

    Lookup priority:
    1. ``playbook_id`` — exact match, returns a list of one if found.
    2. ``kind`` — every playbook whose ``applies_to`` contains the kind.
    3. ``query`` — case-insensitive substring match against id, title,
       and summary.

    Returns ``{"matches": [...], "count": int}``. Empty matches are not
    an error.
    """
    data = _load_corpus(path)
    matches: list[dict[str, Any]] = []
    if playbook_id:
        entry = get_playbook(playbook_id, path=path)
        if entry:
            matches.append(entry)
    elif kind:
        for p in data["playbooks"]:
            if kind in (p.get("applies_to") or []):
                matches.append(dict(p))
    elif query:
        needle = query.lower()
        for p in data["playbooks"]:
            hay = " ".join(
                [
                    str(p.get("id") or ""),
                    str(p.get("title") or ""),
                    str(p.get("summary") or ""),
                ]
            ).lower()
            if needle in hay:
                matches.append(dict(p))
    return {"matches": matches, "count": len(matches)}


def lookup_for_kinds(
    kinds: Iterable[str], *, path: Path | None = None
) -> list[dict[str, Any]]:
    """Helper used by ``analyze_node``: collect unique playbooks
    matching any of the supplied issue kinds, preserving corpus order
    and de-duplicating by playbook id.
    """
    data = _load_corpus(path)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    kind_set = set(kinds)
    for p in data["playbooks"]:
        pid = p.get("id")
        if pid in seen:
            continue
        applies = set(p.get("applies_to") or [])
        if applies & kind_set:
            seen.add(pid)
            out.append(dict(p))
    return out
