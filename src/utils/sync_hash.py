# src/utils/sync_hash.py
import os, json, hashlib
from pathlib import Path

def _sha(t: str) -> str:
    return hashlib.sha256(t.encode("utf-8")).hexdigest()

def filter_changed(docs: list[dict], state_path: Path | None = None) -> list[dict]:
    state_path = state_path or Path("data/4-sync/.index_state.json")
    state = json.load(open(state_path)) if state_path.exists() else {}
    changed = []
    for d in docs:
        h = _sha(d["texto"])
        if state.get(d["id"]) != h:
            state[d["id"]] = h
            changed.append(d)
    json.dump(state, open(state_path, "w"), ensure_ascii=False, indent=2)
    return changed
