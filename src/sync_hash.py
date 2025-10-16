import os, json, hashlib

STATE = ".index_state.json"

def sha(t): return hashlib.sha256(t.encode("utf-8")).hexdigest()

def filter_changed(docs):
    state = json.load(open(STATE)) if os.path.exists(STATE) else {}
    changed = []
    for d in docs:
        h = sha(d["texto"])
        if state.get(d["id"]) != h:
            state[d["id"]] = h
            changed.append(d)
    json.dump(state, open(STATE, "w"), ensure_ascii=False, indent=2)
    return changed
