# src/pipeline/preprocess/merge.py
import json
from pathlib import Path

PROCESSED_DIR = Path("data/2-processed")
OUT_DIR = Path("data/3-out")
MERGED_FILE = OUT_DIR / "dataset_all.json"

def merge_processed_to_dataset():
    all_docs = []
    for file in sorted(PROCESSED_DIR.glob("*.json")):
        with open(file, encoding="utf-8") as f:
            try:
                data = json.load(f)
                all_docs.extend(data)
            except json.JSONDecodeError:
                print(f"⚠️ Skipped invalid JSON: {file.name}")

    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        json.dump(all_docs, f, ensure_ascii=False, indent=2)
    print(f"\n📚 Merged {len(all_docs)} documents into {MERGED_FILE.name}")

    return MERGED_FILE
