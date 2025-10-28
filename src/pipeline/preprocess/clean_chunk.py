# src/pipeline/preprocess/clean_chunk.py
import re, json
from pathlib import Path

OUT_DIR = Path("data/2-processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OVERLAP_CHARS = 200
MIN_WORDS = 30

def clean_ocr_text(text: str) -> str:
    lines = text.splitlines()
    lines = [ln for ln in lines if len(ln.strip()) > 20]
    text = " ".join(lines)
    text = re.sub(r'[^A-Za-zÀ-ÿ0-9\s,.;:!?\'\"()\-–—]{3,}', ' ', text)
    text = re.sub(r'(\w+)-\s*\n\s*(\w+)', r'\1\2', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def generate_chunks(pages_text, overlap=0):
    chunks = []
    for i, text in enumerate(pages_text):
        if not text.strip():
            continue
        sections = re.split(r'(?<=\n)([A-ZÀ-Ý\s]{4,}(?:[:!?.-]))', text)
        buffer = ""
        for part in sections:
            if not part.strip():
                continue
            buffer += " " + part.strip()
            if len(buffer) > 800:
                combined = buffer.strip()
                if overlap and i > 0 and len(combined) < 400:
                    prev_tail = pages_text[i - 1][-overlap:]
                    combined = prev_tail + " " + combined
                chunks.append(combined)
                buffer = ""
        if buffer.strip():
            chunks.append(buffer.strip())
    return chunks

def preprocess_one(base_id: str, titulo: str, pages_text: list[str]) -> list[dict]:
    cleaned_pages = [clean_ocr_text(t) for t in pages_text]
    chunks = generate_chunks(cleaned_pages, overlap=OVERLAP_CHARS)
    seen, docs = set(), []
    for i, chunk in enumerate(chunks, start=1):
        if len(chunk.split()) < MIN_WORDS or chunk in seen:
            continue
        seen.add(chunk)
        docs.append({
            "id": f"{base_id}_p{i:03d}",
            "documento": base_id,
            "pagina": i,
            "titulo": titulo,
            "texto": chunk,
            "length": len(chunk),
        })

    out_file = OUT_DIR / f"{base_id}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)
    print(f"✅ Generated {out_file.name} with {len(docs)} records.")
    return docs
