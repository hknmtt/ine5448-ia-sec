#!/usr/bin/env python3
import os
import re
import json
import subprocess
from pathlib import Path

# ---------- Config ----------
PDF_DIR = Path("data/pdfs")
OUT_DIR = Path("out")

OVERLAP_CHARS = 200
MAX_PAGES = 9999
MIN_WORDS = 30
MERGED_FILE = OUT_DIR / "dataset_all.json"


# ---------- Text cleaning helpers ----------

def clean_ocr_text(text: str) -> str:
    """Remove noise but keep accents and punctuation."""
    lines = text.splitlines()
    lines = [ln for ln in lines if len(ln.strip()) > 20]
    text = " ".join(lines)
    text = re.sub(r'[^A-Za-zÀ-ÿ0-9\s,.;:!?\'"()\-–—]{3,}', ' ', text)
    text = re.sub(r'(\w+)-\s*\n\s*(\w+)', r'\1\2', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def normalize_portuguese_noise(text: str) -> str:
    """Fix common OCR typos."""
    corrections = {
        "ôe": "se", "arto": "arte", "homom": "homem",
        "arfo": "arco", "prosonça": "presença",
        "matoriais": "materiais", "ciêm": "ciem",
        "educandário": "escola", "prôf": "prof",
        "aôr": "amor", "dô": "do", "qne": "que",
    }
    for wrong, right in corrections.items():
        text = re.sub(rf"\b{wrong}\b", right, text, flags=re.IGNORECASE)
    return text


def extract_page_text(pdf_path: Path, page_num: int) -> str:
    """Extract text from a single page using pdftotext."""
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", "-f", str(page_num), "-l", str(page_num), str(pdf_path), "-"],
            capture_output=True, text=True, check=True
        )
        return result.stdout
    except subprocess.CalledProcessError:
        return ""


def generate_chunks(pages_text, overlap=0):
    """Split document text into smaller semantically-coherent chunks."""
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


# ---------- Main PDF processor ----------

def process_pdf(pdf_path: Path):
    base_id = pdf_path.stem
    titulo = base_id.replace("_", " ")

    print(f"\n→ Processing {pdf_path.name} ...")
    pages_text = []

    for page in range(1, MAX_PAGES + 1):
        text = extract_page_text(pdf_path, page)
        if not text.strip():
            break
        cleaned = clean_ocr_text(text)
        cleaned = normalize_portuguese_noise(cleaned)
        pages_text.append(cleaned)
        print(f"  Page {page:03d}: {len(cleaned.split())} words")

    if not pages_text:
        print(f"⚠️ No text extracted from {pdf_path.name}")
        return []

    chunks = generate_chunks(pages_text, overlap=OVERLAP_CHARS)
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


def merge_json_files():
    """Merge all JSONs from OUT_DIR into one file."""
    all_docs = []
    for file in sorted(OUT_DIR.glob("*.json")):
        if file.name == MERGED_FILE.name:
            continue
        with open(file, encoding="utf-8") as f:
            try:
                data = json.load(f)
                all_docs.extend(data)
            except json.JSONDecodeError:
                print(f"⚠️ Skipped invalid JSON: {file.name}")

    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        json.dump(all_docs, f, ensure_ascii=False, indent=2)
    print(f"\n📚 Merged {len(all_docs)} documents into {MERGED_FILE.name}")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {PDF_DIR}")
        return

    all_docs = []
    for pdf in pdfs:
        docs = process_pdf(pdf)
        all_docs.extend(docs)

    merge_json_files()
    print("\n🎉 Dataset ready! You can now index 'out/dataset_all.json' in Meilisearch.")


if __name__ == "__main__":
    main()
