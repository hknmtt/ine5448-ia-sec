# src/pipeline/ingest/extract.py
import subprocess
from pathlib import Path

PDF_DIR = Path("data/1-raw")

def extract_page_text(pdf_path: Path, page_num: int) -> str:
    try:
        res = subprocess.run(
            ["pdftotext", "-layout", "-f", str(page_num), "-l", str(page_num), str(pdf_path), "-"],
            capture_output=True, text=True, check=True
        )
        return res.stdout
    except subprocess.CalledProcessError:
        return ""

def ingest_pdfs(max_pages=9999):
    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {PDF_DIR}")
        return []

    docs = []  # [(base_id, titulo, [page_text1, page_text2,...])]
    for pdf in pdfs:
        base_id = pdf.stem
        titulo = base_id.replace("_", " ")
        print(f"\n→ Ingesting {pdf.name} ...")

        pages_text = []
        for page in range(1, max_pages + 1):
            text = extract_page_text(pdf, page)
            if not text.strip():
                break
            pages_text.append(text)
            # print(f"  Page {page:03d}: extracted")

        if pages_text:
            print(f"  Extracted {len(pages_text)} pages from {pdf.name}")
            docs.append((base_id, titulo, pages_text))
        else:
            print(f"⚠️ No text extracted from {pdf.name}")

    return docs
