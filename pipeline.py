#!/usr/bin/env python3
import os
from dotenv import load_dotenv

load_dotenv()

def _skip(name: str) -> bool:
    return os.getenv(name, "").lower() in ("1", "true", "yes")

# dataset stage
from src.pipeline.ingest.extract import ingest_pdfs
from src.pipeline.preprocess.clean_chunk import preprocess_one
from src.pipeline.preprocess.merge import merge_processed_to_dataset

# meili stage
from src.pipeline.index.meili_ops import (
    ensure_index, load_docs, configure_settings,
    configure_embedder, wait_for_embeddings_ready, sanity_queries
)

def main():
    print("🚀 Starting pipeline\n")

    # ---------- [1] Build dataset ----------
    if _skip("SKIP_DATASET"):
        print("⏭️  Skip dataset stage (SKIP_DATASET=true)")
    else:
        print("[1/3] 🧱 Ingest (PDF → page texts)")
        triples = ingest_pdfs()

        print("[2/3] 🧼 Preprocess (clean + chunk → JSON per PDF)")
        for base_id, titulo, pages in triples:
            preprocess_one(base_id, titulo, pages)

        print("[3/3] 🧩 Merge (processed → data/3-out/dataset_all.json)")
        merged = merge_processed_to_dataset()
        print(f"    → Merged file: {merged}")

    # ---------- [2] Ensure index ----------
    if _skip("SKIP_INDEX"):
        print("⏭️  Skip index creation (SKIP_INDEX=true)")
    else:
        print("[2] 🏁 Ensuring Meilisearch index...")
        ensure_index()

    # ---------- [3] Upsert docs ----------
    if _skip("SKIP_UPSERT"):
        print("⏭️  Skip upsert (SKIP_UPSERT=true)")
    else:
        print("[3] ⤴️ Upserting changed/new documents...")
        total = load_docs("data/3-out/dataset_all.json")
        print(f"    → {total} documents upserted")

    # ---------- [4] Settings + Embeddings ----------
    if _skip("SKIP_SETTINGS"):
        print("⏭️  Skip settings (SKIP_SETTINGS=true)")
    else:
        print("[4.1] ⚙️ Applying Meili settings...")
        configure_settings()

    if _skip("SKIP_EMBEDDINGS"):
        print("⏭️  Skip embedder & wait (SKIP_EMBEDDINGS=true)")
    else:
        print("[4.2] 🧠 Configuring embedder & waiting...")
        configure_embedder()
        wait_for_embeddings_ready()

    # ---------- [5] Sanity ----------
    if _skip("SKIP_SANITY"):
        print("⏭️  Skip sanity queries (SKIP_SANITY=true)")
    else:
        print("[5] ✅ Sanity queries")
        sanity_queries()

    print("\n🎉 Pipeline finished.")

if __name__ == "__main__":
    main()
