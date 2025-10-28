# src/pipeline/index/meili_ops.py
import os, json, time, requests, meilisearch
from pathlib import Path
from dotenv import load_dotenv
from src.utils.sync_hash import filter_changed

load_dotenv()

MEILI_URL = os.getenv("MEILI_URL", "http://127.0.0.1:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY", "")
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")
INDEX_UID = "documentos"

client = meilisearch.Client(MEILI_URL, MEILI_KEY)
index = client.index(INDEX_UID)

def wait_task(uid):
    while True:
        task = client.get_task(uid)
        status = task.status
        if status in ("succeeded", "failed", "canceled"):
            if status != "succeeded":
                raise RuntimeError(f"Tarefa {uid} falhou: {task}")
            return
        time.sleep(0.3)

def ensure_index():
    try:
        client.create_index(INDEX_UID, {"primaryKey": "id"})
        print(f"🆕 Índice '{INDEX_UID}' criado.")
    except meilisearch.errors.MeilisearchApiError:
        print(f"ℹ️ Índice '{INDEX_UID}' já existe.")

def load_docs(path="data/3-out/dataset_all.json", batch_size=1000):
    with open(path, "r", encoding="utf-8") as f:
        docs = json.load(f)

    # Only changed/new
    state_file = Path("data/4-sync/.index_state.json")  # << moved here
    docs = filter_changed(docs, state_path=state_file)

    if not docs:
        print("ℹ️ Nenhum documento novo ou alterado. Pulando indexação.")
        return 0

    total = len(docs)
    print(f"📦 Iniciando upsert de {total} documentos (novos/alterados)...")
    for i in range(0, total, batch_size):
        batch = docs[i:i + batch_size]
        task = index.add_documents(batch)
        wait_task(task.task_uid)
        print(f"  → Lote {i//batch_size + 1}: {len(batch)} docs")
    return total

def configure_settings():
    print("⚙️  Configurando atributos e sinônimos...")
    task = index.update_filterable_attributes(["documento","pagina","titulo","length"]); wait_task(task.task_uid)
    task = index.update_searchable_attributes(["texto","titulo"]); wait_task(task.task_uid)
    task = index.update_displayed_attributes(["titulo","documento","pagina","texto"]); wait_task(task.task_uid)
    task = index.update_ranking_rules(["words","typo","proximity","attribute","exactness","sort","length:desc"]); wait_task(task.task_uid)
    synonyms = {
        "comunista": ["subversivo", "infiltrado", "agitador"],
        "unb": ["universidade de brasilia", "unb"],
        "estudantil": ["movimento estudantil", "alunos", "greve"],
        "segurança": ["dsi", "asi", "informações", "espionagem"],
        "documento": ["informe", "relatório", "memorando"],
        "mec": ["ministerio da educacao", "educacao e cultura"]
    }
    task = index.update_synonyms(synonyms); wait_task(task.task_uid)
    print("✅ Configuração aplicada.")

def configure_embedder():
    if not OPENAI_KEY:
        print("⚠️  OPENAI_API_KEY não definido. Pulando embedder.")
        return
    print("🧠 Verificando embedder existente...")
    r = requests.get(f"{MEILI_URL}/indexes/{INDEX_UID}/settings/embedders",
                     headers={"Authorization": f"Bearer {MEILI_KEY}"}, timeout=30)
    r.raise_for_status()
    embedders = r.json()
    if "documentos-openai" in embedders:
        print("ℹ️  Embedder já configurado. Pulando.")
        return
    payload = {
        "documentos-openai": {
            "source": "openAi",
            "apiKey": OPENAI_KEY,
            "model": "text-embedding-3-small",
            "documentTemplate": (
                "Relatório histórico. Título: '{{doc.titulo}}'. Conteúdo: '{{doc.texto}}'"
            ),
        }
    }
    r = requests.patch(f"{MEILI_URL}/indexes/{INDEX_UID}/settings/embedders",
                       headers={"Content-Type":"application/json","Authorization": f"Bearer {MEILI_KEY}"},
                       json=payload, timeout=60)
    r.raise_for_status()
    print("✅ Embedder configurado.")

def wait_for_embeddings_ready():
    print("🕒 Aguardando embeddings...")
    while True:
        r = requests.get(f"{MEILI_URL}/tasks", headers={"Authorization": f"Bearer {MEILI_KEY}"}, timeout=30)
        r.raise_for_status()
        tasks = r.json().get("results", [])
        pending = [t for t in tasks if t.get("type")=="indexEmbeddingGeneration"
                   and t.get("indexUid")==INDEX_UID
                   and t.get("status") not in ("succeeded","failed","canceled")]
        if not pending:
            print("✅ Todas as embeddings foram geradas.")
            return
        print(f"  → Aguardando {len(pending)} tarefa(s)...")
        time.sleep(5)

def sanity_queries():
    print("\n🔍 Testando consultas:")
    print("→ Full-text:")
    res1 = index.search("infiltração comunista UnB", {"limit": 3})
    for hit in res1["hits"]:
        print(f"  - {hit['titulo']} (p.{hit['pagina']})")

    print("\n→ Semântica:")
    res2 = index.search("vigilância de estudantes em Brasília",
                        {"hybrid":{"semanticRatio":0.7,"embedder":"documentos-openai"},"limit":1})
    for hit in res2["hits"]:
        print(f"  - {hit['titulo']} (p.{hit['pagina']})")
