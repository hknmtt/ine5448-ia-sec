#!/usr/bin/env python3
import os, json, time
import requests
import meilisearch
from sync_hash import filter_changed
from dotenv import load_dotenv
load_dotenv()

MEILI_URL = os.getenv("MEILI_URL", "http://127.0.0.1:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY", "")
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")

INDEX_UID = "documentos"

client = meilisearch.Client(MEILI_URL, MEILI_KEY)
index = client.index(INDEX_UID)

# ---------------- Helpers ---------------- #

def wait_task(uid):
    """Bloqueia até o término de uma task (polling leve)."""
    while True:
        task = client.get_task(uid)
        status = task.status
        if status in ("succeeded", "failed", "canceled"):
            if status != "succeeded":
                raise RuntimeError(f"Tarefa {uid} falhou: {task}")
            return
        time.sleep(0.3)

def ensure_index():
    """Garante que o índice existe (sem erro se já existir)."""
    try:
        client.create_index(INDEX_UID, {"primaryKey": "id"})
        print(f"🆕 Índice '{INDEX_UID}' criado.")
    except meilisearch.errors.MeilisearchApiError:
        print(f"ℹ️ Índice '{INDEX_UID}' já existe.")

# ---------------- Pipeline ---------------- #

def load_docs(path="out/dataset_all.json", batch_size=1000):
    """Carrega e insere apenas documentos novos ou alterados."""
    with open(path, "r", encoding="utf-8") as f:
        docs = json.load(f)

    # 👇 Only keep changed/new docs
    docs = filter_changed(docs)

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
    """Define campos filtráveis, sinônimos e ajustes de ranking para documentos históricos."""
    print("⚙️  Configurando atributos e sinônimos...")

    # Campos úteis para filtro ou facetas
    task = index.update_filterable_attributes([
        "documento", "pagina", "titulo", "length"
    ])
    wait_task(task.task_uid)

    # Campos visíveis em resultados
    task = index.update_searchable_attributes([
        "texto", "titulo"
    ])
    wait_task(task.task_uid)

    # Campos que aparecem como snippet nos resultados
    task = index.update_displayed_attributes([
        "titulo", "documento", "pagina", "texto"
    ])
    wait_task(task.task_uid)

    # Ranking personalizado: dar mais peso para trechos longos e para o título
    task = index.update_ranking_rules([
        "words",
        "typo",
        "proximity",
        "attribute",
        "exactness",
        "sort",
        "length:desc"
    ])
    wait_task(task.task_uid)

    # Sinônimos úteis para o contexto de segurança, censura e regime militar
    synonyms = {
        "comunista": ["subversivo", "infiltrado", "agitador"],
        "unb": ["universidade de brasilia", "unb"],
        "estudantil": ["movimento estudantil", "alunos", "greve"],
        "segurança": ["dsi", "asi", "informações", "espionagem"],
        "documento": ["informe", "relatório", "memorando"],
        "mec": ["ministerio da educacao", "educacao e cultura"]
    }
    task = index.update_synonyms(synonyms)
    wait_task(task.task_uid)

    print("✅ Configuração básica e sinônimos aplicados.")

def configure_embedder():
    """Configura busca semântica via OpenAI (via REST API, idempotente)."""
    if not OPENAI_KEY:
        print("⚠️  OPENAI_API_KEY não definido. Pulando embedder.")
        return

    print("🧠 Verificando embedder existente...")

    # Step 1: check current embedders
    resp = requests.get(
        f"{MEILI_URL}/indexes/{INDEX_UID}/settings/embedders",
        headers={"Authorization": f"Bearer {MEILI_KEY}"},
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Erro ao consultar embedders: {resp.status_code} {resp.text}")

    embedders = resp.json()
    if "documentos-openai" in embedders:
        print("ℹ️  Embedder já configurado. Pulando recriação.")
        return

    # Step 2: create only if missing
    print("🧠 Configurando embedder OpenAI (contexto histórico/político)...")

    payload = {
        "documentos-openai": {
            "source": "openAi",
            "apiKey": OPENAI_KEY,
            "model": "text-embedding-3-small",
            "documentTemplate": (
                "Relatório histórico de segurança nacional ou universitária. "
                "Título: '{{doc.titulo}}'. "
                "Conteúdo: '{{doc.texto}}'"
            ),
        }
    }

    r = requests.patch(
        f"{MEILI_URL}/indexes/{INDEX_UID}/settings/embedders",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {MEILI_KEY}",
        },
        json=payload,
        timeout=60,
    )
    if not r.ok:
        raise RuntimeError(f"Erro configurando embedder: {r.status_code} {r.text}")

    print("✅ Embedder configurado com sucesso.")

def wait_for_embeddings_ready():
    """Waits until all embedding generation tasks finish."""
    print("🕒 Aguardando finalização das embeddings...")

    while True:
        r = requests.get(
            f"{MEILI_URL}/tasks",
            headers={"Authorization": f"Bearer {MEILI_KEY}"},
            timeout=30,
        )
        r.raise_for_status()
        tasks = r.json().get("results", [])

        # Filter embedding generation tasks
        pending = [
            t for t in tasks
            if t.get("type") == "indexEmbeddingGeneration"
            and t.get("indexUid") == INDEX_UID
            and t.get("status") not in ("succeeded", "failed", "canceled")
        ]

        if not pending:
            print("✅ Todas as embeddings foram geradas.")
            return

        # Show short status summary
        statuses = [t["status"] for t in pending]
        print(f"  → Aguardando {len(pending)} tarefa(s): {statuses}")
        time.sleep(5)

def sanity_queries():
    """Executa buscas de teste representativas."""
    print("\n🔍 Testando consultas:")
    print("→ Full-text (palavras-chave diretas):")
    res1 = index.search("infiltração comunista UnB", {"limit": 3})
    for hit in res1["hits"]:
        print(f"  - {hit['titulo']} (p.{hit['pagina']})")

    print("\n→ Semântica (busca contextual OpenAI):")
    res2 = index.search("documentos sobre vigilância de estudantes em Brasília",
        {
            "hybrid": {"semanticRatio": 0.7, "embedder": "documentos-openai"},
            "limit": 1
        }
    )
    for hit in res2["hits"]:
        print(f"  - {hit['titulo']} (p.{hit['pagina']})")


# ---------------- Main ---------------- #
if __name__ == "__main__":
    ensure_index()
    total = load_docs("out/dataset_all.json")
    print(f"✅ Upsert completo: {total} registros inseridos.")
    configure_settings()
    configure_embedder()
    wait_for_embeddings_ready()
    sanity_queries()
