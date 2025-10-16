import streamlit as st
import meilisearch
import os
from dotenv import load_dotenv
load_dotenv()

# --- Config ---
MEILI_URL = os.getenv("MEILI_URL", "http://127.0.0.1:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY", "")
INDEX_UID = "documentos"

client = meilisearch.Client(MEILI_URL, MEILI_KEY)
index = client.index(INDEX_UID)

st.set_page_config(page_title="Busca Histórica", page_icon="📜")
st.title("📜 Chat com o Arquivo Histórico")
st.caption("Busque documentos e relatórios históricos com compreensão semântica.")

# --- Sidebar config ---
semantic_ratio = st.sidebar.slider("Peso semântico", 0.0, 1.0, 0.7, 0.1)
limit = st.sidebar.slider("Resultados", 1, 10, 5)

# --- Chat UI ---
if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Digite sua pergunta sobre os documentos..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Consultando o acervo..."):
            res = index.search(prompt, {
                "hybrid": {"semanticRatio": semantic_ratio, "embedder": "documentos-openai"},
                "limit": limit
            })

        if not res["hits"]:
            st.markdown("_Nenhum documento relevante encontrado._")
        else:
            for h in res["hits"]:
                st.markdown(f"**{h['titulo']}** (p.{h['pagina']})  \n> {h['texto'][:400]}...")

    st.session_state.messages.append({
        "role": "assistant",
        "content": "\n".join([h['titulo'] for h in res["hits"]])
    })
