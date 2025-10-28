import streamlit as st
import meilisearch
import os
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

# --- Config ---
MEILI_URL = os.getenv("MEILI_URL", "http://127.0.0.1:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY", "")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
INDEX_UID = "documentos"

meili_client = meilisearch.Client(MEILI_URL, MEILI_KEY)
index = meili_client.index(INDEX_UID)
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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
        with st.status("🔎 Iniciando busca no acervo...", expanded=True) as status:

            # 1️⃣ Interpretação da intenção
            status.update(label="🧠 Interpretando a pergunta com IA...")
            # TODO: Refinar prompt
            system_prompt = """
            Você é um assistente especializado em documentos do Arquivo Nacional.
            Sua tarefa é transformar perguntas em linguagem natural em termos de busca relevantes
            para um sistema de indexação semântica de documentos. Seja conciso e foque em palavras-chave contextuais.
            Responda apenas com a consulta gerada.
            Exemplo:
            Pergunta: "Quero ver relatórios sobre espionagem de estudantes em Brasília."
            Resposta: "relatórios espionagem estudantes Brasília"
            """
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini", # TODO: Alterar modelo?
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
            )
            query = response.choices[0].message.content.strip()

            status.update(label=f"📚 Buscando documentos relacionados a: `{query}`...")

            # 2️⃣ Busca vetorial
            res = index.search(query, {
                "hybrid": {"semanticRatio": semantic_ratio, "embedder": "documentos-openai"},
                "limit": limit
            })

            # 3️⃣ Montagem da resposta
            if not res["hits"]:
                st.markdown("_Nenhum documento relevante encontrado._")
                assistant_msg = "Não encontrei documentos que se encaixem bem nessa descrição."
                status.update(label="❌ Nenhum documento encontrado.", state="error")
            else:
                status.update(label="✍️ Gerando resposta com base nos documentos encontrados...")

                context_text = "\n\n".join([
                    f"Título: {h['titulo']} (p.{h['pagina']})\nTrecho: {h['texto'][:400]}..."
                    for h in res["hits"]
                ])

                # TODO: Refinar prompt
                synthesis_prompt = f"""
                Você é um assistente de busca do Arquivo Nacional.
                Pergunta do usuário: {prompt} 
                Documentos correspondentes:
                {context_text}

                Responda de forma curta e objetiva, apenas confirmando que foram encontrados documentos
                relacionados ao tema solicitado. Liste ou mencione brevemente os principais títulos,
                sem interpretar o conteúdo nem adicionar contexto histórico.
                """

                summary = openai_client.chat.completions.create(
                    model="gpt-4o-mini", # TODO: Alterar modelo?
                    messages=[
                        {"role": "system", "content": "Você é um chat assistente historiador analítico e preciso."},
                        {"role": "user", "content": synthesis_prompt},
                    ],
                )
                assistant_msg = summary.choices[0].message.content.strip()
                st.markdown(assistant_msg)

                # Listagem dos documentos encontrados
                with st.expander("📄 Documentos relacionados"):
                    for h in res["hits"]:
                        st.markdown(f"**{h['titulo']}** (p.{h['pagina']})  \n> {h['texto'][:400]}...")

                status.update(label="✅ Busca concluída com sucesso!", state="complete")

    st.session_state.messages.append({"role": "assistant", "content": assistant_msg})
