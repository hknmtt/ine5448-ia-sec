# 

[introducao]

---

## Como Usar

```bash
# 1. Construir a imagem Docker
make build

# 2. Inicializar Meilisearch + Aplicação de Chat
make up
```

Então abrir [http://localhost:8501](http://localhost:8501) em seu navegador.

---

## 🧱 Pipeline de dados

Se você adicionou ou alterou documentos na pasta `data/1-raw`, execute:

```bash
make pipeline
```

Isto irá rodar o pipeline completo de ingestão → pré-processamento → indexação no Meilisearch **uma vez** dentro do Docker.

---

## 🧰 Comandos

```bash
# Você pode verificar os comandos com:
make help
```
