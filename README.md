
# Inferyx AI Chatbot Suite

> Built during internship at **Inferyx** — an end-to-end AI assistant system covering documentation Q&A, intelligent datapod management, and automated knowledge graph construction from enterprise data schemas.

---

## What This Is

This repository contains three production-grade AI systems built on top of the Inferyx platform, each solving a distinct operational problem:

| System | File | What It Does |
|---|---|---|
| **Docs Chatbot** | `new_chatbot.py` | RAG-based Q&A over Inferyx's Confluence documentation |
| **DataPod Agent** | `DP_chatbot.py` + `agent_1v4.py` | Conversational agent for creating/deleting datapods via the Inferyx SDK |
| **Knowledge Graph Builder** | `kg_code.py` + `folder_to_kg_v4.py` | Converts MySQL schemas and CSV files into a Neo4j knowledge graph |

---

## System 1 — Documentation Chatbot (`new_chatbot.py`)

Answers natural language questions over Inferyx's internal Confluence documentation (272+ indexed pages).

**Architecture:**
```
User Query → FAISS Similarity Search → Top-K Doc Retrieval
         → Groq LLaMA 3 (8B) → Self-verified Answer + Source URLs
```

**Key design decisions:**
- Uses **Google `embedding-001`** for semantic similarity, not keyword search
- Score threshold filtering (`> 0.5`) prevents hallucinated answers from low-relevance retrievals
- LLM is prompted to self-report via `[ANSWERED_FROM_CONTEXT]` / `[NOT_ANSWERED_FROM_CONTEXT]` tags — sources are only shown when the model confirms it answered from context
- Greeting detection short-circuits the FAISS + LLM pipeline entirely for efficiency
- Exposed as a **Flask REST API** (`POST /ask_ai`) with structured JSON responses

**Stack:** `LangChain` · `FAISS` · `Groq (LLaMA 3-8b-8192)` · `Google Generative AI Embeddings` · 

---

## System 2 — DataPod Conversational Agent

Two agent implementations for managing Inferyx datapods through natural language:

### `agent_1v4.py` — CLI Agent (LLaMA 3 via Groq)
Guides users through CSV-to-datapod creation in a conversational loop. Parses structured commands (`create <name> <pk>`, `delete <name>`) from free-form conversation using regex on the LLM's final output line.

### `DP_chatbot.py` — ReAct Agent (Gemini 2.5 Flash via LangGraph)
Full agentic workflow using **LangGraph's `create_react_agent`** with 7 tools:

```
autodetect_csv_structure  →  assign_schema_from_csv / infer_schema_from_csv
                          →  create_or_edit_schema (LLM-driven)
                          →  generate_data_from_schema
                          →  save_schema_and_data
                          →  create_datapod_from_csv  →  Inferyx SDK
```

Notable features:
- **Auto-detects** whether a CSV has headers using column type heuristics — no user input required
- Handles two distinct flows: *create from scratch* (schema generation + synthetic data) and *create from existing CSV*
- `with_data` boolean flag controls whether the datapod is created schema-only or with data written
- In-memory state managed across tool calls; cleared atomically after datapod creation
- Error handling distinguishes between "already exists", "invalid PK", and generic SDK failures

**Stack:** `LangGraph` · `LangChain Tools` · `Gemini 2.5 Flash` · `Inferyx Python SDK` · `Google Generative AI`

---

## System 3 — Knowledge Graph Builder

Converts structured data sources into a queryable Neo4j knowledge graph — two approaches:

### `kg_code.py` — MySQL Schema → Neo4j
```
User Input (DB name) → LLaMA 3 extracts DB name → MySQL SHOW TABLES/COLUMNS
                    → LLMGraphTransformer → Neo4j (nodes = tables, edges = FK relationships)
```

### `folder_to_kg_v4.py` — CSV/Excel Folder → Graph Schema (Gemini)
Processes an entire directory of files in batches, building a cumulative graph schema:
```
CSV/Excel files → header extraction → batched LLM prompts (with rolling context)
               → structured output (GraphNode + GraphRelationship Pydantic models)
               → printed graph schema (extensible to Neo4j push)
```

Key design: **rolling context** is passed between batches so entity deduplication and relationship continuity are maintained across files without re-processing prior batches.

**Stack:** `LangChain Graph Transformers` · `Neo4j` · `Gemini 2.0 Flash` · `MySQL Connector` · `LangChain Experimental`

---

## Meta-Retriever (`meta_retriever.py`)

A standalone FAISS + MongoDB retrieval service used by other components:

- Loads a per-app FAISS index from `/app/framework/index/{app_uuid}`
- Supports **collection-level filtering** on vector search results
- Deduplicates by `(uuid, version)` before querying MongoDB
- Applies a relevance score threshold (`> 0.6`) and fetches only `active: "Y"` documents
- Returns structured JSON with consistent error/success envelope format

---

## Repository Structure

```
Inferyx/
├── inferyx_chatbot/
│   ├── new_chatbot.py          # Docs RAG chatbot (Flask API)
│   ├── DP_chatbot.py           # DataPod ReAct agent (LangGraph)
│   ├── agent_1.py → agent_1v4  # CLI agent iterations
│   ├── kg_code.py              # MySQL → Neo4j KG builder
│   ├── folder_to_kg_v4.py      # CSV folder → graph schema
│   ├── meta_retriever.py       # FAISS + MongoDB retrieval service
│   ├── schema.py / final_schema.py   # Schema extraction utilities
│   ├── create_meta_index.py    # FAISS index builder
│   ├── search_docs.py          # Document search utilities
│   ├── data_quality.py         # Data validation
│   └── .env                    # Environment config (not committed)
├── inferyx_links.json          # Crawled Confluence URL index (272+ pages)
└── test.csv                    # Sample test data
```

---

## Environment Setup

```bash
# Install dependencies
pip install langchain langchain-groq langchain-google-genai langgraph
pip install faiss-cpu flask flask-cors pymongo mysql-connector-python
pip install langchain-community langchain-experimental neo4j python-dotenv pandas

# Configure environment variables
GROQ_API_KEY=...
GOOGLE_API_KEY=...
INFERYX_HOST=dev.inferyx.com
INFERYX_APP_TOKEN=...
INFERYX_ADMIN_TOKEN=...
```

---

## Running the Systems

```bash
# 1. Documentation chatbot (Flask API on port 5000)
python inferyx_chatbot/new_chatbot.py

# 2. DataPod agent (CLI)
python inferyx_chatbot/agent_1v4.py

# 3. DataPod ReAct agent (CLI, full workflow)
python inferyx_chatbot/DP_chatbot.py

# 4. Knowledge graph builder (MySQL)
python inferyx_chatbot/kg_code.py

# 5. Knowledge graph builder (CSV folder)
python inferyx_chatbot/folder_to_kg_v4.py
```

---

## Design Principles

**Separation of concerns** — each component (retrieval, generation, SDK interaction, graph construction) is independently runnable and testable.

**Fail-safe generation** — the docs chatbot explicitly hides sources when the LLM signals it couldn't answer from context, preventing confident-but-wrong responses from reaching the user.

**Iterative agent development** — the 6 agent versions (`agent_1` through `agent_1V6`) reflect progressive refinement: from regex-based command parsing to a full tool-calling ReAct loop.

**Structured outputs over free text** — the KG builder uses Pydantic models (`GraphNode`, `GraphRelationship`) with `with_structured_output()` to enforce type-safe graph construction.

---

## Tech Stack Summary

| Layer | Technology |
|---|---|
| LLMs | Groq LLaMA 3 (8B), Google Gemini 2.0/2.5 Flash, Gemini 1.5 Flash |
| Embeddings | Google `embedding-001` (semantic similarity) |
| Vector Store | FAISS (local, per-app index) |
| Agent Framework | LangGraph (`create_react_agent`), LangChain Tools |
| Graph DB | Neo4j |
| App DB | MongoDB, MySQL |
| API | Flask + Flask-CORS |
| Platform SDK | Inferyx Python SDK (`Datapod`, `AppConfig`) |
| Data Source | Inferyx Confluence (272+ pages, crawled + indexed) |

---

*Built at Inferyx · Internship Project · 2025*
