from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from hybridrag import HybridRAG
from ragconfig import RAGConfig

app = FastAPI()

config = RAGConfig("config-hybrid.yaml")
config.set_openai_api_key()
rag = HybridRAG(config)

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse("<p>RAG-system of The World Avatar</p>")

@app.get("/query/")
async def query(question: str=""):
    answer = rag.query(question) if question != "" else ""
    return {"answer": answer}
