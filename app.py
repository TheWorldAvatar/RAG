import os
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

from hybridrag import HybridRAG
from ragconfig import RAGConfig
from questions import Questions

class RAGApp(FastAPI):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rag: HybridRAG = None
        # Load configuration
        self.config = RAGConfig("config-hybrid.yaml")
        self.config.set_openai_api_key()
        # Load example question catalogue
        questions = Questions()
        questions.load(os.path.join("data", "questions-example.json"))
        cat_qs = questions.categorised_question_dict(default_cat="Allgemein")
        subdomains: list[dict[str, any]] = []
        for cat in cat_qs:
            subdomains.append({"label": cat, "questions": cat_qs[cat]})
        self.subdomains = subdomains
        # Load HTML templates
        self.html_templates = Jinja2Templates(directory="html_templates")

@asynccontextmanager
async def lifespan(app: RAGApp):
    # Initialise RAG system
    app.rag = HybridRAG(app.config)
    yield

app = RAGApp(lifespan=lifespan)
# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return app.html_templates.TemplateResponse(
        "qa.html",
        dict(
            request=request,
            name="replace_me_name",
            ga_measurement_id="replace_me_id",
            title="RAG-System",
            sample_questions=[
                {
                    "label": "Parlamentsdebatten",
                    "subdomains": app.subdomains
                }
            ],
        ),
    )

@app.get("/query/")
async def query(question: str=""):
    if question == "":
        answer = ""
        sources = ""
    else:
        result = app.rag.query(question)
        answer = result[app.rag.chain.answer_key].replace("\n", "<br/>")
        sources = result[app.rag.chain.sources_key].replace("\n", "<br/>")
    return {
        "question": question,
        "answer": answer,
        "sources": sources
    }
