"""
Question answering over an RDF or OWL graph using SPARQL.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from langchain.chains.base import Chain
from langchain.chains.llm import LLMChain
from langchain_core.callbacks import CallbackManagerForChainRun
from langchain_core.language_models import BaseLanguageModel
from langchain_core.prompts.base import BasePromptTemplate
from langchain_core.prompts.prompt import PromptTemplate
from pydantic import Field

from rdflib.query import ResultRow
from rdflib import Variable, URIRef, Literal
import storeclient
from CommonNamespaces import nameFromIRI

SPARQL_GENERATION_SELECT_TEMPLATE = """Task: Generate a SPARQL SELECT statement for querying a graph database.
For instance, to find all email addresses of John Doe, the following query in backticks would be suitable:
```
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?email
WHERE {{
    ?person foaf:name "John Doe" .
    ?person foaf:mbox ?email .
}}
```
Instructions:
Use only the node types and properties provided in the schema.
Do not use any node types and properties that are not explicitly provided.
Include all necessary prefixes.
Schema:
{schema}
Note: Be as concise as possible.
Do not include any explanations or apologies in your responses.
Do not respond to any questions that ask for anything else than for you to construct a SPARQL query.
Do not include any text except the SPARQL query generated.
Do not wrap the query in backticks.

The question is:
{prompt}"""
SPARQL_GENERATION_SELECT_PROMPT = PromptTemplate(
    input_variables=["schema", "prompt"], template=SPARQL_GENERATION_SELECT_TEMPLATE
)

SPARQL_QA_TEMPLATE = """Task: Generate a natural language response from the results of a SPARQL query.
You are an assistant that creates well-written and human understandable answers.
The information part contains the information provided, which you can use to construct an answer.
The information provided is authoritative, you must never doubt it or try to use your internal knowledge to correct it.
Make your response sound like the information is coming from an AI assistant, but don't add any information.
Information:
{context}

Question: {prompt}
Helpful Answer:"""
SPARQL_QA_PROMPT = PromptTemplate(
    input_variables=["context", "prompt"], template=SPARQL_QA_TEMPLATE
)

prefixes = {
    "owl": """PREFIX owl: <http://www.w3.org/2002/07/owl#>\n""",
    "rdf": """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n""",
    "rdfs": """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n""",
    "xsd": """PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n""",
}

cls_query_rdf = prefixes["rdfs"] + (
    """SELECT DISTINCT ?cls ?com\n"""
    """WHERE { \n"""
    """    ?instance a ?cls . \n"""
    """    OPTIONAL { ?cls rdfs:comment ?com } \n"""
    """}"""
)

cls_query_rdfs = prefixes["rdfs"] + (
    """SELECT DISTINCT ?cls ?com\n"""
    """WHERE { \n"""
    """    ?instance a/rdfs:subClassOf* ?cls . \n"""
    """    OPTIONAL { ?cls rdfs:comment ?com } \n"""
    """}"""
)

cls_query_owl = prefixes["rdfs"] + (
    """SELECT DISTINCT ?cls ?com\n"""
    """WHERE { \n"""
    """    ?instance a/rdfs:subClassOf* ?cls . \n"""
    """    FILTER (isIRI(?cls)) . \n"""
    """    OPTIONAL { ?cls rdfs:comment ?com } \n"""
    """}"""
)

rel_query_rdf = prefixes["rdfs"] + (
    """SELECT DISTINCT ?rel ?com\n"""
    """WHERE { \n"""
    """    ?subj ?rel ?obj . \n"""
    """    OPTIONAL { ?rel rdfs:comment ?com } \n"""
    """}"""
)

rel_query_rdfs = (
    prefixes["rdf"]
    + prefixes["rdfs"]
    + (
        """SELECT DISTINCT ?rel ?com\n"""
        """WHERE { \n"""
        """    ?rel a/rdfs:subPropertyOf* rdf:Property . \n"""
        """    OPTIONAL { ?rel rdfs:comment ?com } \n"""
        """}"""
    )
)

op_query_owl = (
    prefixes["rdfs"]
    + prefixes["owl"]
    + (
        """SELECT DISTINCT ?op ?com\n"""
        """WHERE { \n"""
        """    ?op a/rdfs:subPropertyOf* owl:ObjectProperty . \n"""
        """    OPTIONAL { ?op rdfs:comment ?com } \n"""
        """}"""
    )
)

dp_query_owl = (
    prefixes["rdfs"]
    + prefixes["owl"]
    + (
        """SELECT DISTINCT ?dp ?com\n"""
        """WHERE { \n"""
        """    ?dp a/rdfs:subPropertyOf* owl:DatatypeProperty . \n"""
        """    OPTIONAL { ?dp rdfs:comment ?com } \n"""
        """}"""
    )
)

def _res_to_str(res: dict, var: str) -> str:
    iri = res[var]["value"]
    comment = res["com"]["value"] if "com" in res else ""
    return (
        "<"
        + iri
        + "> ("
        + nameFromIRI(iri)
        + ", "
        + comment
        + ")"
    )

def get_store_schema(sc: storeclient.StoreClient, standard: str) -> str:
    if standard == "custom":
        clss = sc.query(cls_query_rdfs)["results"]["bindings"]
        rels = sc.query(rel_query_rdf)["results"]["bindings"]
        return (
            f"In the following, each IRI is followed by the local name and "
            f"optionally its description in parentheses. \n"
            f"The RDF graph supports the following node types:\n"
            f'{", ".join([_res_to_str(r, "cls") for r in clss])}\n'
            f"The RDF graph supports the following relationships:\n"
            f'{", ".join([_res_to_str(r, "rel") for r in rels])}\n'
        )
    elif standard == "rdfs":
        clss = sc.query(cls_query_rdfs)["results"]["bindings"]
        rels = sc.query(rel_query_rdfs)["results"]["bindings"]
        return (
            f"In the following, each IRI is followed by the local name and "
            f"optionally its description in parentheses. \n"
            f"The RDF graph supports the following node types:\n"
            f'{", ".join([_res_to_str(r, "cls") for r in clss])}\n'
            f"The RDF graph supports the following relationships:\n"
            f'{", ".join([_res_to_str(r, "rel") for r in rels])}\n'
        )
    elif standard == "owl":
        clss = sc.query(cls_query_owl)["results"]["bindings"]
        ops = sc.query(op_query_owl)["results"]["bindings"]
        dps = sc.query(dp_query_owl)["results"]["bindings"]
        return (
            f"In the following, each IRI is followed by the local name and "
            f"optionally its description in parentheses. \n"
            f"The OWL graph supports the following node types:\n"
            f'{", ".join([_res_to_str(r, "cls") for r in clss])}\n'
            f"The OWL graph supports the following object properties, "
            f"i.e., relationships between objects:\n"
            f'{", ".join([_res_to_str(r, "op") for r in ops])}\n'
            f"The OWL graph supports the following data properties, "
            f"i.e., relationships between objects and literals:\n"
            f'{", ".join([_res_to_str(r, "dp") for r in dps])}\n'
        )
    else:
        raise ValueError(f"Schema mode '{standard}' is not supported!")

def _make_result_row(r: dict) -> ResultRow:
    values = {}
    labels = []
    for varname in r:
        value = r[varname]["value"]
        values[Variable(varname)] = (
            URIRef(value) if value.startswith("http") else Literal(value)
        )
        labels.append(Variable(varname))
    return ResultRow(values, labels)

class KGQAChain(Chain):
    """
    Question-answering against an RDF or OWL graph by generating SPARQL statements.
    """

    store_client: storeclient.StoreClient = Field(exclude=True)
    sparql_generation_select_chain: LLMChain
    qa_chain: LLMChain
    return_sparql_query: bool = False
    input_key: str = "query"  #: :meta private:
    output_key: str = "result"  #: :meta private:
    sparql_query_key: str = "sparql_query"  #: :meta private:

    @property
    def input_keys(self) -> List[str]:
        """Return the input keys.

        :meta private:
        """
        return [self.input_key]

    @property
    def output_keys(self) -> List[str]:
        """Return the output keys.

        :meta private:
        """
        _output_keys = [self.output_key]
        return _output_keys

    @classmethod
    def from_llm(
        cls,
        llm: BaseLanguageModel,
        *,
        qa_prompt: BasePromptTemplate = SPARQL_QA_PROMPT,
        sparql_select_prompt: BasePromptTemplate = SPARQL_GENERATION_SELECT_PROMPT,
        **kwargs: Any,
    ) -> KGQAChain:
        """Initialize from LLM."""
        qa_chain = LLMChain(llm=llm, prompt=qa_prompt)
        sparql_generation_select_chain = LLMChain(llm=llm, prompt=sparql_select_prompt)

        return cls(
            qa_chain=qa_chain,
            sparql_generation_select_chain=sparql_generation_select_chain,
            **kwargs,
        )

    def _call(
        self,
        inputs: Dict[str, Any],
        run_manager: Optional[CallbackManagerForChainRun] = None,
    ) -> Dict[str, str]:
        """
        Generate SPARQL query, use it to retrieve a response from the gdb and answer
        the question.
        """
        _run_manager = run_manager or CallbackManagerForChainRun.get_noop_manager()
        callbacks = _run_manager.get_child()
        prompt = inputs[self.input_key]

        sparql_generation_chain = self.sparql_generation_select_chain

        generation_result = sparql_generation_chain.invoke(
            {"prompt": prompt,
             "schema": get_store_schema(self.store_client, "custom")},
            callbacks=callbacks
        )
        generated_sparql = generation_result[sparql_generation_chain.output_key]

        _run_manager.on_text("Generated SPARQL:", end="\n", verbose=self.verbose)
        _run_manager.on_text(
            generated_sparql, color="green", end="\n", verbose=self.verbose
        )

        reply = self.store_client.query(generated_sparql)["results"]["bindings"]
        # Turn reply dictionary into list of result rows.
        context = [_make_result_row(r) for r in reply]

        _run_manager.on_text("Full Context:", end="\n", verbose=self.verbose)
        _run_manager.on_text(
            str(context), color="green", end="\n", verbose=self.verbose
        )
        result = self.qa_chain.invoke(
            {"prompt": prompt, "context": context},
            callbacks=callbacks,
        )
        res = result[self.qa_chain.output_key]

        chain_result: Dict[str, Any] = {self.output_key: res}
        if self.return_sparql_query:
            chain_result[self.sparql_query_key] = generated_sparql
        return chain_result
