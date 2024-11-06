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
from pydantic import Field

from langchain_community.chains.graph_qa.prompts import (
    SPARQL_GENERATION_SELECT_PROMPT,
    SPARQL_INTENT_PROMPT,
    SPARQL_QA_PROMPT,
)

from rdflib.query import ResultRow
from rdflib import Variable, URIRef, Literal
import storeclient
from CommonNamespaces import nameFromIRI

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
    sparql_intent_chain: LLMChain
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
        sparql_intent_prompt: BasePromptTemplate = SPARQL_INTENT_PROMPT,
        **kwargs: Any,
    ) -> KGQAChain:
        """Initialize from LLM."""
        qa_chain = LLMChain(llm=llm, prompt=qa_prompt)
        sparql_generation_select_chain = LLMChain(llm=llm, prompt=sparql_select_prompt)
        sparql_intent_chain = LLMChain(llm=llm, prompt=sparql_intent_prompt)

        return cls(
            qa_chain=qa_chain,
            sparql_generation_select_chain=sparql_generation_select_chain,
            sparql_intent_chain=sparql_intent_chain,
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

        _intent = self.sparql_intent_chain.run({"prompt": prompt}, callbacks=callbacks)
        intent = _intent.strip()

        if "SELECT" in intent:
            sparql_generation_chain = self.sparql_generation_select_chain
            intent = "SELECT"
        else:
            raise ValueError(
                "SELECT is the only supported SPARQL query type in prompts!"
            )

        _run_manager.on_text("Identified intent:", end="\n", verbose=self.verbose)
        _run_manager.on_text(intent, color="green", end="\n", verbose=self.verbose)

        generated_sparql = sparql_generation_chain.run(
            {"prompt": prompt,
             "schema": get_store_schema(self.store_client, "custom")},
            callbacks=callbacks
        )

        _run_manager.on_text("Generated SPARQL:", end="\n", verbose=self.verbose)
        _run_manager.on_text(
            generated_sparql, color="green", end="\n", verbose=self.verbose
        )

        if intent == "SELECT":
            reply = self.store_client.query(generated_sparql)["results"]["bindings"]
            # Turn reply dictionary into list of result rows.
            context = [_make_result_row(r) for r in reply]

            _run_manager.on_text("Full Context:", end="\n", verbose=self.verbose)
            _run_manager.on_text(
                str(context), color="green", end="\n", verbose=self.verbose
            )
            result = self.qa_chain(
                {"prompt": prompt, "context": context},
                callbacks=callbacks,
            )
            res = result[self.qa_chain.output_key]
        else:
            raise ValueError("Unsupported SPARQL query type.")

        chain_result: Dict[str, Any] = {self.output_key: res}
        if self.return_sparql_query:
            chain_result[self.sparql_query_key] = generated_sparql
        return chain_result
