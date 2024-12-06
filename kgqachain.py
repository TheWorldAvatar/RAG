"""
Question answering over an RDF or OWL graph using SPARQL.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from langchain.chains.base import Chain
from langchain_core.callbacks import CallbackManagerForChainRun
from langchain_core.language_models import BaseLanguageModel
from langchain_core.prompts.base import BasePromptTemplate
from langchain_core.prompts.prompt import PromptTemplate
from langchain_core.runnables.base import RunnableSequence
from pydantic import Field

from rdflib.query import ResultRow
from rdflib import Variable, URIRef, Literal
import storeclient
from CommonNamespaces import namespace_name_or_iri
from SPARQLBuilder import make_prefix_str
from common import assemble_schema_description

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

op_query_custom = prefixes["rdfs"] + (
    """SELECT DISTINCT ?op ?com\n"""
    """WHERE { \n"""
    """    ?subj ?op ?obj . \n"""
    """    OPTIONAL { ?op rdfs:comment ?com } . \n"""
    """    FILTER isIRI(?obj) \n"""
    """}"""
)

dp_query_custom = prefixes["rdfs"] + (
    """SELECT DISTINCT ?dp ?com\n"""
    """WHERE { \n"""
    """    ?subj ?dp ?obj . \n"""
    """    OPTIONAL { ?dp rdfs:comment ?com } . \n"""
    """    FILTER isLiteral(?obj) \n"""
    """}"""
)

cls_owl_tbox_query = prefixes["rdfs"] + prefixes["owl"] + (
    """SELECT DISTINCT ?iri ?com\n"""
    """WHERE {\n"""
    """    ?iri a owl:Class .\n"""
    """    OPTIONAL { ?iri rdfs:comment ?com } .\n"""
    """    FILTER isIRI(?iri) \n"""
    """}"""
)

op_owl_tbox_query = prefixes["rdfs"] + prefixes["owl"] + (
    """SELECT DISTINCT ?iri ?dom ?rng ?com\n"""
    """WHERE {\n"""
    """    ?iri a owl:ObjectProperty .\n"""
    """    ?iri rdfs:domain ?dom .\n"""
    """    ?iri rdfs:range ?rng .\n"""
    """    OPTIONAL { ?iri rdfs:comment ?com }\n"""
    """}"""
)

dp_owl_tbox_query = prefixes["rdfs"] + prefixes["owl"] + (
    """SELECT DISTINCT ?iri ?dom ?com\n"""
    """WHERE {\n"""
    """    ?iri a owl:DatatypeProperty .\n"""
    """    ?iri rdfs:domain ?dom .\n"""
    """    OPTIONAL { ?iri rdfs:comment ?com }\n"""
    """}"""
)

def _describe_iri(res: dict, prefixes: dict[str, str]) -> str:
    iri = res["iri"]["value"]
    ns_iri = namespace_name_or_iri(iri, prefixes, "")
    additions = []
    if "dom" in res:
        domain = res["dom"]["value"] if "dom" in res else ""
        ns_domain = namespace_name_or_iri(domain, prefixes, "")
        additions.append(ns_domain)
    if "rng" in res:
        range = res["rng"]["value"]
        ns_range = namespace_name_or_iri(range, prefixes, "")
        additions.append(ns_range)
    if "com" in res:
        comment = res["com"]["value"]
        additions.append(comment)
    if len(additions) > 0:
        add_str = f" ({', '.join(additions)})"
    else:
        add_str = ""
    return f"{ns_iri}{add_str}"

def get_store_schema(sc: storeclient.StoreClient,
    prefixes: dict[str, str]) -> str:
    prefixes_str = "\n".join(
        make_prefix_str(p, prefixes[p]) for p in prefixes)
    classes = sc.query(cls_owl_tbox_query)["results"]["bindings"]
    classes_str = "\n".join([_describe_iri(r, prefixes) for r in classes])
    ops = sc.query(op_owl_tbox_query)["results"]["bindings"]
    ops_str = "\n".join([_describe_iri(r, prefixes) for r in ops])
    dtps = sc.query(dp_owl_tbox_query)["results"]["bindings"]
    dtps_str = "\n".join([_describe_iri(r, prefixes) for r in dtps])
    return assemble_schema_description(
        prefixes_str, classes_str, ops_str, dtps_str)

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
    schema_description: str
    sparql_generation_select_chain: RunnableSequence
    qa_chain: RunnableSequence
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
        qa_chain = qa_prompt | llm
        sparql_generation_select_chain = sparql_select_prompt | llm

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

        generation_result = self.sparql_generation_select_chain.invoke(
            {"prompt": prompt,
             "schema": self.schema_description},
            callbacks=callbacks
        )
        generated_sparql = generation_result.content

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
        res = result.content

        chain_result: Dict[str, Any] = {self.output_key: res}
        if self.return_sparql_query:
            chain_result[self.sparql_query_key] = generated_sparql
        return chain_result
