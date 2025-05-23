from __future__ import annotations

from typing import Any, Dict, List, Optional

from langchain.chains.base import Chain
from langchain_core.callbacks import CallbackManagerForChainRun
from langchain_core.language_models import BaseLanguageModel
from langchain_core.prompts.prompt import PromptTemplate
from langchain_core.prompt_values import StringPromptValue
from langchain_core.runnables.base import RunnableSequence
from langchain.schema.runnable import Runnable
from langchain_core.documents import Document
from langchain_core.vectorstores import VectorStoreRetriever
from langchain_qdrant import QdrantVectorStore
from qdrant_client import models
from pydantic import Field

from common import *
from ragconfig import *
from storeclient import StoreClient

class RunnableLogInputs(Runnable):
    """
    Runnable whose sole purpose is to log a string input that is
    being passed through.
    """

    def invoke(
        self, input_data: StringPromptValue, config: Dict[str, Any] = None
    ) -> StringPromptValue:
        log_msg(input_data.to_string(), level=logging.DEBUG)
        return input_data

def query_result_pretty_str(result: list[dict[str, str]],
    max_items: int, additions: dict[str, str] = None
) -> str:
    """
    Turns a list of dictionaries returned by a SPARQL query
    into a string representation intended to be inserted into
    an LLM prompt template.
    """
    str_list = ["["]
    for index, r in enumerate(result):
        str_list.append("(")
        for varname in r:
            value = r[varname]["value"]
            if not value.startswith("http"):
                str_list.append(f"{varname}: {value},")
        if additions is not None:
            for key in additions:
                str_list.append(f"{key}: {additions[key]},")
        str_list.append("),")
        if index > max_items:
            break
    str_list.append("]")
    return "".join(str_list)

def make_date_range_filter(
    start_date: str=None, end_date: str=None
) -> models.Filter:
    # https://qdrant.tech/documentation/concepts/filtering/
    # Use a full datetime stamp, e.g. "2023-02-08T10:49:00Z", but
    # just date also seems to work.
    return models.Filter(
        must=[
            models.FieldCondition(
                key="metadata.Datum",
                range=models.DatetimeRange(
                    gt=None,
                    gte=start_date,
                    lt=None,
                    lte=end_date,
                ),
            )
        ]
    )

def extract_references(text: str, docs: list[Document]) -> str:
    """
    Scans a text for references to any of the given documents by ID
    and returns a new citation-style formatted text containing the
    references found.
    """
    refs: list[str] = []
    used_refs: set[str] = set()
    for doc in docs:
        if "ID" in doc.metadata:
            if doc.metadata["ID"] in text and doc.metadata["ID"] not in used_refs:
                # This document is being referenced.
                refs.append(f'[{doc.metadata["ID"]}] Plenarprotokoll '
                    f'{doc.metadata["Wahlperiode"]}/{doc.metadata["Sitzungnr"]}')
                used_refs.add(doc.metadata["ID"])
    return "\n".join(refs) if len(refs) > 0 else ""

class HybridQAChain(Chain):
    store_client: StoreClient = Field(exclude=True)
    schema_description: str
    parties: list[str]
    config: RAGConfig
    vector_store: QdrantVectorStore
    threshold_retriever: VectorStoreRetriever = Field(exclude=True)
    top_k_retriever: VectorStoreRetriever = Field(exclude=True)
    sparql_gen_chain: RunnableSequence
    sparql_classify_chain: RunnableSequence
    sparql_gen_or_retrieve_chain: RunnableSequence
    need_content_chain: RunnableSequence
    sparql_gen_with_ids_chain: RunnableSequence
    sparql_gen_with_docs_chain: RunnableSequence
    answer_gen_chain: RunnableSequence
    return_sparql_query: bool = False
    input_key: str = "query"  #: :meta private:
    answer_key: str = "answer"  #: :meta private:
    sources_key: str = "sources"  #: :meta private:
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
        _output_keys = [self.answer_key, self.sources_key]
        return _output_keys

    @classmethod
    def from_llm(
        cls,
        llm: BaseLanguageModel,
        sparql_gen_prompt: PromptTemplate,
        sparql_classify_prompt: PromptTemplate,
        sparql_gen_or_retrieve_prompt: PromptTemplate,
        need_content_prompt: PromptTemplate,
        sparql_gen_with_ids_prompt: PromptTemplate,
        sparql_gen_with_docs_prompt: PromptTemplate,
        answer_gen_prompt: PromptTemplate,
        *,
        schema_description: str,
        parties: list[str],
        **kwargs: Any,
    ) -> HybridQAChain:
        """Initialise from LLM."""
        sparql_gen_chain = (
            sparql_gen_prompt
            | RunnableLogInputs()
            | llm
        )
        sparql_classify_chain = (
            sparql_classify_prompt
            | RunnableLogInputs()
            | llm.with_structured_output(None, method="json_mode")
        )
        sparql_gen_or_retrieve_chain = (
            sparql_gen_or_retrieve_prompt
            | RunnableLogInputs()
            | llm
        )
        need_content_chain = (
            need_content_prompt
            | RunnableLogInputs()
            | llm
        )
        sparql_gen_with_ids_chain = (
            sparql_gen_with_ids_prompt
            | RunnableLogInputs()
            | llm
        )
        sparql_gen_with_docs_chain = (
            sparql_gen_with_docs_prompt
            | RunnableLogInputs()
            | llm
        )
        answer_gen_chain = (
            answer_gen_prompt
            | RunnableLogInputs()
            | llm
        )

        return cls(
            sparql_gen_chain=sparql_gen_chain,
            sparql_classify_chain=sparql_classify_chain,
            need_content_chain=need_content_chain,
            sparql_gen_or_retrieve_chain=sparql_gen_or_retrieve_chain,
            sparql_gen_with_ids_chain=sparql_gen_with_ids_chain,
            sparql_gen_with_docs_chain=sparql_gen_with_docs_chain,
            answer_gen_chain=answer_gen_chain,
            schema_description=schema_description,
            parties=parties,
            **kwargs,
        )

    def _retrieve_from_vector_store(self, query: str, top_k: int=4,
        score_threshold: float=None, filter: models.Filter=None,
        exclude_page_content: bool=False
    ) -> list[Document]:
        embedded_query_dense_vec = (
            self.vector_store.embeddings.embed_query(query)
        )
        # https://qdrant.tech/documentation/concepts/search/
        result = self.vector_store.client.query_points(
            collection_name=self.vector_store.collection_name,
            query=embedded_query_dense_vec,
            query_filter=filter,
            #search_params=models.SearchParams(hnsw_ef=128, exact=False),
            #with_vectors=True, #seems to default to False
            with_payload=(
                models.PayloadSelectorExclude(exclude=["page_content"])
                if exclude_page_content else True
            ), #seems to default to True
            limit=top_k,
            score_threshold=score_threshold
        )
        # Turn the query result into a list of documents.
        doc_list: list[Document] = []
        for p in result.points:
            if "metadata" in p.payload:
                log_msg(f"Metadata: {str(p.payload["metadata"])}, "
                    f"score: {p.score}", level=logging.DEBUG)
            doc_list.append(Document(
                page_content=(p.payload["page_content"]
                    if "page_content" in p.payload else ""),
                metadata=(p.payload["metadata"]
                    if "metadata" in p.payload else {})
            ))
        return doc_list

    def _call(
        self,
        inputs: Dict[str, Any],
        run_manager: Optional[CallbackManagerForChainRun] = None,
    ) -> Dict[str, str]:
        """
        Break the question down into retrieval steps, either from the vector
        store or from the knowledge graph or both as required, and then use
        the retrieved information to generate an answer to the question.
        """
        #_run_manager = run_manager or CallbackManagerForChainRun.get_noop_manager()
        #callbacks = _run_manager.get_child()
        question = inputs[self.input_key]

        # Ask the LLM to generate a SPARQL query
        schema_and_question_inputs = {
            "schema": self.schema_description,
            "parties": self.parties,
            "question": question
        }
        # Generate initial SPARQL query
        gen_res_str: str = self.sparql_gen_chain.invoke(
            schema_and_question_inputs).content
        initial_query = gen_res_str.strip(" .`'")
        # Determine if prior vector store retrieval is necessary. If the
        # query involves filtering the textual content of speeches, then
        # this means we need to retrieve the latter from the vector store
        # first. The LLM should tell us that.
        cl_res = self.sparql_classify_chain.invoke({"query": initial_query})
        log_msg(f"Classification result: {str(cl_res)}", level=logging.DEBUG)
        # Things to be added to KG-retrieved results, as determined by
        # detected filters. This is a fudge, and propbably doesn't work
        # for more complex, nested questions!
        # TODO: Revisit addition of filter information to KG-retrieved
        # results!
        additions: dict[str, str] = {}
        if cl_res["topic"] != "":
            # The LLM told us to retrieve documents from the vector store first.
            topic = cl_res["topic"]
            log_msg(f"Topic to be retrieved from vector store: '{topic}'",
                level=logging.DEBUG)
            # Construct date range filter, if any.
            if cl_res["start_date"] == "" and cl_res["end_date"] == "":
                date_filter = None
            else:
                date_filter = make_date_range_filter(
                    start_date=(cl_res["start_date"]
                        if cl_res["start_date"] != "" else None),
                    end_date=(cl_res["end_date"]
                        if cl_res["end_date"] != "" else None),
                )
            log_msg(f"Date filter: {str(date_filter)}", level=logging.DEBUG)
            # Party filter, if any.
            party_filter = None
            if "party" in cl_res:
                if cl_res["party"] != "":
                    party_filter = models.Filter(
                        must=[
                            models.FieldCondition(
                                key="metadata.Fraktion",
                                match=models.MatchValue(value=cl_res["party"])
                            )
                        ]
                    )
                    additions["Fraktion"] = cl_res["party"]
            log_msg(f"Party filter: {str(party_filter)}", level=logging.DEBUG)
            # Combine filters, if any.
            # TODO: Add filtering for all metadata! Develop generic
            # infrastructure to handle filters!
            if date_filter is not None and party_filter is not None:
                combined_filter = models.Filter(
                    must=[date_filter, party_filter])
            elif date_filter is not None:
                combined_filter = date_filter
            elif party_filter is not None:
                combined_filter = party_filter
            else:
                combined_filter = None
            log_msg(f"Combined filter: {str(combined_filter)}",
                level=logging.DEBUG)
            # Determine if we need the speech texts.
            need_content_str: str = self.need_content_chain.invoke(
                schema_and_question_inputs).content
            ncs = need_content_str.lower().strip(' ."')
            log_msg(f"Is speech content to be retrieved: '{need_content_str}'",
                level=logging.DEBUG)
            need_content = ("yes" in ncs) or ("ja" in ncs)
            # Retrieve documents from vector store.
            if need_content:
                retrieved_from_vs = self._retrieve_from_vector_store(topic,
                    top_k=self.config.get(CVN_TOP_K), filter=combined_filter)
            else:
                # Note: Even if speech content is not needed, we
                # retrieve it. People tend to like embellishments even if they
                # are not explicitly asked for! This is only feasible for small
                # top-k numbers, though, and unrealistic for global questions!
                # TODO: Develop a more sophisticated way of detecting whether
                # or not speech content is required, and if not, use much
                # higher limits for item numbers, e.g. >1k!
                retrieved_from_vs = self._retrieve_from_vector_store(topic,
                    top_k=self.config.get(CVN_THRESHOLD_TOP_K),
                    score_threshold=self.config.get(CVN_THRESHOLD_SCORE),
                    filter=combined_filter, exclude_page_content=False)
            log_msg(f"Retrieved {len(retrieved_from_vs)} items from "
                "vector store.", level=logging.DEBUG)
            if need_content:
                # If the content of the speeches is needed, then we don't need
                # to query the KG at all. NB This is a simplification that
                # prevents us from answering certain nested questions!
                # TODO: Adopt a more agentic design to allow for nested questions!
                sparql_query = ""
            else:
                # Otherwise, try again to generate a query, this time with
                # retrieved information as context.
                gen_wc_inputs = {
                    "schema": self.schema_description,
                    "parties": self.parties,
                    "context": retrieved_from_vs,
                    "question": question
                }
                gen_wc_res_str: str = (
                    # TODO: Revisit the question if we need different query
                    # regeneration prompts for different context types!
                    #self.sparql_gen_with_docs_chain.invoke(gen_wc_inputs).content
                    #if need_content else
                    self.sparql_gen_with_ids_chain.invoke(gen_wc_inputs).content
                )
                sparql_query = gen_wc_res_str
        else:
            retrieved_from_vs: list[Document] = []
            sparql_query = initial_query
        # Execute SPARQL query, if necessary
        if sparql_query.lower().startswith("sparql"):
            sparql_query = sparql_query[len("sparql"):]
        log_msg(f"SPARQL query:\n{sparql_query}", level=logging.DEBUG)
        if sparql_query != "":
            # Retrieve from KG
            reply = self.store_client.query(sparql_query)["results"]["bindings"]
            # TODO: Adjust prompts to limit query result item numbers,
            # instead of restricting item numbers here!
            retrieved_from_kg = query_result_pretty_str(reply,
                self.config.get(CVN_KG_MAX_ITEMS), additions=additions)
            # TODO: Write a function that retrieves from KG as list
            # of documents in order to unify retrieval. Investigate
            # beforehand if this is sensible at all, or if there is a
            # better way to structure retrieved information in general!
        else:
            retrieved_from_kg = ""
        log_msg(f"Retrieved from KG:\n{retrieved_from_kg}",
            level=logging.DEBUG)
        # Generate answer, based on retrieved info
        answer: str = self.answer_gen_chain.invoke({
            "context": retrieved_from_kg,
            "speeches": retrieved_from_vs,
            "question": question
        }).content

        # TODO: Extend reference extraction to KG-retrieved results!
        # Ideally, treat context in a single, unified way, irrespective of
        # where it was retrieved from!
        return {
            self.answer_key: answer,
            self.sources_key: extract_references(answer, retrieved_from_vs)
        }
