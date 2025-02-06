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
from pydantic import Field

from common import *
from ragconfig import *
from storeclient import StoreClient

class RunnableExtractSource(Runnable):
    """
    Extracts the source from the metadata of a list of documents.
    """

    def invoke(
        self, input_data: List[Document], config: Dict[str, Any] = None
    ) -> List[str]:
        return [d.metadata["ID"] for d in input_data]

class RunnableLogInputs(Runnable):

    def invoke(
        self, input_data: StringPromptValue, config: Dict[str, Any] = None
    ) -> StringPromptValue:
        log_msg(input_data.to_string())
        return input_data

def query_result_pretty_str(result: list[dict[str, str]]) -> str:
    str_list = ["["]
    for r in result:
        str_list.append("(")
        for varname in r:
            value = r[varname]["value"]
            if not value.startswith("http"):
                str_list.append(f"{varname}: {value},")
        str_list.append("),")
    str_list.append("]")
    return "".join(str_list)

class HybridQAChain(Chain):
    store_client: StoreClient = Field(exclude=True)
    schema_description: str
    threshold_retriever: VectorStoreRetriever = Field(exclude=True)
    top_k_retriever: VectorStoreRetriever = Field(exclude=True)
    sparql_gen_or_retrieve_chain: RunnableSequence
    need_content_chain: RunnableSequence
    sparql_gen_with_ids_chain: RunnableSequence
    sparql_gen_with_docs_chain: RunnableSequence
    answer_gen_chain: RunnableSequence
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
        sparql_gen_prompt: PromptTemplate,
        need_content_prompt: PromptTemplate,
        sparql_gen_with_ids_prompt: PromptTemplate,
        sparql_gen_with_docs_prompt: PromptTemplate,
        answer_gen_prompt: PromptTemplate,
        *,
        schema_description: str,
        **kwargs: Any,
    ) -> HybridQAChain:
        """Initialise from LLM."""
        sparql_gen_or_retrieve_chain = (
            sparql_gen_prompt
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
            need_content_chain=need_content_chain,
            sparql_gen_or_retrieve_chain=sparql_gen_or_retrieve_chain,
            sparql_gen_with_ids_chain=sparql_gen_with_ids_chain,
            sparql_gen_with_docs_chain=sparql_gen_with_docs_chain,
            answer_gen_chain=answer_gen_chain,
            schema_description=schema_description,
            **kwargs,
        )

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

        # Ask the LLM to generate a SPARQL query. If the query involves
        # filtering the textual content of speeches, then this means we need
        # to retrieve the latter from the vector store first. The LLM should
        # tell us that.
        schema_and_question_inputs = {
            "schema": self.schema_description,
            "question": question
        }
        gen_res_str: str = self.sparql_gen_or_retrieve_chain.invoke(
            schema_and_question_inputs).content
        query_or_retrieve = gen_res_str.strip(" .`")
        if query_or_retrieve.lower().startswith("retrieve"):
            # The LLM told us to retrieve documents from the vector store first.
            topic = query_or_retrieve[len("retrieve"):].strip(' ."')
            log_msg(f"Topic to be retrieved from vector store: '{topic}'")
            need_content_str: str = self.need_content_chain.invoke(
                schema_and_question_inputs).content
            ncs = need_content_str.lower().strip(' ."')
            log_msg(f"Is speech content to be retrieved: '{need_content_str}'")
            need_content = ("yes" in ncs) or ("ja" in ncs)
            if need_content:
                retriever = self.top_k_retriever
            else:
                retriever = self.threshold_retriever | RunnableExtractSource()
            retrieved_from_vs = retriever.invoke(topic)
            if need_content:
                # If the content of the speeches is needed, then we don't need
                # to query the KG at all.
                sparql_query = ""
            else:
                # Otherwise, try again to generate a query, this time with
                # retrieved information as context.
                gen_wc_inputs = {
                    "schema": self.schema_description,
                    "context": retrieved_from_vs,
                    "question": question
                }
                gen_wc_res_str: str = (
                    #self.sparql_gen_with_docs_chain.invoke(gen_wc_inputs).content
                    #if need_content else
                    self.sparql_gen_with_ids_chain.invoke(gen_wc_inputs).content
                )
                sparql_query = gen_wc_res_str
        else:
            retrieved_from_vs: list[Document] = []
            sparql_query = query_or_retrieve
        # Execute SPARQL query, if necessary
        if sparql_query.lower().startswith("sparql"):
            sparql_query = sparql_query[len("sparql"):]
        log_msg(f"SPARQL query:\n{sparql_query}")
        if sparql_query != "":
            reply = self.store_client.query(sparql_query)["results"]["bindings"]
            retrieved_from_kg = query_result_pretty_str(reply)
        else:
            retrieved_from_kg = ""
        log_msg(f"Retrieved from KG:\n{retrieved_from_kg}")
        # Generate answer, based on retrieved info
        answer: str = self.answer_gen_chain.invoke({
            "context": retrieved_from_kg,
            "speeches": retrieved_from_vs,
            "question": question
        }).content

        return {self.output_key: answer}
