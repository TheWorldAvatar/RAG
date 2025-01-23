import os
from datetime import datetime
from langchain_openai import ChatOpenAI
from langchain_core.prompts.prompt import PromptTemplate

from common import *
from ragconfig import *
from storeclient import RemoteStoreClient
from kgqachain import KGQAChain
from questions import Questions, Answer

class KGRAG:

    def __init__(self, config: RAGConfig) -> None:
        store_client = RemoteStoreClient(config.get(CVN_ENDPOINT))
        #schema = read_text_from_file(
        #    os.path.join("data", "processed",
        #    "MDB_STAMMDATEN-xml-tbox-description.txt"))
        schema = get_store_schema(store_client,
            {MMD_PREFIX: MMD_BASE_IRI, PD_PREFIX: PD_BASE_IRI})
        log_msg(schema)
        llm = ChatOpenAI(
            model=config.get(CVN_MODEL),
            temperature=config.get(CVN_TEMPERATURE)
        )
        sparql_gen_prompt = PromptTemplate(
            template=read_text_from_file(
                os.path.join("prompt_templates", "kg_sparql_gen.txt")
            ),
            input_variables=["schema", "prompt"]
        )
        sparql_qa_prompt = PromptTemplate(
            template=read_text_from_file(
                os.path.join("prompt_templates", "kg_sparql_qa.txt")
            ),
            input_variables=["context", "prompt"]
        )
        self.chain = KGQAChain.from_llm(
            llm, sparql_gen_prompt, sparql_qa_prompt,
            store_client=store_client, schema_description=schema,
            verbose=True, return_sparql_query=True
        )

    def query(self, question: str) -> str:
        """
        Returns an answer as a string to a given natural
        language question.
        WARNING: This may cost real money and may be expensive!
        """
        try:
            response = self.chain.invoke(question)
            return response[self.chain.output_key]
        except Exception as e:
            return f"Error processing query: {str(e)}"

def main():
    logging.basicConfig(filename="kgrag.log", encoding=ES_UTF_8,
        level=logging.INFO)
    config = RAGConfig("config.yaml")
    config.check()
    config.set_openai_api_key()
    rag = KGRAG(config)

    q_catalogue_name = "questions-mine"
    q_cat_save_filename = os.path.join("data",
        "".join([q_catalogue_name, "-with-answers", ".json"]))
    q_cat_load_filename = (q_cat_save_filename if
        os.path.isfile(q_cat_save_filename) else
        os.path.join("data", "".join([q_catalogue_name, ".json"])))
    questions = Questions()
    questions.load(q_cat_load_filename)
    question = questions.find_question_by_id("1")
    nlq = question.get_text()
    log_msg(f"Frage: {nlq}")
    answer = rag.query(nlq)
    log_msg(f"Antwort: {answer}")
    question.add_answer(Answer(answer, "KG-RAG", datetime.now()))
    questions.save(q_cat_save_filename)

if __name__ == "__main__":
    main()
