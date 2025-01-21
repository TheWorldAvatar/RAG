import os
from langchain_openai import ChatOpenAI

from common import *
from ragconfig import *
from storeclient import RemoteStoreClient
from kgqachain import KGQAChain

class KGRAG:

    def __init__(self, config: RAGConfig) -> None:
        sc = RemoteStoreClient(config.get(CVN_ENDPOINT))
        #schema = read_text_from_file(
        #    os.path.join("data", "processed",
        #    "MDB_STAMMDATEN-xml-tbox-description.txt"))
        schema = get_store_schema(
            RemoteStoreClient(config.get(CVN_TBOX_ENDPOINT)),
            {MMD_PREFIX: MMD_BASE_IRI, PD_PREFIX: PD_BASE_IRI}
        )
        log_msg(schema)
        llm = ChatOpenAI(
            model=config.get(CVN_MODEL),
            temperature=config.get(CVN_TEMPERATURE)
        )
        self.chain = KGQAChain.from_llm(
            llm, store_client=sc, schema_description=schema,
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

    #nlq = "Welche ID hat das MdB Adenauer?"
    #nlq = "Welchen Nachnamen hat das MdB mit ID 11000009?"
    #nlq = "Wann wurde Willy Brandt geboren?"
    nlq = "Wie viele Reden haben mehr als einen Redner?"
    log_msg(f"Frage: {nlq}")
    answer = rag.query(nlq)
    log_msg(f"Antwort: {answer}")

if __name__ == "__main__":
    main()
