from datetime import datetime
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.embeddings import CacheBackedEmbeddings
from langchain.storage import LocalFileStore
from langchain.prompts import PromptTemplate
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams

from common import *
from ragconfig import *
from hybridqachain import HybridQAChain
from storeclient import RemoteStoreClient
from debateloader import SpeechKGLoader
from questions import Questions, Answer

class HybridRAG:

    def __init__(self, config: RAGConfig) -> None:
        self.store_client = RemoteStoreClient(config.get(CVN_ENDPOINT))
        schema = get_store_schema(
            RemoteStoreClient(config.get(CVN_TBOX_ENDPOINT)),
            {MMD_PREFIX: MMD_BASE_IRI, PD_PREFIX: PD_BASE_IRI}
        )
        #schema = read_text_from_file(
        #    os.path.join("data", "processed", "20137-xml-tbox-description.txt"))
        #log_msg(schema)

        llm = ChatOpenAI(
            model=config.get(CVN_MODEL),
            temperature=config.get(CVN_TEMPERATURE)
        )
        sparql_gen_prompt = PromptTemplate(
            template=read_text_from_file(
                os.path.join("prompt_templates", "hybrid_sparql_gen.txt")
            ),
            input_variables=["schema", "question"]
        )
        sparql_classify_prompt = PromptTemplate(
            template=read_text_from_file(
                os.path.join("prompt_templates", "hybrid_sparql_classify.txt")
            ),
            input_variables=["query"]
        )
        sparql_gen_or_retrieve_prompt = PromptTemplate(
            template=read_text_from_file(
                os.path.join("prompt_templates", "hybrid_sparql_gen_or_ask_retrieve.txt")
            ),
            input_variables=["schema", "question"]
        )
        need_content_prompt = PromptTemplate(
            template=read_text_from_file(
                os.path.join("prompt_templates", "hybrid_need_content.txt")
            ),
            input_variables=["question"]
        )
        sparql_gen_with_ids_prompt = PromptTemplate(
            template=read_text_from_file(
                os.path.join("prompt_templates", "hybrid_sparql_gen_with_ids.txt")
            ),
            input_variables=["schema", "context", "question"]
        )
        sparql_gen_with_docs_prompt = PromptTemplate(
            template=read_text_from_file(
                os.path.join("prompt_templates", "hybrid_sparql_gen_with_docs.txt")
            ),
            input_variables=["schema", "context", "question"]
        )
        answer_gen_prompt = PromptTemplate(
            template=read_text_from_file(
                os.path.join("prompt_templates", "hybrid_answer_gen.txt")
            ),
            input_variables=["context", "speeches", "prompt"]
        )

        self._init_vector_store(config)
        # Similarity score threshold retrieval:
        # NB Maximum number k of retrieved documents is still used!
        threshold_retriever = self.vector_store.as_retriever(
            search_type="similarity_score_threshold",
            search_kwargs={
                "score_threshold": 0.7,
                "k": 1000
            }
        )
        # Retriever for the "top k" most similar documents.
        top_k_retriever = self.vector_store.as_retriever(
            search_type="similarity", # This is the default search type.
            search_kwargs={
                "k": config.get(CVN_TOP_K) # Defaults to 4, apparently, if not given.
            }
        )
        self.chain = HybridQAChain.from_llm(
            llm, sparql_gen_prompt, sparql_classify_prompt,
            sparql_gen_or_retrieve_prompt, need_content_prompt,
            sparql_gen_with_ids_prompt, sparql_gen_with_docs_prompt,
            answer_gen_prompt,
            threshold_retriever=threshold_retriever,
            top_k_retriever=top_k_retriever,
            store_client=self.store_client, schema_description=schema,
            verbose=True, return_sparql_query=True
        )

    def _init_vector_store(self, config: RAGConfig) -> None:
        # https://platform.openai.com/docs/guides/embeddings/
        underlying_embeddings = OpenAIEmbeddings(
            model=config.get(CVN_EMBEDDING_MODEL)
        )
        emb_cache_store = LocalFileStore(config.get(CVN_EMBEDDING_CACHE))
        embeddings = CacheBackedEmbeddings.from_bytes_store(
            underlying_embeddings, emb_cache_store,
            namespace=underlying_embeddings.model
        )
        collection_name = config.get(CVN_VS_COLLECTION)
        vs_cache_path = config.get(CVN_VSTORE_CACHE)
        # If the vector store cache directory exists, we attempt to
        # read an existing collection.
        if os.path.isdir(vs_cache_path):
            log_msg(f"Reading collection '{collection_name}' from "
                f"existing vector store in '{vs_cache_path}'...")
            self.vector_store = QdrantVectorStore.from_existing_collection(
                path=vs_cache_path,
                collection_name=collection_name,
                embedding=embeddings
            )
        else:
            log_msg(f"Creating new vector store in '{vs_cache_path}', "
                f"with new collection '{collection_name}'...")
            client = QdrantClient(path=vs_cache_path)
            # NB Unfortunately, at time of writing, there does not
            # seem to be a good way to determine the dimension of
            # the embedding, so we read that from config, too.
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=config.get(CVN_EMBEDDING_DIM),
                    distance=Distance.COSINE
                )
            )
            self.vector_store = QdrantVectorStore(
                client=client,
                collection_name=collection_name,
                embedding=embeddings
            )

    def load_speeches_from_kg(self, period: str=None,
        session: str=None) -> list[str]:
        """
        Queries speeches from the KG and loads them into the vector store.
        WARNING: This will potentially calculate embeddings for all speeches,
        if they are not cached already, so this may cost real money
        and may be expensive!
        """
        documents = SpeechKGLoader(self.store_client,
            period=period, session=session).load()
        log_msg(f"Adding {len(documents)} speeches queried from "
            "the store client to the vector store...")
        return self.vector_store.add_documents(documents)

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
    logging.basicConfig(filename="hybridrag.log", encoding=ES_UTF_8,
        level=logging.INFO)
    config = RAGConfig("config-hybrid.yaml")
    config.check()
    config.set_openai_api_key()
    rag = HybridRAG(config)

    # Load documents, i.e. embed and store in vector store.
    # WARNING: 1) This may be expensive! 2) Do not load documents
    # that have already been loaded. This creates duplicates!
    #rag.load_speeches_from_kg()
    #i = 0
    #for fn in reversed(os.listdir(os.path.join("data", "raw"))):
    #    if fn.endswith(".xml") and (
    #        fn.startswith("18") or fn.startswith("19") or fn.startswith("20")
    #    ):
    #        period = fn[0:2]
            # Strip leading zeros from session number string,
            # as that is not included in raw data.
    #        session = fn[2:5].lstrip("0")
    #        print(period,session)
    #        rag.load_speeches_from_kg(period=period, session=session)
    #        i += 1
            #if (i>=100):
            #    break
    #exit()

    q_catalogue_name = "questions-mine" #mine #A
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
    question.add_answer(Answer(answer, "Hybrid-RAG", datetime.now()))
    questions.save(q_cat_save_filename)

if __name__ == "__main__":
    main()
