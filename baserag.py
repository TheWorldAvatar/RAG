from datetime import datetime
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.embeddings import CacheBackedEmbeddings
from langchain.storage import LocalFileStore
from langchain_core.runnables import RunnablePassthrough
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams

from common import *
from ragconfig import *
from debateloader import DebateLoader
from questions import Questions, Answer

class BaseRAG:

    def __init__(self, config: RAGConfig) -> None:
        # https://platform.openai.com/docs/guides/embeddings/
        underlying_embeddings = OpenAIEmbeddings(
            model=config.get(CVN_EMBEDDING_MODEL)
        )
        self.emb_cache_store = LocalFileStore(config.get(CVN_EMBEDDING_CACHE))
        self.embeddings = CacheBackedEmbeddings.from_bytes_store(
            underlying_embeddings, self.emb_cache_store,
            namespace=underlying_embeddings.model
        )
        self._init_vector_store(config)
        self.llm = ChatOpenAI(
            model=config.get(CVN_MODEL),
            temperature=config.get(CVN_TEMPERATURE)
        )
        self._init_chain(config)

    def _init_vector_store(self, config: RAGConfig) -> None:
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
                embedding=self.embeddings
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
                embedding=self.embeddings
            )

    def _init_chain(self, config: RAGConfig) -> None:
        prompt = PromptTemplate(
            template=read_text_from_file(
                os.path.join("prompt_templates", "base_answer_gen.txt")
            ),
            input_variables=["context", "question"]
        )
        # Retriever for the "top k" most similar documents.
        retriever = self.vector_store.as_retriever(
            search_type="similarity", # This is the default search type.
            search_kwargs={
                "k": config.get(CVN_TOP_K) # Defaults to 4, apparently, if not given.
            }
        )
        self.chain = (
            {
                "context": retriever,
                "question": RunnablePassthrough()
            }
            | prompt
            | self.llm
            | StrOutputParser()
        )

    def load_debates(self, filename: str,
        chunk_size: int, chunk_overlap: int) -> list[str]:
        """
        Loads a single JSON file that was returned from the DIP API
        for the debate minute text resource type, chunks the text field
        of each debate, and adds the chunks to the vector store.
        WARNING: This will potentially calculate embeddings for all chunks,
        if they are not cached already, so this may cost real money
        and may be expensive!
        """
        raw_documents = DebateLoader(filename).load()
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=["\n\n\n", "\n"]
        )
        documents = text_splitter.split_documents(raw_documents)
        log_msg(f"Adding {len(documents)} chunks split from "
            f"{len(raw_documents)} documents to the vector store...")
        return self.vector_store.add_documents(documents)

    def query(self, question: str) -> str:
        """
        Returns an answer as a string to a given natural
        language question.
        WARNING: This may cost real money and may be expensive!
        """
        try:
            response = self.chain.invoke(question)
            return response
        except Exception as e:
            return f"Error processing query: {str(e)}"

def main():
    logging.basicConfig(filename="baserag.log", encoding=ES_UTF_8,
        level=logging.INFO)
    config = RAGConfig("config.yaml")
    config.check()
    config.set_openai_api_key()
    rag = BaseRAG(config)

    # Load documents, i.e. embed and store in vector store.
    # WARNING: 1) This may be expensive! 2) Do not load documents
    # that have already been loaded. This creates duplicates!
    download_folder = os.path.join("data", "raw")
    debate_filename = os.path.join(download_folder,
        "plenarprotokoll-text-2023-20137.json")
        #"plenarprotokoll-text-2023-AoJwkMqOoYsDNFBsZW5hcnByb3Rva29sbC01NjAx.json")
        #"plenarprotokoll-text-1998-AoJw-N24mdQBNFBsZW5hcnByb3Rva29sbC0xMTM1.json")
    #rag.load_debates(debate_filename,
    #    config.get(CVN_CHUNK_SIZE), config.get(CVN_CHUNK_OVERLAP))

    q_catalogue_name = "questions-mine"
    q_cat_save_filename = os.path.join("data",
        "".join([q_catalogue_name, "-with-answers", ".json"]))
    q_cat_load_filename = (q_cat_save_filename if
        os.path.isfile(q_cat_save_filename) else
        os.path.join("data", "".join([q_catalogue_name, ".json"])))
    questions = Questions()
    questions.load(q_cat_load_filename)
    question = questions.find_question_by_id("2")
    nlq = question.get_text()
    log_msg(f"Frage: {nlq}")
    answer = rag.query(nlq)
    log_msg(f"Antwort: {answer}")
    question.add_answer(Answer(answer, "Base-RAG", datetime.now()))
    questions.save(q_cat_save_filename)

if __name__ == "__main__":
    main()
