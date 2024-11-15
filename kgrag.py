import os
from langchain_openai import ChatOpenAI

from ragconfig import *
import storeclient
from kgqachain import KGQAChain

config = RAGConfig("config.yaml")
config.check()
config.set_openai_api_key()

with open(os.path.join("data", "processed", "MDB_STAMMDATEN-xml-tbox-description.txt"), "r") as f:
    schema = f.read()

sc = storeclient.RemoteStoreClient(config.get(CVN_ENDPOINT))

llm = ChatOpenAI(
    model=config.get(CVN_MODEL),
    temperature=config.get(CVN_TEMPERATURE)
)

chain = KGQAChain.from_llm(
    llm, store_client=sc, schema_description=schema,
    verbose=True, return_sparql_query=True
)

nlq = "Welche ID hat das MdB Adenauer?"
#nlq = "Welchen Nachnamen hat das MdB mit ID 11000009?"
#nlq = "Wann wurde Willy Brandt geboren?"
result = chain.invoke(nlq)
print(f"Frage: {result[chain.input_key]}")
print(f"Antwort: {result[chain.output_key]}")
