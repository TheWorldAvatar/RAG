import yaml
import os
from langchain_openai import ChatOpenAI

import storeclient
from kgqachain import KGQAChain

# Configuration variable names
CVN_ENDPOINT       = "Endpoint"
CVN_MODEL          = "Model"
CVN_OPENAI_API_KEY = "OPENAI_API_KEY"
CVN_TEMPERATURE    = "Temperature"
CONFIG_VAR_NAMES = [CVN_ENDPOINT, CVN_MODEL, CVN_OPENAI_API_KEY, CVN_TEMPERATURE]

# Read configuration file and check contents
with open("config.yaml", "r") as file_object:
    config = yaml.safe_load(file_object)
for v in CONFIG_VAR_NAMES:
    if v in config:
        if config[v] is None:
            raise ValueError(f"The '{v}' provided "
                f"in the configuration file is empty!")
    else:
        raise NameError(f"No '{v}' provided in configuration file!")

# Set OpenAI API key
if config[CVN_OPENAI_API_KEY] != "":
    if os.environ.get(CVN_OPENAI_API_KEY):
        print(f"WARNING: Environment variable '{CVN_OPENAI_API_KEY}' is set.")
        print("It will be overridden by the value provided in the configuration file!")
    os.environ[CVN_OPENAI_API_KEY] = config[CVN_OPENAI_API_KEY]
else:
    if not os.environ.get(CVN_OPENAI_API_KEY):
        raise NameError(f"No '{CVN_OPENAI_API_KEY}' provided in either"
            f"configuration file or environment variables!")

sc = storeclient.RemoteStoreClient(config[CVN_ENDPOINT])

llm = ChatOpenAI(temperature=config[CVN_TEMPERATURE], model=config[CVN_MODEL])

chain = KGQAChain.from_llm(
    llm, store_client=sc,
    verbose=True, return_sparql_query=True
)

nlq = "Welche ID hat das MdB Adenauer?"
result = chain.invoke(nlq)
print(f"Frage: {result[chain.input_key]}")
print(f"Antwort: {result[chain.output_key]}")
