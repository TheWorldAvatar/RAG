import yaml
import os

# Configuration variable names
CVN_CHUNK_OVERLAP   = "ChunkOverlap"
CVN_CHUNK_SIZE      = "ChunkSize"
CVN_EMBEDDING_CACHE = "EmbeddingCacheDirectory"
CVN_EMBEDDING_DIM   = "EmbeddingDimension"
CVN_EMBEDDING_MODEL = "EmbeddingModel"
CVN_ENDPOINT        = "Endpoint"
CVN_MODEL           = "Model"
CVN_OPENAI_API_KEY  = "OPENAI_API_KEY"
CVN_TEMPERATURE     = "Temperature"
CVN_TBOX_ENDPOINT   = "TBoxEndpoint"
CVN_VS_COLLECTION   = "VectorStoreCollectionName"
CVN_VSTORE_CACHE    = "VectorStoreCacheDirectory"
CONFIG_VAR_NAMES = [CVN_ENDPOINT, CVN_MODEL, CVN_OPENAI_API_KEY, CVN_TEMPERATURE]

class RAGConfig:

    def __init__(self, yaml_file_name: str) -> None:
        """
        Reads a YAML configuration file.
        """
        with open(yaml_file_name, "r") as file_object:
            self._config = yaml.safe_load(file_object)

    def get(self, var_name: str):
        """
        Returns the value of the variable with the given name.
        """
        if var_name in self._config:
            return self._config[var_name]
        else:
            raise NameError(f"No '{var_name}' provided in configuration!")

    def check(self) -> None:
        """
        Checks for presence and non-emptiness of all expected
        configuration variables.
        """
        for v in CONFIG_VAR_NAMES:
            if self.get(v) is None:
                raise ValueError(f"The '{v}' provided "
                    f"in the configuration file is empty!")

    def set_openai_api_key(self) -> None:
        """
        Sets the configured OpenAI API key as an environment variable.
        """
        new_key = self.get(CVN_OPENAI_API_KEY)
        if new_key != "":
            if os.environ.get(CVN_OPENAI_API_KEY):
                print(f"WARNING: Environment variable '{CVN_OPENAI_API_KEY}' is set.")
                print("It will be overridden by the value provided in the configuration file!")
            os.environ[CVN_OPENAI_API_KEY] = new_key
        else:
            if not os.environ.get(CVN_OPENAI_API_KEY):
                raise NameError(f"No '{CVN_OPENAI_API_KEY}' provided in either"
                    f"configuration file or environment variables!")
