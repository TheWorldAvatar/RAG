from pathlib import Path
from typing import Iterator, Union
import json

from langchain_community.document_loaders.base import BaseLoader
from langchain_core.documents import Document

from common import *
from storeclient import StoreClient

class DebateLoader(BaseLoader):
    """
    Load debate texts from a JSON file.
    """

    def __init__(
        self,
        file_path: Union[str, Path],
    ):
        """
        Initialise with file path.
        """
        self.file_path = file_path

    def lazy_load(self) -> Iterator[Document]:
        """
        Load from file path.
        """
        try:
            with open(self.file_path, "r", encoding=ES_UTF_8) as infile:
                json_str = infile.read()
                debate_dict = json.loads(json_str)
        except Exception as e:
            raise RuntimeError(f"Error loading {self.file_path}!") from e

        for doc in debate_dict["documents"]:
            if "text" in doc:
                if doc["text"] != "[NoTextAvailable]":
                    metadata = {
                        "source": str(self.file_path),
                        "dokumentnummer": doc["dokumentnummer"]
                    }
                    yield Document(page_content=doc["text"], metadata=metadata)

class SpeechKGLoader(BaseLoader):
    """
    Load speech texts queried from a knowledge graph.
    """

    def __init__(
        self,
        store_client: StoreClient,
    ):
        """
        Initialise with store client.
        """
        self.store_client = store_client

    def lazy_load(self) -> Iterator[Document]:
        """
        Load from store client.
        """
        # TODO: Don't hard-code the prefix!
        qstr = (
            """PREFIX pd: <https://www.theworldavatar.com/kg/ontoparlamentsdebatten/>\n"""
            """SELECT ?ID (GROUP_CONCAT(?Value; SEPARATOR=" ") AS ?Text) WHERE {\n"""
            """  SELECT ?ID ?Value WHERE {\n"""
            """    ?r a pd:Rede .\n"""
            """    ?r pd:hatId ?ID .\n"""
            """    ?r pd:hatP ?p .\n"""
            """    ?p pd:hatIndex ?Index .\n"""
            """    ?p pd:hatValue ?Value\n"""
            """  } ORDER BY ?Index\n"""
            """} GROUP BY ?ID"""
        )
        try:
            speeches = self.store_client.query(qstr)["results"]["bindings"]
        except Exception as e:
            raise RuntimeError(f"Error querying speeches from store client!") from e

        for speech in speeches:
            metadata = {
                "source": speech["ID"]["value"]
            }
            yield Document(page_content=speech["Text"]["value"],
                metadata=metadata)
