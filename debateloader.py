from pathlib import Path
from typing import Iterator, Union
import json

from langchain_community.document_loaders.base import BaseLoader
from langchain_core.documents import Document

from common import *

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
