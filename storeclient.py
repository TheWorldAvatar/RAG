from SPARQLWrapper import SPARQLWrapper, JSON, POST
from rdflib import Graph
import json

class StoreClient:

    def __init__(self, URL) -> None:
        self._url = URL

    def url(self):
        return self._url

    def query(self, query_str: str) -> dict:
        raise Exception(f"Query method is not implemented "
            f"for abstract {self.__class__.__name__} class!")

    def update(self, query_str: str) -> None:
        raise Exception(f"Update method is not implemented "
            f"for abstract {self.__class__.__name__} class!")

class RemoteStoreClient(StoreClient):

    def query(self, query_str: str) -> dict:
        w = SPARQLWrapper(self.url())
        w.setReturnFormat(JSON)
        w.setQuery(query_str)
        return w.query().convert()

    def update(self, query_str: str) -> None:
        if query_str is not None and query_str != "":
            w = SPARQLWrapper(self.url())
            w.setMethod(POST)
            w.setQuery(query_str)
            w.query()

class RdflibStoreClient(StoreClient):

    def __init__(self, g: Graph=None) -> None:
        self._g = Graph() if g is None else g

    def query(self, query_str: str) -> dict:
        reply = self._g.query(query_str)
        JsonBytes = reply.serialize(format='json')
        # Decode UTF-8 bytes to Unicode, and convert single quotes 
        # to double quotes to make it a valid JSON string.
        JsonStr = JsonBytes.decode('utf8').replace("'", '"')
        return json.loads(JsonStr)

    def update(self, query_str: str) -> None:
        if query_str is not None and query_str != "":
            self._g.update(query_str)
