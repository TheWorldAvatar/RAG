import json
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from rdflib import Graph

class StoreClient:

    def __init__(self, url: str) -> None:
        self._url = url

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

    def __init__(self,
        g: Graph | None = None, filename: str | None = None
    ) -> None:
        if g is None:
            self._g = Graph()
            if filename is not None:
                self._g.parse(filename)
        else:
            self._g = g

    def query(self, query_str: str) -> dict:
        reply = self._g.query(query_str)
        json_bytes = reply.serialize(format='json')
        if json_bytes is None:
            q_result = {}
        else:
            # Decode UTF-8 bytes to Unicode, and convert single quotes
            # to double quotes to make it a valid JSON string.
            json_str = json_bytes.decode('utf8').replace("'", '"')
            q_result = json.loads(json_str)
        return q_result

    def update(self, query_str: str) -> None:
        if query_str is not None and query_str != "":
            self._g.update(query_str)
