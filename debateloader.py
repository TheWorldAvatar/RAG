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
        period: str=None,
        session: str=None
    ):
        """
        Initialise with store client.
        """
        self.store_client = store_client
        self.period = period
        self.session = session

    def lazy_load(self) -> Iterator[Document]:
        """
        Load from store client.
        """
        id_var_name = "ID"
        date_var_name = "Datum"
        period_var_name = "Wahlperiode"
        session_var_name = "Sitzungnr"
        text_var_name = "Text"
        speaker_var_name = "Redner"
        givenname_var_name = "Vorname"
        surname_var_name = "Nachname"
        party_var_name = "Fraktion"
        reading_var_name = "Lesung"
        # TODO: Don't hard-code the prefix!
        if self.period is None or self.session is None:
            # TODO: Add speaker and party, or remove this altogether.
            qstr = (
                'PREFIX pd: <https://www.theworldavatar.com/kg/ontoparlamentsdebatten/>\n'
                f'SELECT ?{id_var_name} ?{date_var_name} ?{period_var_name} ?{session_var_name} (GROUP_CONCAT(?Value; SEPARATOR=" ") AS ?{text_var_name}) WHERE\n'
                '{\n'
                f'  SELECT ?{id_var_name} ?Value ?{date_var_name} ?{period_var_name} ?{session_var_name} WHERE\n'
                '  {\n'
                '    ?r a pd:Rede .\n'
                f'    ?r pd:hatId ?{id_var_name} .\n'
                '    ?r pd:hatP ?p .\n'
                '    ?p pd:hatIndex ?Index .\n'
                '    ?p pd:hatValue ?Value .\n'
                '    ?s pd:hatSitzungsverlauf/pd:hatTagesordnungspunkt/pd:hatRede ?r .\n'
                f'    ?s pd:hatSitzung-datum ?{date_var_name} .\n'
                f'    ?s pd:hatWahlperiode ?{period_var_name} .\n'
                f'    ?s pd:hatSitzung-nr ?{session_var_name}\n'
                '  } ORDER BY ?Index\n'
                '}\n'
                f'GROUP BY ?{id_var_name} ?{date_var_name} ?{period_var_name} ?{session_var_name}'
            )
        else:
            qstr = (
                'PREFIX pd: <https://www.theworldavatar.com/kg/ontoparlamentsdebatten/>\n'
                f'SELECT ?{id_var_name} ?{date_var_name} ?{text_var_name} ?{givenname_var_name} ?{surname_var_name} ?{party_var_name} ?{reading_var_name} WHERE\n'
                '{\n'
                '  ?r a pd:Rede .\n'
                f'  ?r pd:hatId ?{id_var_name} .\n'
                f'  ?r pd:hatDatum ?{date_var_name} .\n'
                f'  ?r pd:hatText ?{text_var_name} .\n'
                '  ?s pd:hatSitzungsverlauf/pd:hatTagesordnungspunkt ?top .\n'
                '  ?top pd:hatRede ?r .\n'
                f'  ?s pd:hatWahlperiode "{self.period}" .\n'
                f'  ?s pd:hatSitzung-nr "{self.session}" .\n'
                f'  ?r pd:hatRedner ?redner .\n'
                '  OPTIONAL {'
                f'    ?redner pd:hatFraktion/pd:hatName_kurz ?{party_var_name}'
                '  } .\n'
                '  OPTIONAL {'
                f'    ?top pd:hatLesung ?{reading_var_name}'
                '  } .\n'
                f'  ?redner pd:hatVorname ?{givenname_var_name} .\n'
                f'  ?redner pd:hatNachname ?{surname_var_name}\n'
                '}'
            )
        try:
            speeches = self.store_client.query(qstr)["results"]["bindings"]
        except Exception as e:
            raise RuntimeError(f"Error querying speeches from store client!") from e

        for speech in speeches:
            if self.period is None or self.session is None:
                period = speech[period_var_name]["value"]
                session = speech[session_var_name]["value"]
            else:
                period = self.period
                session = self.session
            metadata = {
                id_var_name: speech[id_var_name]["value"],
                date_var_name: speech[date_var_name]["value"],
                period_var_name: period,
                session_var_name: session,
                reading_var_name: speech[reading_var_name]["value"] if reading_var_name in speech else "",
                speaker_var_name: " ".join(
                    [speech[givenname_var_name]["value"], speech[surname_var_name]["value"]]
                ),
                party_var_name: speech[party_var_name]["value"] if party_var_name in speech else ""
            }
            yield Document(page_content=speech[text_var_name]["value"],
                metadata=metadata)
