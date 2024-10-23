import json
from time import strftime
from rdflib import URIRef
import logging
logger = logging.getLogger(__name__)

# Encoding string
ES_UTF_8 = "UTF-8"

TWA_BASE_IRI = "https://www.theworldavatar.com/kg/"

PROP_HAS_PREFIX = "hat"

# TBox customisation keys
TC_DELETIONS    = "deletions"
TC_SHORTCUTS    = "shortcuts"
TC_SHORTCUTS_WP = "shortcuts_with_parents"
TC_INDEX_FIELDS = "index_fields"
TC_REPLACEMENTS = "replacements"

def log_msg(msg: str, level = logging.INFO) -> None:
    """
    Utility function that prints a message to the console and
    appends the same message to a log file for record keeping.
    """
    timestamp = strftime('%Y-%m-%dT%H:%M:%S')
    logger.log(level, f"{timestamp}: {msg}")
    print(f"{timestamp}: WARNING: {msg}" if level == logging.WARN
        else f"{timestamp}: {msg}")

def export_dict_to_json(d: dict, filename: str) -> None:
    with open(filename, "w", encoding=ES_UTF_8) as outfile:
        json.dump(d, outfile, indent=2, ensure_ascii=False)

def make_rel_iri(base_iri: str, class_name: str) -> str:
    """
    Returns the full IRI string of a relationship (object or data
    type property), consisting of a base IRI followed by "has" and
    a capitalised class name.
    """
    return "".join([base_iri, PROP_HAS_PREFIX, class_name.capitalize()])

def make_rel_ref(base_iri: str, class_name: str) -> URIRef:
    return URIRef(make_rel_iri(base_iri, class_name))
