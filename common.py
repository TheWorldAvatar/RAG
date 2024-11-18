import json
from time import strftime
from rdflib import Namespace, URIRef
import logging
logger = logging.getLogger(__name__)

from CommonNamespaces import *

# Encoding string
ES_UTF_8 = "UTF-8"

TWA_BASE_IRI = "https://www.theworldavatar.com/kg/"

PROP_HAS_PREFIX = "hat"

# Namespaces
MMD_PREFIX = "msd"
MMD_BASE_IRI = TWA_BASE_IRI+"ontomdbstammdaten/"
MMD_NAMESPACE = Namespace(MMD_BASE_IRI)
PD_PREFIX = "pd"
PD_BASE_IRI = TWA_BASE_IRI+"ontoparlamentsdebatten/"
PD_NAMESPACE = Namespace(PD_BASE_IRI)

# TBox customisation keys
TC_DELETIONS    = "deletions"
TC_SHORTCUTS    = "shortcuts"
TC_SHORTCUTS_WP = "shortcuts_with_parents"
TC_INDEX_FIELDS = "index_fields"
TC_REPLACEMENTS = "replacements"

# Literal data type strings
LDTS_DATE    = expandIRI(XSD_DATE, default_prefixes)
LDTS_INTEGER = expandIRI(XSD_INTEGER, default_prefixes)
LDTS_STRING  = expandIRI(XSD_STRING, default_prefixes)
LDTS_TIME    = expandIRI(XSD_TIME, default_prefixes)

field_data_type_map = {
    "datum": LDTS_DATE,
    "_von": LDTS_DATE,
    "_bis": LDTS_DATE,
    "uhrzeit": LDTS_TIME
}

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

def substr_map_or_default(the_str: str, map: dict[str, str],
    default: str) -> str:
    """
    If any key in the map is found as a substring of the lower-cased
    given string, the corresponding map value is returned, otherwise
    the given default value.
    """
    for key in map:
        if key in the_str.lower():
            return map[key]
    # If we cannot find a match in the map, return the default.
    return default

def get_field_data_type_iri(field_name: str) -> str:
    return substr_map_or_default(
        field_name, field_data_type_map, LDTS_STRING)

def assemble_schema_description(prefixes: str, classes: str,
    ops: str, dtps: str) -> str:
    return (
        f"The schema uses the following prefixes:\n"
        f"{prefixes}\n"
        f"The schema provides the following node types, "
        f"where each node type is optionally followed by its "
        f"description in parentheses:\n"
        f"{classes}\n"
        f"The schema provides the following object properties, "
        f"i.e. relationships between objects, where each property "
        f"is followed by its domain and range and optionally its "
        f"description in parentheses:\n"
        f"{ops}\n"
        f"The schema provides the following datatype properties, "
        f"i.e. relationships between objects and literals, where "
        f"each property is followed by its domain and optionally its "
        f"description in parentheses:\n"
        f"{dtps}\n"
    )
