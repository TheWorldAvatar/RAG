import json
from time import strftime
from rdflib import Namespace, URIRef
import logging
logger = logging.getLogger(__name__)

from CommonNamespaces import *
from storeclient import StoreClient
from SPARQLBuilder import make_prefix_str

# Encoding string
ES_UTF_8 = "UTF-8"

TWA_BASE_IRI = "https://www.theworldavatar.com/kg/"

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

# Property naming
PROP_HAS_PREFIX = "hat"
PROP_NAME_CLASH_ADDENDUM = "_lit"

# Comment activities
CA_APPLAUSE       = "Beifall"
CA_MERRYMENT      = "Heiterkeit"
CA_LAUGHTER       = "Lachen"
CA_CONTRADICTION  = "Widerspruch"
CA_UNREST         = "Unruhe"
CA_CALL           = "Zuruf"
CA_COUNTERCALL    = "Gegenruf"
CA_INTERJECTION   = "Zwischenruf"
COMMENT_ACTIVITIES = [CA_APPLAUSE, CA_MERRYMENT,
    CA_LAUGHTER, CA_CONTRADICTION, CA_UNREST]
COMMENT_ACTIVITIES_LONG = COMMENT_ACTIVITIES.copy()
COMMENT_ACTIVITIES_LONG.extend([CA_CALL, CA_COUNTERCALL])
# Comment relationships (without the 'has' prefixed)
CR_GROUP_WHOLE = "fraktion_ganz"

# Format strings
FMT_DATE = "%Y-%m-%d"
FMT_TIME = "%H:%M:%S"
FMT_DATE_TIME = FMT_DATE + "T" + FMT_TIME

prefixes = {
    "owl": """PREFIX owl: <http://www.w3.org/2002/07/owl#>\n""",
    "rdf": """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n""",
    "rdfs": """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n""",
    "xsd": """PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n""",
}

cls_owl_tbox_query = prefixes["rdfs"] + prefixes["owl"] + (
    """SELECT DISTINCT ?iri ?com\n"""
    """WHERE {\n"""
    """    ?iri a owl:Class .\n"""
    """    OPTIONAL { ?iri rdfs:comment ?com } .\n"""
    """    FILTER isIRI(?iri) \n"""
    """}"""
)

def log_msg(msg: str, level = logging.INFO) -> None:
    """
    Utility function that prints a message to the console and
    appends the same message to a log file for record keeping.
    """
    timestamp = strftime(FMT_DATE_TIME)
    logger.log(level, f"{timestamp}: {msg}")
    print(f"{timestamp}: WARNING: {msg}" if level == logging.WARN
        else f"{timestamp}: {msg}")

def export_dict_to_json(d: dict, filename: str) -> None:
    with open(filename, "w", encoding=ES_UTF_8) as outfile:
        json.dump(d, outfile, indent=2, ensure_ascii=False)

def read_text_from_file(filename: str) -> str:
    with open(filename, "r", encoding=ES_UTF_8) as f:
        return f.read().strip()

def make_rel_name(class_name: str) -> str:
    return "".join([PROP_HAS_PREFIX, class_name.capitalize()])

def make_rel_iri(base_iri: str, class_name: str) -> str:
    """
    Returns the full IRI string of a relationship (object or data
    type property), consisting of a base IRI followed by "has" and
    a capitalised class name.
    """
    return "".join([base_iri, make_rel_name(class_name)])

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

def _describe_iri(res: dict, prefixes: dict[str, str],
    include_range: bool=True) -> str:
    iri = res["iri"]["value"]
    ns_iri = namespace_name_or_iri(iri, prefixes, "")
    additions = []
    if "dom" in res:
        domain = res["dom"]["value"] if "dom" in res else ""
        ns_domain = namespace_name_or_iri(domain, prefixes, "")
        additions.append(ns_domain)
    if include_range and "rng" in res:
        range = res["rng"]["value"]
        ns_range = namespace_name_or_iri(range, prefixes, "")
        additions.append(ns_range)
    if "com" in res:
        comment = res["com"]["value"]
        additions.append(comment)
    if len(additions) > 0:
        add_str = f" ({', '.join(additions)})"
    else:
        add_str = ""
    return f"{ns_iri}{add_str}"

def make_prop_tbox_query(prop_type: str) -> str:
    return prefixes["rdf"] + prefixes["rdfs"] + prefixes["owl"] + (
        'SELECT ?iri ?dom ?rng ?com\n'
        'WHERE {\n'
        '  { {\n'
        '    SELECT DISTINCT ?iri ?dom\n'
        '    WHERE {\n'
        f'      ?iri a {prop_type} .\n'
        '      ?iri rdfs:domain ?dom .\n'
        '      FILTER isIRI(?dom)\n'
        '    }\n'
        '  } UNION {\n'
        '    SELECT DISTINCT ?iri (GROUP_CONCAT(?domain; SEPARATOR=" UNION ") AS ?dom)\n'
        '    WHERE {\n'
        f'      ?iri a {prop_type} .\n'
        '      ?iri rdfs:domain ?domain_b .\n'
        '      ?domain_b owl:unionOf ?union_b .\n'
        '      ?union_b rdf:rest* ?o .\n'
        '      ?o rdf:first ?domain\n'
        '    } GROUP BY ?iri\n'
        '  } } .\n'
        '  ?iri rdfs:range ?rng .\n'
        '  OPTIONAL { ?iri rdfs:comment ?com }\n'
        '}'
    )

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
        f"is followed by its domain (where multiple domains are separated by the UNION keyword) and range and optionally its "
        f"description in parentheses:\n"
        f"{ops}\n"
        f"The schema provides the following datatype properties, "
        f"i.e. relationships between objects and literals, where "
        f"each property is followed by its domain (where multiple domains are separated by the UNION keyword) and optionally its "
        f"description in parentheses:\n"
        f"{dtps}\n"
    )

def get_store_schema(sc: StoreClient, prefixes: dict[str, str]) -> str:
    prefixes_str = "\n".join(
        make_prefix_str(p, prefixes[p]) for p in prefixes)
    classes = sc.query(cls_owl_tbox_query)["results"]["bindings"]
    classes_str = "\n".join([_describe_iri(r, prefixes) for r in classes])
    op_owl_tbox_query = make_prop_tbox_query("owl:ObjectProperty")
    ops = sc.query(op_owl_tbox_query)["results"]["bindings"]
    ops_str = "\n".join([_describe_iri(r, prefixes) for r in ops])
    dp_owl_tbox_query = make_prop_tbox_query("owl:DatatypeProperty")
    dtps = sc.query(dp_owl_tbox_query)["results"]["bindings"]
    dtps_str = "\n".join(
        [_describe_iri(r, prefixes, include_range=False) for r in dtps])
    return assemble_schema_description(
        prefixes_str, classes_str, ops_str, dtps_str)
