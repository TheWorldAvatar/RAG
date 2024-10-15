NS_SEP = ':'

OWL_NS = 'owl'
OWL_TERM_ALLVALUESFROM = 'allValuesFrom'
OWL_ALLVALUESFROM = OWL_NS + NS_SEP + OWL_TERM_ALLVALUESFROM
OWL_TERM_CLASS = 'Class'
OWL_CLASS = OWL_NS + NS_SEP + OWL_TERM_CLASS
OWL_TERM_DATATYPEPROPERTY = 'DatatypeProperty'
OWL_DATATYPEPROPERTY = OWL_NS + NS_SEP + OWL_TERM_DATATYPEPROPERTY
OWL_TERM_NAMEDINDIVIDUAL = 'NamedIndividual'
OWL_NAMEDINDIVIDUAL = OWL_NS + NS_SEP + OWL_TERM_NAMEDINDIVIDUAL
OWL_TERM_OBJECTPROPERTY = 'ObjectProperty'
OWL_OBJECTPROPERTY = OWL_NS + NS_SEP + OWL_TERM_OBJECTPROPERTY
OWL_TERM_ONPROPERTY = 'onProperty'
OWL_ONPROPERTY = OWL_NS + NS_SEP + OWL_TERM_ONPROPERTY
OWL_TERM_QUALIFIEDCARDINALITY = 'qualifiedCardinality'
OWL_QUALIFIEDCARDINALITY = OWL_NS + NS_SEP + OWL_TERM_QUALIFIEDCARDINALITY
OWL_TERM_RESTRICTION = 'Restriction'
OWL_RESTRICTION = OWL_NS + NS_SEP + OWL_TERM_RESTRICTION
OWL_TERM_THING = 'Thing'
OWL_THING = OWL_NS + NS_SEP + OWL_TERM_THING
OWL_TERM_UNIONOF = 'unionOf'
OWL_UNIONOF = OWL_NS + NS_SEP + OWL_TERM_UNIONOF

RDF_NS = 'rdf'
RDF_TERM_PROPERTY = 'Property'
RDF_PROPERTY = RDF_NS + NS_SEP + RDF_TERM_PROPERTY
RDF_TERM_TYPE = 'type'
RDF_TYPE = RDF_NS + NS_SEP + RDF_TERM_TYPE

RDFS_NS = 'rdfs'
RDFS_TERM_COMMENT = 'comment'
RDFS_COMMENT = RDFS_NS + NS_SEP + RDFS_TERM_COMMENT
RDFS_TERM_DOMAIN = 'domain'
RDFS_DOMAIN = RDFS_NS + NS_SEP + RDFS_TERM_DOMAIN
RDFS_TERM_ISDEFINEDBY = 'isDefinedBy'
RDFS_ISDEFINEDBY = RDFS_NS + NS_SEP + RDFS_TERM_ISDEFINEDBY
RDFS_TERM_LABEL = 'label'
RDFS_LABEL = RDFS_NS + NS_SEP + RDFS_TERM_LABEL
RDFS_TERM_RANGE = 'range'
RDFS_RANGE = RDFS_NS + NS_SEP + RDFS_TERM_RANGE
RDFS_TERM_SUBCLASSOF = 'subClassOf'
RDFS_SUBCLASSOF = RDFS_NS + NS_SEP + RDFS_TERM_SUBCLASSOF

SKOS_NS = 'skos'
SKOS_TERM_ALTLABEL = 'altLabel'
SKOS_ALTLABEL = SKOS_NS + NS_SEP + SKOS_TERM_ALTLABEL
SKOS_TERM_PREFLABEL = 'prefLabel'
SKOS_PREFLABEL = SKOS_NS + NS_SEP + SKOS_TERM_PREFLABEL

XSD_NS = 'xsd'
XSD_TERM_BOOLEAN = 'boolean'
XSD_BOOLEAN = XSD_NS + NS_SEP + XSD_TERM_BOOLEAN
XSD_BOOL_TRUE = '1'
XSD_BOOL_FALSE = '0'
XSD_BOOL_TRUE_ALT = 'true'
XSD_BOOL_FALSE_ALT = 'false'
XSD_TERM_DATETIME = 'dateTime'
XSD_DATETIME = XSD_NS + NS_SEP + XSD_TERM_DATETIME
XSD_DATETIME_FORMATSTR = '%Y-%m-%dT%H:%M:%S.%f'
XSD_TERM_DOUBLE = 'double'
XSD_DOUBLE = XSD_NS + NS_SEP + XSD_TERM_DOUBLE
XSD_TERM_FLOAT = 'float'
XSD_FLOAT = XSD_NS + NS_SEP + XSD_TERM_FLOAT
XSD_TERM_INTEGER = 'integer'
XSD_INTEGER = XSD_NS + NS_SEP + XSD_TERM_INTEGER
XSD_TERM_STRING = 'string'
XSD_STRING = XSD_NS + NS_SEP + XSD_TERM_STRING

## Common namespaces
_default_prefixes = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcam": "http://purl.org/dc/dcam/",
    "dcterms": "http://purl.org/dc/terms/",
    "fn": "http://www.w3.org/2005/xpath-functions#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    OWL_NS: "http://www.w3.org/2002/07/owl#",
    RDF_NS: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    RDFS_NS: "http://www.w3.org/2000/01/rdf-schema#",
    SKOS_NS: "http://www.w3.org/2004/02/skos/core#",
    XSD_NS: "http://www.w3.org/2001/XMLSchema#",
}

_default_prefixes_inverse = \
    {value: key for key, value in _default_prefixes.items()}

def expandIRI(iri: str, knss) -> str:
    # Split the IRI string at the first colon.
    partList = iri.split(':', 1)
    # If the first part matches a known namespace, then replace
    # that namespace with its full URL, otherwise just return
    # the original string.
    return knss[partList[0]] + partList[1] if partList[0] in knss else iri

def isNamespacedIRI(iri: str, knss) -> bool:
    # Split the IRI string at the first colon.
    partList = iri.split(':', 1)
    return partList[0] in knss

def nameFromIRI(iri: str, includeNamespace: bool=False,
    knssInv: dict=None) -> str:
    """
    Extracts the 'name' of an entity from an IRI string, by using
    everything following the last hash if present, otherwise
    the last forward slash if present, otherwise the last colon if
    present. If none of these are present, the whole string is returned.
    Prepends the namespace with an underscore if applicable and requested.
    WARNING: IRIs are arbitrary and should never be abused to convey or
    extract information, meaning, or conclusion of *any kind*!
    """
    if iri.find('#') > 0:
        if includeNamespace:
            parts = iri.rsplit('#', 1)
            key = parts[0] + '#'
            if key in knssInv:
                parts[0] = knssInv[key]
                return '_'.join(parts)
            else:
                return parts[1]
        else:
            return iri.rsplit('#', 1)[1]
    elif iri.find('/') > 0:
        if includeNamespace:
            parts = iri.rsplit('/', 1)
            key = parts[0] + '/'
            if key in knssInv:
                parts[0] = knssInv[key]
                return '_'.join(parts)
            else:
                return parts[1]
        else:
            return iri.rsplit('/', 1)[1]
    elif iri.find(':') > 0:
        # This covers namespaced IRIs.
        if includeNamespace:
            return '_'.join(iri.rsplit(':', 1))
        else:
            return iri.rsplit(':', 1)[1]
    else:
        return iri
