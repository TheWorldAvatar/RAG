from typing import Callable
import os
import uuid
from time import sleep
import pandas as pd
import json
import xml.etree.ElementTree as ET
from rdflib import Namespace, URIRef, Graph, Literal
from rdflib.namespace import RDF, XSD, OWL
import logging

from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_core.runnables.base import RunnableSequence

from common import *
import storeclient
from SPARQLBuilder import SPARQLSelectBuilder, makeVarRef, makeIRIRef, makeLiteralStr
from CommonNamespaces import RDF_TYPE, XSD_STRING

DEBUG = False

# Parser states
PS_NONE       = 0
PS_GROUP      = 1
PS_PART_GROUP = 2
PS_PERSON     = 3
PS_BRACKET    = 4

# Special characters
LINE_FEED       = chr(10)
CARRIAGE_RETURN = chr(13)
NO_BREAK_SPACE  = chr(160)
SOFT_HYPHEN     = chr(173)
EN_DASH         = chr(8211)

def generate_instance_iri(base_iri: str, class_name: str) -> str:
    return f"{base_iri}{class_name}_{str(uuid.uuid4()).replace('-', '_')}"

class ABox:

    def __init__(self, base_iri: str, existing_g: Graph=None,
        mdb_lookup: dict[str, str]=None
    ) -> None:
        if existing_g is None:
            self.graph = Graph()
        else:
            self.graph = existing_g
        self.store_client = storeclient.RdflibStoreClient(g=self.graph)
        self.base_iri = base_iri
        # Cache 'static' relationship references
        self.has_value_ref = make_rel_ref(self.base_iri, "value")
        # Cache dictionary lookup for IRI of parliamentary groups by
        # fragment. To be populated during instantiation.
        self.group_iri_lookup = {}
        # Optional MdB look-up
        self.mdb_lookup = mdb_lookup if mdb_lookup is not None else {}

    def add_prefix(self, prefix: str, namespace: Namespace) -> None:
        self.graph.bind(prefix, namespace)

    def write_to_turtle(self, filename: str) -> None:
        self.graph.serialize(filename, format="turtle")

    def load_tbox(self, basename: str) -> None:
        self.tbox_df = pd.read_csv(f"{basename}.csv")
        with open(f"{basename}.json", "r", encoding=ES_UTF_8) as infile:
            json_str = infile.read()
            self.tbox_dict = json.loads(json_str)
        with open(f"{basename}-customisations.json", "r", encoding=ES_UTF_8) as infile:
            json_str = infile.read()
            self.tbox_customisations = json.loads(json_str)

    def is_object_property(self, name: str) -> bool:
        f = self.tbox_df.loc[(self.tbox_df["Type"] == "Object Property") &
            (self.tbox_df["Source"] == name)]
        return len(f) >= 1

    def get_group_key(self, group: str) -> str:
        """
        Returns a string to be used as a key (e.g. in a dictionary look-up)
        to identify parliamentary groups. The string of the parliamentary
        group itself cannot be used, because it can appear in different
        grammatical variations in some cases!
        """
        group_upper = group.replace(NO_BREAK_SPACE, " ").upper()
        if "LINKE" in group_upper:
            key = "LINKE"
        elif "GRÜNE" in group_upper:
            key = "GRÜNE"
        elif "FPD" in group_upper:
            # This is a fudge to deal with a typo.
            key = "FDP"
        elif "SDP" in group_upper:
            # This is a fudge to deal with a typo.
            key = "SPD"
        elif "CDU" in group_upper or "CSU" in group_upper:
            # This is a fudge to deal with omission of the other party.
            key = "CDU/CSU"
        else:
            # NB Don't destroy "fraktionslos"!
            key = (group_upper.replace("[", "").replace("]", "").
                replace("/ ", "/").replace("FRAKTION ", " ").strip(" -()"))
        return key

    def find_inst_with_prop(self, sc: storeclient.StoreClient,
        class_iri: str, rel_iri: str, obj_str: str) -> tuple[str, URIRef]:
        sb = SPARQLSelectBuilder()
        c_var_name = "c"
        sb.addVar(makeVarRef(c_var_name))
        sb.addWhere(makeVarRef(c_var_name),
            makeIRIRef(RDF_TYPE), makeIRIRef(class_iri))
        sb.addWhere(makeVarRef(c_var_name),
            makeIRIRef(rel_iri), obj_str)
        reply = sc.query(sb.build())
        result_bindings = reply["results"]["bindings"]
        if len(result_bindings) > 0:
            # We have found an existing entity that matches.
            inst_iri = result_bindings[0][c_var_name]["value"]
            inst_ref = URIRef(inst_iri)
        else:
            inst_iri = None
            inst_ref = None
        return inst_iri, inst_ref

    def add_new_inst(self, class_name: str, class_iri: str) -> tuple[str, URIRef]:
        inst_iri = generate_instance_iri(self.base_iri, class_name)
        inst_ref = URIRef(inst_iri)
        self.graph.add((inst_ref, RDF.type, URIRef(class_iri)))
        self.graph.add((inst_ref, RDF.type, OWL.NamedIndividual))
        return inst_iri, inst_ref

    def link_comment_to_mdb(self, comment_ref: URIRef, person_str: str) -> None:
        parts = person_str.split("[")
        # If there is a non-breakable space, we assume it is used to
        # prefix a title. We remove this, and any remaining leading
        # or trailing whitespace.
        if NO_BREAK_SPACE in parts[0]:
            name = parts[0].split(NO_BREAK_SPACE)[1].strip()
        else:
            name = parts[0].strip()
        # Strip out any titles for the purpose of name-matching.
        name = (name.replace("Dr.", "").replace("Prof.", "").
            replace("Ing.", "").replace("Graf ", "").
            replace("Freiherr ", "").strip(" -"))
        # Remove excessive internal white-space.
        name = " ".join(name.split())
        # Try to find the name in the MdB name/ID look-up
        name_lower = name.lower()
        if name_lower in self.mdb_lookup:
            self.graph.add((comment_ref, make_rel_ref(self.base_iri, "id"),
                Literal(self.mdb_lookup[name_lower], datatype=XSD.string)))
            # Add parliamentary group, if there is one
            if len(parts) > 1:
                key = self.get_group_key(parts[-1])
                if key in self.group_iri_lookup:
                    self.graph.add((comment_ref, make_rel_ref(self.base_iri, "mdb_von"),
                        URIRef(self.group_iri_lookup[key])))
                else:
                    log_msg(f"Key '{key}' extracted from MdB '{person_str}' "
                        f"is not a known parliamentary group!",
                        level=logging.WARN)
        else:
            log_msg(f"Unable to find '{name}' in MdB master data!",
                level=logging.WARN)
        # Debug only!
        if DEBUG:
            self.graph.add((comment_ref,
                make_rel_ref(self.base_iri, "debug_person"),
                Literal(person_str, datatype=XSD.string)))

    def process_originator(self, comment_ref: URIRef, originator: str) -> None:
        parts = originator.split(" ")
        ignore = ["bei", "beim", "von", "der", "des", "dem", "sowie",
            "und", "Parl.", "auf", "vom"]
        state = PS_NONE
        cumulative_name = ""
        for part in parts:
            if part.startswith("Weiter"):
                continue
            if part.startswith("Anhaltend"):
                continue
            if part.startswith("Langanhaltend"):
                continue
            if part.startswith("Lebhaft"):
                continue
            if state == PS_NONE:
                # Check for activities first.
                if any(a in part for a in COMMENT_ACTIVITIES_LONG):
                    state = PS_GROUP
                    cumulative_name = ""
                    continue
                # If no activity, we most likely have a named person.
                state = PS_PERSON
                cumulative_name = ""
            commit = False
            if part.endswith(","):
                part_no_comma = part.replace(",", "")
                commit = True
            else:
                part_no_comma = part
            if part_no_comma == "und" or part_no_comma == "sowie":
                if cumulative_name != "":
                    commit = True
            if state < PS_PERSON:
                if any(part == ign for ign in ignore):
                    pass
                # Ignore any occurrence of multiple activities.
                elif any(a in part for a in COMMENT_ACTIVITIES_LONG):
                    pass
                elif part == "Abgeordneten" or part == "Abg.":
                    pass
                else:
                    # Identify parliamentary group (whether part or whole)
                    if cumulative_name == "":
                        cumulative_name = part_no_comma
                    else:
                        cumulative_name = " ".join([cumulative_name, part_no_comma])
                if commit:
                    key = self.get_group_key(cumulative_name)
                    if key in self.group_iri_lookup:
                        self.graph.add((comment_ref, make_rel_ref(self.base_iri,
                            CR_GROUP_WHOLE if state == PS_GROUP else "abgeordnete_von"),
                            #Literal(cumulative_name, datatype=XSD.string)))
                            URIRef(self.group_iri_lookup[key])))
                    else:
                        log_msg(f"Key '{key}' extracted from comment originator "
                            f"'{originator}' could not be found in parliamentary "
                            f"group look-up!", level=logging.WARN)
                    cumulative_name = ""
                if part == "Abgeordneten":
                    if parts.index(part)+1 >= len(parts):
                        # This should not happen.
                        log_msg(f"Nothing following 'Abgeordneten' in "
                            f"originator '{originator}'!", level=logging.WARN)
                        return
                    if parts[parts.index(part)+1].startswith("d"):
                        # What follows will be parts of parliamentary groups.
                        state = PS_PART_GROUP
                    else:
                        state = PS_PERSON
                    cumulative_name = ""
                elif (part.startswith("Abg") or part.startswith("Bundesminister")
                    or part.startswith("Staats") or part.startswith("Minister")
                    or part.startswith("Vizepr")): #sekretär #minister
                    # What follows will be a named person.
                    state = PS_PERSON
                    cumulative_name = ""
            else:
                # We're expecting a named person here.
                if part_no_comma != "und" and part_no_comma != "sowie":
                    if cumulative_name == "":
                        cumulative_name = part_no_comma
                    else:
                        cumulative_name = " ".join([cumulative_name, part_no_comma])
                if commit:
                    self.link_comment_to_mdb(comment_ref, cumulative_name)
                    cumulative_name = ""
        if state == PS_PERSON and cumulative_name != "":
            self.link_comment_to_mdb(comment_ref, cumulative_name)
        if state < PS_PERSON and cumulative_name != "":
            if "ganze" in cumulative_name and "Haus" in cumulative_name:
                self.graph.add((comment_ref, make_rel_ref(self.base_iri,
                    CR_WHOLE_HOUSE), Literal(1, datatype=XSD.int)))
            elif "aller" in cumulative_name and "Fraktionen" in cumulative_name:
                self.graph.add((comment_ref, make_rel_ref(self.base_iri,
                    CR_ALL_GROUPS), Literal(1, datatype=XSD.int)))
            else:
                key = self.get_group_key(cumulative_name)
                if key != "Tribüne" and key != "Besuchertribüne" and key != "Regierungsbank":
                    if key in self.group_iri_lookup:
                        self.graph.add((comment_ref, make_rel_ref(self.base_iri,
                            CR_GROUP_WHOLE if state == PS_GROUP else "abgeordnete_von"),
                            #Literal(cumulative_name, datatype=XSD.string)))
                            URIRef(self.group_iri_lookup[key])))
                    else:
                        log_msg(f"Key '{key}' extracted from comment originator "
                            f"'{originator}' could not be found in parliamentary "
                            f"group look-up!", level=logging.WARN)
        # Debug only!
        if DEBUG:
            self.graph.add((comment_ref,
                make_rel_ref(self.base_iri, "debug_originator"),
                Literal(originator, datatype=XSD.string)))

    def process_comment(self, comment_ref: URIRef, comment: str) -> None:
        """
        Parses comment string and instantiates comment as properties
        of the given IRI reference. The comment string is assumed to
        be a single comment, and not multiple ones separated by a hyphen.
        """
        recognised = False
        # We assume that, if there is a colon in the string, then
        # everything after the first one is identified content.
        if ":" in comment:
            recognised = True
            parts = comment.split(":", 1)
            first_part = parts[0]
            # Everything after the first colon is the identified content.
            self.graph.add((comment_ref, self.has_value_ref,
                Literal(parts[1].strip(), datatype=XSD.string)))
        else:
            first_part = comment
        if ((CA_CALL in first_part) or 
            (CA_COUNTERCALL in first_part) or recognised):
            recognised = True
            self.graph.add((comment_ref,
                make_rel_ref(self.base_iri, CA_INTERJECTION),
                Literal(2 if CA_COUNTERCALL in first_part else 1,
                datatype=XSD.int)))
        if any(a in first_part for a in COMMENT_ACTIVITIES):
            recognised = True
            # Add mark-up for the type of activity/-ies.
            for activity in COMMENT_ACTIVITIES:
                if activity in first_part:
                    self.graph.add((comment_ref,
                        make_rel_ref(self.base_iri, activity),
                        Literal(1, datatype=XSD.int)))
        if recognised:
            self.process_originator(comment_ref, first_part)
        else:
            # In all other cases, we fall back to simply instantiating
            # the whole string as a value literal, with no semantic
            # mark-up.
            log_msg(f"Unable to semantically represent comment '{comment}'! "
                f"Reverting to plain text representation.", level=logging.WARN)
            self.graph.add((comment_ref, self.has_value_ref,
                Literal(comment, datatype=XSD.string)))

    def transform_text_by_type_iri(self, txt: str, type_iri: str) -> str:
        if type_iri == LDTS_DATE:
            return "-".join(reversed(txt.split(".")))
        elif type_iri == LDTS_TIME:
            parts = txt.split(":")
            if len(parts) == 2:
                parts.append("00")
            return ":".join(p.zfill(2) for p in parts)
        else:
            return txt

    def instantiate_xml_node(self, node: ET.Element, index: int=0,
        parent: ET.Element=None, parent_iri_ref: URIRef=None) -> None:
        next_index = index
        if node.tag in self.tbox_customisations[TC_DELETIONS]:
            log_msg(f"Skipping node '{node.tag}' due to custom deletion.")
            return next_index
        if node.tag in self.tbox_customisations[TC_DELETIONS_WP]:
            if parent.tag in self.tbox_customisations[TC_DELETIONS_WP][node.tag]:
                log_msg(f"Skipping node '{node.tag}' with parent "
                    f"'{parent.tag}' due to custom deletion.")
                return next_index
        if node.tag in self.tbox_customisations[TC_REPLACEMENTS]:
            if node.tag == "fraktion":
                log_msg(f"Custom replacement for node '{node.tag}'.")
                rel_iri = make_rel_iri(self.base_iri, "name_kurz")
                class_name = node.tag.capitalize()
                class_iri = self.base_iri+class_name
                # We need to check uniqueness prior to instantiation!
                fraktion_raw = node.text.replace(NO_BREAK_SPACE, " ")
                key = self.get_group_key(fraktion_raw)
                if key in self.group_iri_lookup:
                    inst_iri = self.group_iri_lookup[key]
                    inst_ref = URIRef(inst_iri)
                else:
                    inst_iri, inst_ref = self.find_inst_with_prop(self.store_client,
                        class_iri, rel_iri, makeLiteralStr(fraktion_raw, XSD_STRING))
                if inst_iri is not None:
                    log_msg(f"Re-using instance '{inst_iri}'.")
                else:
                    # No suitable instance exists. Create a new one.
                    inst_iri, inst_ref = self.add_new_inst(class_name, class_iri)
                    self.graph.add((inst_ref,
                        URIRef(rel_iri) , Literal(fraktion_raw, datatype=XSD.string)))
                    log_msg(f"Created instance '{inst_iri}' for "
                        f"parliamentary group '{fraktion_raw}'.")
                if parent is not None:
                    # Relate the parent to the instance.
                    self.graph.add((parent_iri_ref,
                        make_rel_ref(self.base_iri, class_name), inst_ref))
                # Cache the instance IRI in a look-up dictionary for later.
                if key not in self.group_iri_lookup:
                    self.group_iri_lookup[key] = inst_iri
            else:
                raise Exception(f"Custom replacement for '{node.tag}' is not implemented!")
        else:
            do_shortcut = False
            if node.tag in self.tbox_customisations[TC_SHORTCUTS]:
                do_shortcut = True
            elif node.tag in self.tbox_customisations[TC_SHORTCUTS_WP]:
                if self.tbox_customisations[TC_SHORTCUTS_WP][node.tag] == parent.tag:
                    do_shortcut = True
            is_new_inst = True
            if do_shortcut:
                log_msg(f"Short-cutting node '{node.tag}'.")
                # If short-cut, don't add any statements to the graph. Just recurse,
                # with the current parent node as parent, rather than the current
                # node itself.
                effective_parent = parent
                effective_parent_iri_ref = parent_iri_ref
            else:
                attribs = node.items()
                class_name = node.tag.capitalize()
                class_iri = self.base_iri+class_name
                if (len(node) > 0) or (len(attribs) > 0) or (
                    node.tag in self.tbox_customisations[TC_INDEX_FIELDS]):
                    # This needs to be an instance, not a literal.
                    if class_name == "Redner":
                        # We need to check uniqueness prior to instantiation!
                        rid = ""
                        for a in attribs:
                            if a[0] == "id":
                                rid = a[1]
                                break
                        rel_iri = make_rel_iri(self.base_iri, "id")
                        inst_iri, inst_ref = self.find_inst_with_prop(self.store_client,
                            class_iri, rel_iri, makeLiteralStr(rid, XSD_STRING))
                        if inst_iri is not None:
                            log_msg(f"Re-using instance '{inst_iri}'.")
                            is_new_inst = False
                    if is_new_inst:
                        # No suitable instance exists. Create a new one.
                        inst_iri, inst_ref = self.add_new_inst(class_name, class_iri)
                        log_msg(f"Created instance '{inst_iri}'.")
                        # Represent node attributes as literals using datatype properties.
                        for attrib in attribs:
                            type_iri = get_field_data_type_iri(attrib[0])
                            literal_value = self.transform_text_by_type_iri(
                                attrib[1], type_iri)
                            self.graph.add((inst_ref,
                                make_rel_ref(self.base_iri, attrib[0]),
                                Literal(literal_value, datatype=URIRef(type_iri))))
                        # Add index field if desired.
                        if node.tag in self.tbox_customisations[TC_INDEX_FIELDS]:
                            index_rel = make_rel_ref(self.base_iri, "index")
                            self.graph.add((inst_ref,
                                index_rel, Literal(next_index, datatype=XSD.int)))
                        next_index += 1
                        # Add node text as value datatype property.
                        if node.text is not None:
                            value = node.text.strip()
                            if value != "":
                                if class_name == "Kommentar":
                                    # Remove LF/CR, and outside parentheses and whitespace.
                                    value = (value.replace(LINE_FEED, "").
                                        replace(CARRIAGE_RETURN, "").
                                        strip(" ()"))
                                    # Split multi-part comments into parts.
                                    split_str = NO_BREAK_SPACE+EN_DASH
                                    if split_str not in value:
                                        split_str = " "+EN_DASH
                                    comments = value.split(split_str)
                                    add_inst = False
                                    cmt_ref = inst_ref
                                    # Iterate through individual comments.
                                    for c in comments:
                                        # If there is more than one, need new instance
                                        # for every additional one, with correct indexing
                                        # and parent relationship.
                                        if add_inst:
                                            _, cmt_ref = self.add_new_inst(class_name, class_iri)
                                            self.graph.add((cmt_ref, index_rel,
                                                Literal(next_index, datatype=XSD.int)))
                                            next_index += 1
                                            if parent is not None:
                                                self.graph.add((parent_iri_ref,
                                                    make_rel_ref(self.base_iri, class_name),
                                                    cmt_ref))
                                        self.process_comment(cmt_ref,
                                            c.strip(" ()"))
                                        add_inst = True
                                else:
                                    self.graph.add((inst_ref, self.has_value_ref,
                                        Literal(value, datatype=XSD.string)))
                else:
                    # This node has neither children nor attributes. Its text
                    # content will appear as a literal in a datatype property.
                    if node.text is not None and node.text != "":
                        type_iri = get_field_data_type_iri(node.tag)
                        literal_value = self.transform_text_by_type_iri(
                            node.text, type_iri)
                        inst_ref = Literal(literal_value,
                            datatype=URIRef(type_iri))
                    else:
                        inst_ref = None
                if parent is not None:
                    # Relate the parent to the new/existing instance/literal.
                    if inst_ref is not None:
                        # If we have a literal, check for name clash with
                        # object property.
                        subst_class_name = class_name
                        if isinstance(inst_ref, Literal):
                            rel_name = make_rel_name(class_name)
                            if self.is_object_property(rel_name):
                                subst_class_name += PROP_NAME_CLASH_ADDENDUM
                                log_msg(f"Replacing '{rel_name}' with "
                                    f"'{make_rel_name(subst_class_name)}' "
                                    f"due to name clash.")
                        self.graph.add((parent_iri_ref,
                            make_rel_ref(self.base_iri, subst_class_name),
                            inst_ref))
                effective_parent = node
                effective_parent_iri_ref = inst_ref
            # Instantiate children, if any, recursively.
            if is_new_inst:
                # Prioritise speaker list, if it is there
                # (and don't index it!).
                speaker_list_tag = "rednerliste"
                for child in node:
                    if child.tag == speaker_list_tag:
                        self.instantiate_xml_node(child,
                            parent=effective_parent,
                            parent_iri_ref=effective_parent_iri_ref)
                        break
                child_index = 1
                for child in node:
                    if child.tag != speaker_list_tag:
                        if node.tag in self.tbox_customisations[TC_SHORTCUTS_WC]:
                            if child.tag == self.tbox_customisations[TC_SHORTCUTS_WC][node.tag]:
                                log_msg(f"Shortcutting '{node.tag}' for "
                                    f"child '{child.tag}' only.")
                                # If short-cut, we still need to recurse
                                # into and add statements for this child
                                # to the graph, but with the current
                                # parent node as parent, rather than
                                # the current node itself.
                                effective_parent = parent
                                effective_parent_iri_ref = parent_iri_ref
                        child_index = self.instantiate_xml_node(child,
                            index=child_index, parent=effective_parent,
                            parent_iri_ref=effective_parent_iri_ref)
        return next_index

    def instantiate_xml_file(self, filename: str) -> None:
        # Parse XML input file.
        tree = ET.parse(filename)
        root = tree.getroot()
        self.instantiate_xml_node(root)

def assemble_speech_texts(g: Graph) -> None:
    """
    Assemble all speech texts from individual paragraphs.
    """
    log_msg(" - Assembling speech texts...")
    # NB The relevant prefix(es) should already be bound to the graph.
    ustr = (
        'INSERT {\n'
        '  ?r pd:hatText ?Text\n'
        '} WHERE {\n'
        '  SELECT ?r (GROUP_CONCAT(?Value; SEPARATOR=" ") AS ?Text) WHERE {\n'
        '    SELECT ?r ?Value WHERE {\n'
        '      ?r a pd:Rede .\n'
        '      ?r pd:hatP ?p .\n'
        '      ?p pd:hatIndex ?Index .\n'
        '      ?p pd:hatValue ?Value\n'
        '    } ORDER BY ?Index\n'
        '  } GROUP BY ?r\n'
        '}'
    )
    g.update(ustr)

def add_speech_dates(g: Graph) -> None:
    """
    Attaches to every speech the date it was made, i.e. the date of the
    parliamentary session.
    """
    log_msg(" - Adding speech dates...")
    # NB The relevant prefix(es) should already be bound to the graph.
    ustr = (
        'INSERT {\n'
        '  ?r pd:hatDatum ?d\n'
        '} WHERE {\n'
        '  ?r a pd:Rede .\n'
        '  ?p a pd:Dbtplenarprotokoll .\n'
        '  ?p pd:hatSitzungsverlauf/pd:hatTagesordnungspunkt/pd:hatRede ?r.\n'
        '  ?p pd:hatSitzung-datum ?d\n'
        '}'
    )
    g.update(ustr)

def add_speaker_gender(g: Graph) -> None:
    """
    Attaches to every speaker their gender.
    """
    log_msg(" - Adding speaker genders...")
    # NB The relevant prefix(es) should already be bound to the graph.
    ustr = (
        'INSERT {\n'
        '  ?r pd:hatGeschlecht ?g\n'
        '} WHERE {\n'
        '  ?r a pd:Redner .\n'
        '  ?r pd:hatId ?id .\n'
        '  ?m a msd:Mdb .\n'
        '  ?m msd:hatId ?id .\n'
        '  ?m msd:hatGeschlecht ?g\n'
        '}'
    )
    g.update(ustr)

def add_speaker_party(abox: ABox) -> None:
    """
    Attaches to every speaker without parliamentary group affiliation
    one deduced from their MdB party affiliation.
    """
    log_msg(" - Adding missing speaker parties...")
    # Make a look-up dictionary of parliamentary group strings by key.
    pgs = get_parliamentary_groups(abox.store_client)
    pg_lookup = {}
    for pg in pgs:
        pg_lookup[abox.get_group_key(pg)] = pg
    # Query the parties from MdB master data for those speakers
    # without parliamentary group affiliation.
    # NB The relevant prefix(es) should already be bound to the graph.
    id_var_name = "id"
    party_var_name = "partei"
    qstr = (
        f'SELECT ?{id_var_name} ?{party_var_name}\n'
        'WHERE {\n'
        '  ?r a pd:Redner .\n'
        f'  ?r pd:hatId ?{id_var_name} .\n'
        '  FILTER NOT EXISTS { ?r pd:hatFraktion ?fraktion . }\n'
        '  ?mdb a msd:Mdb .\n'
        f'  ?mdb msd:hatId ?{id_var_name} .\n'
        f'  ?mdb msd:hatPartei_kurz ?{party_var_name}\n'
        '}'
    )
    ids_parties = abox.store_client.query(qstr)["results"]["bindings"]
    # Add parliamentary groups to speakers where applicable.
    for id_party in ids_parties:
        speaker_id = id_party[id_var_name]["value"]
        pg_key = abox.get_group_key(id_party[party_var_name]["value"])
        pg_name = pg_lookup[pg_key] if pg_key in pg_lookup else ""
        # NB the XSD string is necessary. It will do nothing without!
        ustr = (
            'INSERT {\n'
            '  ?r pd:hatFraktion ?f\n'
            '} WHERE {\n'
            '  ?r a pd:Redner .\n'
            f'  ?r pd:hatId "{speaker_id}"^^xsd:string .\n'
            '  ?f a pd:Fraktion .\n'
            f'  ?f pd:hatName_kurz "{pg_name}"^^xsd:string\n'
            '}'
        )
        abox.store_client.update(ustr)
        log_msg(f"   Added '{pg_name}' to speaker '{speaker_id}'.")

def name_to_iri(name: str, lookup: dict[str, str]) -> str:
    name_parts = name.split(" ")
    lower_last = name_parts[-1].lower()
    candidate_matches: list[str] = []
    for lname in lookup:
        if lower_last in lname.lower():
            candidate_matches.append(lname)
    if len(candidate_matches) <= 0:
        log_msg(f"     Could not find '{name}' among speakers!")
        return ""
    else:
        log_msg(f"     Candidate matches: {str(candidate_matches)}")
        for candidate in candidate_matches:
            # If there happens to be an exact match, we're done.
            if name == candidate:
                log_msg(f"     Matched '{name}' to '{candidate}'.")
                return lookup[candidate]
        c_matches_rev: list[str] = []
        for candidate in candidate_matches:
            c_lower_last = candidate.split(" ")[-1].lower()
            if c_lower_last in name.lower():
                c_matches_rev.append(candidate)
        log_msg(f"     Shorter list: {str(c_matches_rev)}")
        if len(c_matches_rev) <= 0:
            log_msg(f"     Could not find '{name}' among speakers!")
            return ""
        else:
            # If we really cannot find any better match, default to
            # the first one on the shorter list.
            winner = c_matches_rev[0]
            if len(c_matches_rev) > 1:
                num_match_frag: dict[str, int] = {}
                for c in c_matches_rev:
                    num_matches = 0
                    for p in name_parts:
                        if p in c:
                            num_matches += 1
                    num_match_frag[c] = num_matches
                for c in c_matches_rev:
                    if num_match_frag[c] > num_match_frag[winner]:
                        winner = c
            log_msg(f"     Matched '{name}' to '{winner}'.")
            return lookup[winner]

def make_speaker_name_iri_lookup(abox: ABox) -> dict[str, str]:
    givenname_var_name = "Vorname"
    surname_var_name = "Nachname"
    speaker_var_name = "Redner"
    # NB The relevant prefix(es) should already be bound to the graph.
    qstr = (
        f'SELECT ?{givenname_var_name} ?{surname_var_name} ?{speaker_var_name}\n'
        'WHERE {\n'
        f'  ?{speaker_var_name} a pd:Redner .\n'
        f'  ?{speaker_var_name} pd:hatVorname ?{givenname_var_name} .\n'
        f'  ?{speaker_var_name} pd:hatNachname ?{surname_var_name} .\n'
        '}'
    )
    speakers = abox.store_client.query(qstr)["results"]["bindings"]
    speaker_name_iri_lookup: dict[str, str] = {}
    for speaker in speakers:
        speaker_name_iri_lookup[" ".join([
            speaker[givenname_var_name]["value"],
            speaker[surname_var_name]["value"]
        ])] = speaker[speaker_var_name]["value"]
    return speaker_name_iri_lookup

def process_cto_iris_texts(
    abox: ABox,
    iris_texts,
    text_var_name: str, iri_var_name: str,
    cto_chain: RunnableSequence,
    speaker_name_iri_lookup: dict[str, str]
) -> None:
    for iri_text in iris_texts:
        raw_speaker_list_str: str = cto_chain.invoke(
            {"text": iri_text[text_var_name]["value"]}
        ).content
        log_msg(iri_text[text_var_name]["value"])
        log_msg(f"     Raw list: '{raw_speaker_list_str}'")
        # Wait before the next request - we don't want to get blocked!
        sleep(WAIT_TIME)
        speaker_list_str = raw_speaker_list_str.strip(" '`,.")
        if speaker_list_str != "" and "niemand" not in speaker_list_str.lower():
            speaker_name_list = speaker_list_str.split(",")
            for speaker_name in speaker_name_list:
                speaker_iri = name_to_iri(speaker_name.strip(" "),
                    speaker_name_iri_lookup)
                if speaker_iri != "":
                    ustr = (
                        'INSERT DATA {\n'
                        f'  <{iri_text[iri_var_name]["value"]}> '
                        'pd:hatOrdnungsruf_erteilt_an '
                        f'<{speaker_iri}>\n'
                        '}'
                    )
                    abox.store_client.update(ustr)

def add_calls_to_order(abox: ABox) -> None:
    """
    Annotates every speech and agenda item during which a call to order
    was issued, as judged by an LLM. Warning: Multiple issues to the
    same individual during a single speech/agenda item are not supported.
    """
    log_msg(" - Processing calls to order...")
    # Initialise LLM and chain
    from ragconfig import RAGConfig, CVN_MODEL, CVN_TEMPERATURE
    config = RAGConfig("config-hybrid.yaml")
    config.set_openai_api_key()
    llm = ChatOpenAI(
        model=config.get(CVN_MODEL),
        temperature=config.get(CVN_TEMPERATURE)
    )
    extract_cto_prompt = PromptTemplate(
        template=read_text_from_file(
            os.path.join("prompt_templates", "extract_cto.txt")
        ),
        input_variables=["text"]
    )
    cto_chain = extract_cto_prompt | llm
    speaker_name_iri_lookup = make_speaker_name_iri_lookup(abox)
    # Query candidate speech texts from KG
    iri_var_name = "iri"
    text_var_name = "Text"
    # NB The relevant prefix(es) should already be bound to the graph.
    qstr = (
        f'SELECT DISTINCT ?{iri_var_name} ?{text_var_name}\n'
        'WHERE {\n'
        f'  ?{iri_var_name} a pd:Rede .\n'
        f'  ?{iri_var_name} pd:hatText ?{text_var_name} .\n'
        f'  ?{iri_var_name} pd:hatP/pd:hatValue ?value\n'
        '  FILTER(CONTAINS(?value, "Ordnung") && CONTAINS(?value, "ruf"))\n'
        '}'
    )
    iris_texts = abox.store_client.query(qstr)["results"]["bindings"]
    log_msg(f"   Found {len(iris_texts)} candidate speech(es).")
    process_cto_iris_texts(abox, iris_texts, text_var_name,
        iri_var_name, cto_chain, speaker_name_iri_lookup)
    # Query candidate agenda item texts from KG
    qstr = (
        f'SELECT ?{iri_var_name} (GROUP_CONCAT(?Value; SEPARATOR=" ") AS ?{text_var_name})\n'
        'WHERE {\n'
        f'  SELECT ?{iri_var_name} ?Value\n'
        '  WHERE { {\n'
        f'    SELECT DISTINCT ?{iri_var_name}\n'
        '    WHERE {\n'
        f'      ?{iri_var_name} a pd:Tagesordnungspunkt .\n'
        f'      ?{iri_var_name} pd:hatP/pd:hatValue ?value_filter .\n'
        '      FILTER(CONTAINS(?value_filter, "Ordnung") && CONTAINS(?value_filter, "ruf"))\n'
        '    } }\n'
        f'    ?{iri_var_name} pd:hatP ?p .\n'
        '    ?p pd:hatIndex ?Index .\n'
        '    ?p pd:hatValue ?Value\n'
        '  } ORDER BY ?Index\n'
        '}\n'
        f'GROUP BY ?{iri_var_name}'
    )
    iris_texts = abox.store_client.query(qstr)["results"]["bindings"]
    log_msg(f"   Found {len(iris_texts)} candidate agenda item text(s).")
    process_cto_iris_texts(abox, iris_texts, text_var_name,
        iri_var_name, cto_chain, speaker_name_iri_lookup)

def make_reading_top_set(abox: ABox,
    first: bool, second: bool, third: bool) -> set[str]:
    top_set: set[str] = set()
    first_str = "" if first else "!"
    second_str = "" if second else "!"
    third_str = "" if third else "!"
    qstr = (
        'SELECT DISTINCT ?t WHERE {\n'
        '  ?t a pd:Tagesordnungspunkt .\n'
        '  ?t pd:hatP ?p .\n'
        '  ?p pd:hatValue ?value .\n'
        '  ?p pd:hatIndex ?i\n'
        '  FILTER(CONTAINS(?value, "Beratung") && '
        f'{first_str}CONTAINS(LCASE(?value), "erste") && '
        f'{second_str}CONTAINS(LCASE(?value), "zweite") && '
        f'{third_str}CONTAINS(LCASE(?value), "dritte") && ?i <= 3)\n'
        '}'
    )
    tops = abox.store_client.query(qstr)["results"]["bindings"]
    for top in tops:
        if "t" in top:
            top_set.add(top["t"]["value"])
    return top_set

def add_readings(abox: ABox) -> None:
    """
    Annotates every agenda item with its reading number(s), if
    applicable.
    """
    log_msg(" - Processing readings of agenda items...")
    # Make sets of agenda items for each reading number
    tops_1 = make_reading_top_set(abox, True, False, False)
    log_msg(f"   #agenda items: 1.: {len(tops_1)}")
    tops_2 = make_reading_top_set(abox, False, True, False)
    log_msg(f"   #agenda items: 2.: {len(tops_2)}")
    tops_3 = make_reading_top_set(abox, False, False, True)
    log_msg(f"   #agenda items: 3.: {len(tops_3)}")
    tops_1_23 = make_reading_top_set(abox, True, True, True)
    log_msg(f"   #agenda items: 1. && 2. && 3.: {len(tops_1_23)}")
    tops_not1_23 = make_reading_top_set(abox, False, True, True)
    log_msg(f"   #agenda items: !1. && 2. && 3.: {len(tops_not1_23)}")
    tops_23 = tops_1_23.union(tops_not1_23)
    log_msg(f"   #agenda items: 2. && 3.: {len(tops_23)}")
    log_msg(f"   Overlap 1, 2: {len(tops_1.intersection(tops_2))}")
    log_msg(f"   Overlap 1, 3: {len(tops_1.intersection(tops_3))}")
    log_msg(f"   Overlap 1, 23: {len(tops_1.intersection(tops_23))}")
    log_msg(f"   Overlap 2, 23: {len(tops_2.intersection(tops_23))}")
    log_msg(f"   Overlap 3, 23: {len(tops_3.intersection(tops_23))}")
    tops_1.difference_update(tops_23)
    tops_2.difference_update(tops_23)
    tops_3.difference_update(tops_23)
    # Add the new statements to the KG.
    statements: list[str] = []
    for iri in tops_1:
        statements.append(f'<{iri}> pd:hatLesung "1."^^xsd:string .')
    for iri in tops_2:
        statements.append(f'<{iri}> pd:hatLesung "2."^^xsd:string .')
    for iri in tops_3:
        statements.append(f'<{iri}> pd:hatLesung "3."^^xsd:string .')
    for iri in tops_23:
        statements.append(f'<{iri}> pd:hatLesung "2./3."^^xsd:string .')
    ustr = (
        'INSERT DATA {\n'
        f'{"\n".join(statements)}'
        '}'
    )
    abox.store_client.update(ustr)
    log_msg(f"   Added {len(statements)} statements:")
    log_msg(f"   1.: {len(tops_1)}")
    log_msg(f"   2.: {len(tops_2)}")
    log_msg(f"   3.: {len(tops_3)}")
    log_msg(f"   2./3.: {len(tops_23)}")

def post_pro_debates(abox: ABox) -> None:
    log_msg("Post-processing...")
    assemble_speech_texts(abox.graph)
    add_speech_dates(abox.graph)
    add_speaker_gender(abox.graph)
    add_speaker_party(abox)
    add_calls_to_order(abox)
    add_readings(abox)

def make_mdb_name_id_lookup(sc: storeclient.StoreClient) -> dict[str, str]:
    """
    Queries MdB master data store and returns a dictionary with full
    name (including prefix) as a key and ID as value.
    """
    name_id_lookup = {}
    sb = SPARQLSelectBuilder()
    mdb_var_name = "Mdb"
    id_var_name = "id"
    name_var_name = "name"
    given_name_var_name = "vorname"
    surname_var_name = "nachname"
    prefix_var_name = "praefix"
    mdb_class_iri = MMD_BASE_IRI+mdb_var_name
    has_id_iri = make_rel_iri(MMD_BASE_IRI, id_var_name)
    has_name_iri = make_rel_iri(MMD_BASE_IRI, name_var_name)
    has_given_name_iri = make_rel_iri(MMD_BASE_IRI, given_name_var_name)
    has_surname_iri = make_rel_iri(MMD_BASE_IRI, surname_var_name)
    has_prefix_iri = make_rel_iri(MMD_BASE_IRI, prefix_var_name)
    sb.addVar(makeVarRef(id_var_name))
    sb.addVar(makeVarRef(given_name_var_name))
    sb.addVar(makeVarRef(prefix_var_name))
    sb.addVar(makeVarRef(surname_var_name))
    sb.addWhere(makeVarRef(mdb_var_name),
        makeIRIRef(RDF_TYPE), makeIRIRef(mdb_class_iri))
    sb.addWhere(makeVarRef(mdb_var_name),
        makeIRIRef(has_id_iri), makeVarRef(id_var_name))
    sb.addWhere(makeVarRef(mdb_var_name),
        makeIRIRef(has_name_iri), makeVarRef(name_var_name))
    sb.addWhere(makeVarRef(name_var_name),
        makeIRIRef(has_given_name_iri), makeVarRef(given_name_var_name))
    sb.addWhere(makeVarRef(name_var_name),
        makeIRIRef(has_surname_iri), makeVarRef(surname_var_name))
    sb.addWhere(makeVarRef(name_var_name),
        makeIRIRef(has_prefix_iri), makeVarRef(prefix_var_name),
        optional=True)
    reply = sc.query(sb.build())
    result_bindings = reply["results"]["bindings"]
    for rs in result_bindings:
        id_str = rs[id_var_name]["value"]
        given_name_str = rs[given_name_var_name]["value"]
        if prefix_var_name in rs:
            prefix_str = rs[prefix_var_name]["value"]
        else:
            prefix_str = ""
        surname_str = rs[surname_var_name]["value"]
        if prefix_str != "":
            strlist = [given_name_str, prefix_str, surname_str]
        else:
            strlist = [given_name_str, surname_str]
        key = " ".join(strlist).lower()
        name_id_lookup[key] = id_str
    return name_id_lookup

def instantiate_xml(infolder: str, outfolder: str,
    basename: str, tbox_basename: str, out_basename: str,
    base_iri: str, prefixes: dict[str, str],
    mdb_lookup: dict[str, str]=None, existing_g: Graph=None,
    post_pro: Callable[[ABox], None]=None
) -> None:
    logging.basicConfig(filename=os.path.join(outfolder,
        f"{out_basename}.log"), encoding=ES_UTF_8, level=logging.INFO)
    the_abox = ABox(base_iri, mdb_lookup=mdb_lookup, existing_g=existing_g)
    for prefix in prefixes:
        the_abox.add_prefix(prefix, prefixes[prefix])
    # NB Even though we load the TBox as an input here, it was previously
    # produced as an output, hence the location.
    the_abox.load_tbox(os.path.join(outfolder, tbox_basename))
    log_msg("Starting instantiation.")

    # Use this bit to instantiate a single file:
    xml_file_name = os.path.join(infolder, f"{basename}.xml")
    log_msg(f"Instantiating '{xml_file_name}'...")
    the_abox.instantiate_xml_file(xml_file_name)

    # Use this bit to instantiate all or a selection of files
    # in the input folder:
    # NB We are iterating through these files in reverse order,
    # because this happens to avoid some issues with certain
    # instances not existing at time of first use.
    #for fn in reversed(os.listdir(infolder)):
    #    if fn.endswith(".xml") and (
    #        fn.startswith("18") or fn.startswith("19") or fn.startswith("20")
    #    ):
    #        xml_file_name = os.path.join(infolder, fn)
    #        log_msg("")
    #        log_msg(f"=========================================")
    #        log_msg(f"Instantiating '{xml_file_name}'...")
    #        the_abox.instantiate_xml_file(xml_file_name)

    # Apply any transformations as SPARQL updates to the instantiation.
    if post_pro is not None:
        post_pro(the_abox)
    log_msg("Serialising...")
    the_abox.write_to_turtle(os.path.join(outfolder, f"{out_basename}.ttl"))
    log_msg("Finished!")

def post_process(outfolder: str, out_basename: str,
    base_iri: str, post_pro: Callable[[ABox], None]
) -> None:
    """
    Post-processes a previously created instantiation to be loaded
    from a TTL file. This can be used if one does not want to re-run
    the instantiation itself.
    """
    logging.basicConfig(filename=os.path.join(outfolder,
        f"{out_basename}-postpro.log"), encoding=ES_UTF_8, level=logging.INFO)
    log_msg("Loading existing instantiation...")
    existing_g = Graph()
    existing_g.parse(os.path.join(outfolder, f"{out_basename}.ttl"))
    the_abox = ABox(base_iri, existing_g=existing_g)
    # Apply any transformations as SPARQL updates to the instantiation.
    post_pro(the_abox)
    log_msg("Serialising...")
    the_abox.write_to_turtle(os.path.join(outfolder, f"{out_basename}-postpro.ttl"))
    log_msg("Finished!")

if __name__ == "__main__":
    download_folder = os.path.join("data", "raw")
    processed_folder = os.path.join("data", "processed")

    ## MdB master data instantiation

    basename = "MDB_STAMMDATEN"
    #base_iri = MMD_BASE_IRI
    #prefixes = {MMD_PREFIX: MMD_NAMESPACE}

    #instantiate_xml(download_folder, processed_folder, basename,
    #    f"{basename}-xml-tbox", basename, base_iri, prefixes)

    ## Debate instantiation

    # Read a previously created instantiation of the MdB
    # master data and create a look-up. This is needed for
    # instantiating debates.
    mdb_sc = storeclient.RdflibStoreClient(filename=
        os.path.join(processed_folder, basename+".ttl"))
    mdb_name_id_lookup = make_mdb_name_id_lookup(mdb_sc)
    #export_dict_to_json(mdb_name_id_lookup,
    #    os.path.join(processed_folder, "MdB-lookup.json"))

    # NB It appears sufficient for most purposes to generate
    # a TBox for a single, 'sufficiently rich' parliamentary
    # session (and use this for all other sessions, i.e. the
    # entire dataset). May need to be revisited.
    basename = "20137"
    base_iri = PD_BASE_IRI
    prefixes = {MMD_PREFIX: MMD_NAMESPACE, PD_PREFIX: PD_NAMESPACE}

    instantiate_xml(download_folder, processed_folder, basename,
        f"{basename}-xml-tbox", basename,
        #f"20137-xml-tbox", "complete-rev",
        base_iri, prefixes, mdb_lookup=mdb_name_id_lookup,
        #existing_g=mdb_sc._g,
        post_pro=post_pro_debates
    )

    # If you want to run the post-processing step only, without
    # (re-)running the instantiation itself, uncomment the following
    # (whilst commenting out above instantiations):
    #post_process(processed_folder, basename, base_iri, post_pro_debates)
