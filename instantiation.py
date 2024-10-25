import os
import uuid
import pandas as pd
import json
import xml.etree.ElementTree as ET
from rdflib import Namespace, URIRef, Graph, Literal
from rdflib.namespace import RDF, XSD, OWL
import logging

from common import *
import storeclient
from SPARQLBuilder import SPARQLSelectBuilder, makeVarRef, makeIRIRef, makeLiteralStr
from CommonNamespaces import RDF_TYPE, XSD_STRING

# Namespaces
MMD_PREFIX = "mmd"
MMD_BASE_IRI = TWA_BASE_IRI+"ontomdbstammdaten/"
MMD_NAMESPACE = Namespace(MMD_BASE_IRI)
PD_PREFIX = "pd"
PD_BASE_IRI = TWA_BASE_IRI+"ontoparlamentsdebatten/"
PD_NAMESPACE = Namespace(PD_BASE_IRI)

# Parser states
PS_NONE       = 0
PS_GROUP      = 1
PS_PART_GROUP = 2
PS_PERSON     = 3
PS_BRACKET    = 4

# Comment activities
CA_APPLAUSE       = "Beifall"
CA_MERRYMENT      = "Heiterkeit"
CA_LAUGHTER       = "Lachen"
CA_CONTRADICTION  = "Widerspruch"
CA_UNREST         = "Unruhe"
CA_INTERJECTION   = "Zuruf"
CA_COUNTERJECTION = "Gegenruf"
COMMENT_ACTIVITIES = [CA_APPLAUSE, CA_MERRYMENT,
    CA_LAUGHTER, CA_CONTRADICTION, CA_UNREST]
COMMENT_ACTIVITIES_LONG = COMMENT_ACTIVITIES.copy()
COMMENT_ACTIVITIES_LONG.extend([CA_INTERJECTION, CA_COUNTERJECTION])

def generate_instance_iri(base_iri: str, class_name: str) -> str:
    return f"{base_iri}{class_name}_{str(uuid.uuid4()).replace('-', '_')}"

class ABox:

    def __init__(self, base_iri: str):
        self.graph = Graph()
        self.store_client = storeclient.RdflibStoreClient(g=self.graph)
        self.base_iri = base_iri
        # Cache 'static' relationship references
        self.has_value_ref = make_rel_ref(self.base_iri, "value")
        # Cache dictionary lookup for IRI of parliamentary groups by
        # fragment. To be populated during instantiation.
        self.group_iri_lookup = {}

    def add_prefix(self, prefix: str, namespace: Namespace) -> None:
        self.graph.bind(prefix, namespace)

    def write_to_turtle(self, filename: str) -> None:
        self.graph.serialize(filename, format="turtle")

    def load_tbox(self, filename: str) -> None:
        self.tbox_df = pd.read_csv(f"{filename}.csv")
        with open(f"{filename}.json", "r", encoding=ES_UTF_8) as infile:
            json_str = infile.read()
            self.tbox_dict = json.loads(json_str)
        with open(f"{filename}-customisations.json", "r", encoding=ES_UTF_8) as infile:
            json_str = infile.read()
            self.tbox_customisations = json.loads(json_str)

    def get_group_key(self, group: str) -> str:
        """
        Returns a string to be used as a key (e.g. in a dictionary look-up)
        to identify parliamentary groups. The string of the parliamentary
        group itself cannot be used, because it can appear in different
        grammatical variations in some cases!
        """
        if "LINKE" in group:
            key = "LINKE"
        elif "GRÜNE" in group:
            key = "GRÜNE"
        else:
            key = group.replace("[", "").replace("]", "")
        return key

    def find_inst_with_prop(self, sc: storeclient.StoreClient,
        class_iri: str, rel_iri: str, obj_str: str) -> tuple[str, str]:
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

    def add_new_inst(self, class_name: str, class_iri: str) -> tuple[str, str]:
        inst_iri = generate_instance_iri(self.base_iri, class_name)
        inst_ref = URIRef(inst_iri)
        self.graph.add((inst_ref, RDF.type, URIRef(class_iri)))
        self.graph.add((inst_ref, RDF.type, OWL.NamedIndividual))
        return inst_iri, inst_ref

    def process_originator(self, comment_ref: URIRef, originator: str) -> None:
        parts = originator.split(" ")
        ignore = ["bei", "beim", "von", "der", "des", "dem", "sowie", "und"]
        state = PS_NONE
        cumulative_name = ""
        for part in parts:
            if part.startswith("Weiter"):
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
                    self.graph.add((comment_ref, make_rel_ref(self.base_iri,
                        "fraktion" if state == PS_GROUP else "abgeordnete_von"),
                        #Literal(cumulative_name, datatype=XSD.string)))
                        URIRef(self.group_iri_lookup[self.get_group_key(cumulative_name)])))
                    cumulative_name = ""
                if part == "Abgeordneten":
                    # What follows will be parts of parliamentary groups.
                    state = PS_PART_GROUP
                    cumulative_name = ""
                elif part == "Abg.":
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
                    self.graph.add((comment_ref, make_rel_ref(self.base_iri, "person"),
                        Literal(cumulative_name, datatype=XSD.string)))
                    cumulative_name = ""
        if state == PS_PERSON and cumulative_name != "":
            self.graph.add((comment_ref, make_rel_ref(self.base_iri, "person"),
                Literal(cumulative_name, datatype=XSD.string)))
        if state < PS_PERSON and cumulative_name != "":
            self.graph.add((comment_ref, make_rel_ref(self.base_iri,
                "fraktion" if state == PS_GROUP else "abgeordnete_von"),
                #Literal(cumulative_name, datatype=XSD.string)))
                URIRef(self.group_iri_lookup[self.get_group_key(cumulative_name)])))
        # Debug only!
        self.graph.add((comment_ref, make_rel_ref(self.base_iri, "originator"),
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
        if ((CA_INTERJECTION in first_part) or 
            (CA_COUNTERJECTION in first_part) or recognised):
            recognised = True
            self.graph.add((comment_ref,
                make_rel_ref(self.base_iri, "zwischenruf"),
                Literal(2 if CA_COUNTERJECTION in first_part else 1,
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

    def instantiate_xml_node(self, node: ET.Element, index: int=0,
        parent: ET.Element=None, parent_iri_ref: URIRef=None) -> None:
        next_index = index
        if node.tag in self.tbox_customisations[TC_DELETIONS]:
            log_msg(f"Skipping node '{node.tag}' due to custom deletion.")
        elif node.tag in self.tbox_customisations[TC_REPLACEMENTS]:
            if node.tag == "fraktion":
                log_msg(f"Custom replacement for node '{node.tag}'.")
                rel_iri = make_rel_iri(self.base_iri, "name_kurz")
                class_name = node.tag.capitalize()
                class_iri = self.base_iri+class_name
                # We need to check uniqueness prior to instantiation!
                inst_iri, inst_ref = self.find_inst_with_prop(self.store_client,
                    class_iri, rel_iri, makeLiteralStr(node.text, XSD_STRING))
                if inst_iri is not None:
                    log_msg(f"Re-using instance '{inst_iri}'.")
                else:
                    # No suitable instance exists. Create a new one.
                    inst_iri, inst_ref = self.add_new_inst(class_name, class_iri)
                    self.graph.add((inst_ref,
                        URIRef(rel_iri) , Literal(node.text, datatype=XSD.string)))
                    log_msg(f"Created instance '{inst_iri}'.")
                if parent is not None:
                    # Relate the parent to the instance.
                    self.graph.add((parent_iri_ref,
                        make_rel_ref(self.base_iri, class_name), inst_ref))
                # Cache the instance IRI in a look-up dictionary for later.
                key = self.get_group_key(node.text)
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
                            self.graph.add((inst_ref,
                                make_rel_ref(self.base_iri, attrib[0]),
                                Literal(attrib[1], datatype=XSD.string)))
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
                                    # Remove outside parentheses.
                                    value = value.lstrip("(").rstrip(")")
                                    # Split multi-part comments into parts.
                                    comments = value.split(" –")
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
                                        self.process_comment(cmt_ref, c.strip())
                                        add_inst = True
                                else:
                                    self.graph.add((inst_ref, self.has_value_ref,
                                        Literal(value, datatype=XSD.string)))
                else:
                    # This node has neither children nor attributes. Its text
                    # content will appear as a literal in a datatype property.
                    if node.text is not None and node.text != "":
                        inst_ref = Literal(node.text, datatype=XSD.string)
                    else:
                        inst_ref = None
                if parent is not None:
                    # Relate the parent to the new/existing instance/literal.
                    if inst_ref is not None:
                        self.graph.add((parent_iri_ref,
                            make_rel_ref(self.base_iri, class_name), inst_ref))
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
                        child_index = self.instantiate_xml_node(child,
                            index=child_index, parent=effective_parent,
                            parent_iri_ref=effective_parent_iri_ref)
        return next_index

if __name__ == "__main__":
    download_folder = os.path.join("data", "raw")
    processed_folder = os.path.join("data", "processed")

    #basename = "MDB_STAMMDATEN"
    basename = "20137"

    logging.basicConfig(filename=os.path.join(processed_folder,
        f"{basename}.log"), level=logging.INFO)

    pd_abox = ABox(PD_BASE_IRI)
    pd_abox.add_prefix(PD_PREFIX, PD_NAMESPACE)
    pd_abox.load_tbox(os.path.join(processed_folder, f"{basename}-xml-tbox"))

    tree = ET.parse(os.path.join(download_folder, f"{basename}.xml"))
    root = tree.getroot()

    log_msg("Starting instantiation.")
    pd_abox.instantiate_xml_node(root)

    pd_abox.write_to_turtle(os.path.join(processed_folder, f"{basename}.ttl"))
    log_msg("Finished!")
