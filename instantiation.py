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

DEBUG = False

# Parser states
PS_NONE       = 0
PS_GROUP      = 1
PS_PART_GROUP = 2
PS_PERSON     = 3
PS_BRACKET    = 4

# Special characters
NO_BREAK_SPACE = chr(160)
EN_DASH        = chr(8211)

def generate_instance_iri(base_iri: str, class_name: str) -> str:
    return f"{base_iri}{class_name}_{str(uuid.uuid4()).replace('-', '_')}"

class ABox:

    def __init__(self, base_iri: str, mdb_lookup: dict[str, str]=None):
        self.graph = Graph()
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

    def load_tbox(self, filename: str) -> None:
        self.tbox_df = pd.read_csv(f"{filename}.csv")
        with open(f"{filename}.json", "r", encoding=ES_UTF_8) as infile:
            json_str = infile.read()
            self.tbox_dict = json.loads(json_str)
        with open(f"{filename}-customisations.json", "r", encoding=ES_UTF_8) as infile:
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
        if "LINKE" in group:
            key = "LINKE"
        elif "GRÜNE" in group:
            key = "GRÜNE"
        else:
            key = group.replace("[", "").replace("]", "")
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
        # Try to find the name in the MdB name/ID look-up
        if name in self.mdb_lookup:
            self.graph.add((comment_ref, make_rel_ref(self.base_iri, "id"),
                Literal(self.mdb_lookup[name], datatype=XSD.string)))
            # Add parliamentary group
            self.graph.add((comment_ref, make_rel_ref(self.base_iri, "mdb_von"),
                URIRef(self.group_iri_lookup[self.get_group_key(parts[-1])])))
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
        ignore = ["bei", "beim", "von", "der", "des", "dem", "sowie", "und"]
        state = PS_NONE
        cumulative_name = ""
        for part in parts:
            if part.startswith("Weiter"):
                continue
            if part.startswith("Anhaltend"):
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
                    self.link_comment_to_mdb(comment_ref, cumulative_name)
                    cumulative_name = ""
        if state == PS_PERSON and cumulative_name != "":
            self.link_comment_to_mdb(comment_ref, cumulative_name)
        if state < PS_PERSON and cumulative_name != "":
            self.graph.add((comment_ref, make_rel_ref(self.base_iri,
                "fraktion" if state == PS_GROUP else "abgeordnete_von"),
                #Literal(cumulative_name, datatype=XSD.string)))
                URIRef(self.group_iri_lookup[self.get_group_key(cumulative_name)])))
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
                                    # Remove outside parentheses.
                                    value = value.lstrip("(").rstrip(")")
                                    # Split multi-part comments into parts.
                                    comments = value.split(NO_BREAK_SPACE+EN_DASH)
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
                        child_index = self.instantiate_xml_node(child,
                            index=child_index, parent=effective_parent,
                            parent_iri_ref=effective_parent_iri_ref)
        return next_index

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
        key = " ".join(strlist)
        name_id_lookup[key] = id_str
    return name_id_lookup

def instantiate_xml(infolder: str, outfolder: str, basename: str,
    base_iri: str, prefixes: dict[str, str],
    mdb_lookup: dict[str, str]=None) -> None:
    logging.basicConfig(filename=os.path.join(outfolder,
        f"{basename}.log"), encoding=ES_UTF_8, level=logging.INFO)
    the_abox = ABox(base_iri, mdb_lookup=mdb_lookup)
    for prefix in prefixes:
        the_abox.add_prefix(prefix, prefixes[prefix])
    # NB Even though we load the TBox as an input here, it was previously
    # produced as an output, hence the location.
    the_abox.load_tbox(os.path.join(outfolder, f"{basename}-xml-tbox"))
    # Parse XML input file.
    tree = ET.parse(os.path.join(infolder, f"{basename}.xml"))
    root = tree.getroot()
    log_msg("Starting instantiation.")
    the_abox.instantiate_xml_node(root)
    the_abox.write_to_turtle(os.path.join(outfolder, f"{basename}.ttl"))
    log_msg("Finished!")

if __name__ == "__main__":
    download_folder = os.path.join("data", "raw")
    processed_folder = os.path.join("data", "processed")

    basename = "MDB_STAMMDATEN"
    #base_iri = MMD_BASE_IRI
    #prefixes = {MMD_PREFIX: MMD_NAMESPACE}

    #instantiate_xml(download_folder, processed_folder, basename,
    #    base_iri, prefixes)

    mdb_sc = storeclient.RdflibStoreClient(filename=
        os.path.join(processed_folder, basename+".ttl"))
    mdb_name_id_lookup = make_mdb_name_id_lookup(mdb_sc)
    #export_dict_to_json(mdb_name_id_lookup,
    #    os.path.join(processed_folder, "MdB-lookup.json"))

    basename = "20137"
    base_iri = PD_BASE_IRI
    prefixes = {MMD_PREFIX: MMD_NAMESPACE, PD_PREFIX: PD_NAMESPACE}

    instantiate_xml(download_folder, processed_folder, basename,
        base_iri, prefixes, mdb_name_id_lookup)
