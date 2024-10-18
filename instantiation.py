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

PD_PREFIX = "pd"
PD_BASE_IRI = TWA_BASE_IRI+"ontoparlamentsdebatten/"
PD_NAMESPACE = Namespace(PD_BASE_IRI)

def generate_instance_iri(base_iri: str, class_name: str) -> str:
    return f"{base_iri}{class_name}_{str(uuid.uuid4()).replace('-', '_')}"

class ABox:

    def __init__(self, base_iri: str):
        self.graph = Graph()
        self.store_client = storeclient.RdflibStoreClient(g=self.graph)
        self.base_iri = base_iri

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

    def instantiate_xml_node(self, node: ET.Element, index: int=0,
        parent: ET.Element=None, parent_iri_ref: URIRef=None) -> None:
        if node.tag in self.tbox_customisations[TC_DELETIONS]:
            log_msg(f"Skipping node '{node.tag}' due to custom deletion.")
        elif node.tag in self.tbox_customisations[TC_REPLACEMENTS]:
            if node.tag == "fraktion":
                log_msg(f"Custom replacement for node '{node.tag}'.")
                rel_iri = self.base_iri+"hatName_kurz"
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
                    rel = URIRef(f"{self.base_iri}hat{class_name}")
                    self.graph.add((parent_iri_ref, rel, inst_ref))
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
                        rel_iri = self.base_iri+"hatId"
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
                            rel = URIRef(f"{self.base_iri}hat{attrib[0].capitalize()}")
                            self.graph.add((inst_ref,
                                rel , Literal(attrib[1], datatype=XSD.string)))
                        # Add index field if desired.
                        if node.tag in self.tbox_customisations[TC_INDEX_FIELDS]:
                            rel = URIRef(f"{self.base_iri}hatIndex")
                            self.graph.add((inst_ref,
                                rel , Literal(index, datatype=XSD.int)))
                        # Add node text as value datatype property.
                        if node.text is not None:
                            value = node.text.strip()
                            if value != "":
                                rel = URIRef(f"{self.base_iri}hatValue")
                                if class_name == "Kommentar":
                                    value = value.lstrip("(").rstrip(")")
                                    comments = value.split(" –")
                                    for c in comments:
                                        self.graph.add((inst_ref, rel,
                                            Literal(c.strip(), datatype=XSD.string)))
                                else:
                                    self.graph.add((inst_ref, rel,
                                        Literal(value, datatype=XSD.string)))
                else:
                    # This node has neither children nor attributes. Its text
                    # content will appear as a literal in a datatype property.
                    inst_ref = Literal(node.text, datatype=XSD.string)
                if parent is not None:
                    # Relate the parent to the new/existing instance/literal.
                    rel = URIRef(f"{self.base_iri}hat{class_name}")
                    self.graph.add((parent_iri_ref, rel, inst_ref))
                effective_parent = node
                effective_parent_iri_ref = inst_ref
            # Instantiate children, if any, recursively.
            if is_new_inst:
                child_index = 0
                for child in node:
                    child_index += 1
                    self.instantiate_xml_node(child, index=child_index,
                        parent=effective_parent,
                        parent_iri_ref=effective_parent_iri_ref)

if __name__ == "__main__":
    download_folder = os.path.join("data", "raw")
    processed_folder = os.path.join("data", "processed")

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
