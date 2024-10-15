import os
import uuid
import pandas as pd
import json
import xml.etree.ElementTree as ET
from rdflib import Namespace, URIRef, Graph, Literal
from rdflib.namespace import RDF
import logging

from common import *

PD_PREFIX = "pd"
PD_BASE_IRI = TWA_BASE_IRI+"ontoparlamentsdebatten/"
PD_NAMESPACE = Namespace(PD_BASE_IRI)

def generate_instance_iri(base_iri: str, class_name: str) -> str:
    return f"{base_iri}{class_name}_{str(uuid.uuid4()).replace('-', '_')}"

class ABox:

    def __init__(self, base_iri: str):
        self.graph = Graph()
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

    def instantiate_xml_node(self, node: ET.Element,
        parent: ET.Element=None, parent_iri_ref: URIRef=None) -> None:
        if node.tag in self.tbox_customisations[TC_DELETIONS]:
            log_msg(f"Skipping node '{node.tag}' due to custom deletion.")
        elif node.tag in self.tbox_customisations[TC_REPLACEMENTS]:
            if node.tag == "fraktion":
                log_msg(f"Custom replacement for node '{node.tag}'.")
                # TODO: We need to check uniqueness prior to instantiation!
                class_name = node.tag.capitalize()
                inst_iri = generate_instance_iri(self.base_iri, class_name)
                inst_ref = URIRef(inst_iri)
                self.graph.add((inst_ref,
                    RDF.type , URIRef(self.base_iri+class_name)))
                self.graph.add((inst_ref,
                    URIRef(self.base_iri+"hatName_kurz") , Literal(node.text)))
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
                if (len(node) > 0) or (len(attribs) > 0):
                    # This needs to be a new instance, not a literal.
                    # TODO: We need to check uniqueness of redner using ID prior to instantiation!
                    inst_iri = generate_instance_iri(self.base_iri, class_name)
                    new_ref = URIRef(inst_iri)
                    self.graph.add((new_ref,
                        RDF.type , URIRef(self.base_iri+class_name)))
                    log_msg(f"Created instance '{inst_iri}'.")
                    # Represent node attributes as literals using datatype properties.
                    for attrib in attribs:
                        rel = URIRef(f"{self.base_iri}hat{attrib[0].capitalize()}")
                        self.graph.add((new_ref,
                            rel , Literal(attrib[1])))
                    # Add node text as value datatype property.
                    if node.text is not None:
                        value = node.text.strip()
                        if value != "":
                            rel = URIRef(f"{self.base_iri}hatValue")
                            self.graph.add((new_ref, rel , Literal(value)))
                else:
                    # This node has neither children nor attributes. Its text
                    # content will appear as a literal in a datatype property.
                    new_ref = Literal(node.text)
                if parent is not None:
                    # Relate the parent to the new instance/literal.
                    rel = URIRef(f"{self.base_iri}hat{class_name}")
                    self.graph.add((parent_iri_ref, rel, new_ref))
                effective_parent = node
                effective_parent_iri_ref = new_ref
            # Instantiate children, if any, recursively.
            for child in node:
                self.instantiate_xml_node(child, parent=effective_parent,
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
