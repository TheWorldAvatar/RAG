import requests
import json
import xml.etree.ElementTree as ET
from time import sleep
from mergedeep import merge
import os
import pandas as pd
from typing import Callable
import copy
import logging

from common import *

# Format strings
FS_JSON = "json"
FS_XML  = "xml"

# Field names
FN_DOCUMENT   = "document"
FN_DOCUMENTS  = "documents"
FN_DOC_NUMBER = "dokumentnummer"
FN_CURSOR     = "cursor"
FN_NUM_FOUND  = "numFound"
FN_SOURCE     = "fundstelle"
FN_XML_URL    = "xml_url"

# Wait time in seconds to reduce risk of getting blocked.
WAIT_TIME = 1

# TBox CSV column headers
TC_SOURCE     = "Source"
TC_TYPE       = "Type"
TC_TARGET     = "Target"
TC_RELATION   = "Relation"
TC_DOMAIN     = "Domain"
TC_RANGE      = "Range"
TC_QUANTIFIER = "Quantifier"
TC_COMMENT    = "Comment"
TC_DEFINED_BY = "Defined By"
TC_LABEL      = "Label"
tbox_cols = [TC_SOURCE, TC_TYPE, TC_TARGET, TC_RELATION, TC_DOMAIN,
    TC_RANGE, TC_QUANTIFIER, TC_COMMENT, TC_DEFINED_BY, TC_LABEL]

class Result:
    """
    Stores the result returned by an API query
    and provides various utility functions.
    """

    @staticmethod
    def rec_replace_empty_dict(d: dict, subst: dict, default_subst) -> dict:
        newd = {}
        for key in d:
            if isinstance(d[key], dict):
                if len(d[key]) == 0:
                    found = False
                    for sk in subst:
                        if sk in key.lower():
                            newd[key] = subst[sk]
                            found = True
                    if not found:
                        newd[key] = default_subst
                else:
                    newd[key] = Result.rec_replace_empty_dict(
                        d[key], subst, default_subst)
            else:
                newd[key] = d[key]
        return newd

    def consolidate_property_list(self, l: list,
        existing_names: set=None) -> tuple[list, set]:
        cl = []
        c_prop_names = set()
        for prop in l:
            if prop[TC_SOURCE] not in c_prop_names:
                domain_set = set()
                for dup_prop in l:
                    if prop[TC_SOURCE] == dup_prop[TC_SOURCE]:
                        domain_set.add(dup_prop[TC_DOMAIN])
                        if prop[TC_RANGE] != dup_prop[TC_RANGE]:
                            raise Exception(f"Incompatible properties '{prop}' and '{dup_prop}'!")
                c_prop = dict(prop)
                if len(domain_set) > 1:
                    c_prop[TC_DOMAIN] = " UNION ".join(domain_set)
                if existing_names is not None:
                    if c_prop[TC_SOURCE] in existing_names:
                        # We assume we've got a data type property here
                        # whose name clashes with an existing object property.
                        c_prop[TC_SOURCE] = c_prop[TC_SOURCE] + "_lit"
                cl.append(c_prop)
                c_prop_names.add(prop[TC_SOURCE])
        return cl, c_prop_names

    def tbox_collect_def(self, d: dict, elt_name: str,
        ontoiri: str=None) -> tuple[set, list, list]:
        class_set = set()
        op_list = []
        dtp_list = []
        for key in d:
            if isinstance(d[key], dict):
                concept_name = key.capitalize()
                class_set.add(concept_name)
                if elt_name != "":
                    op_row = {TC_SOURCE: f"{PROP_HAS_PREFIX}{concept_name}",
                        TC_TYPE: "Object Property",
                        TC_DOMAIN: elt_name.capitalize(),
                        TC_RANGE: concept_name}
                    if ontoiri is not None:
                        op_row[TC_DEFINED_BY] = ontoiri
                    op_list.append(op_row)
                rcs, rol, rdl = self.tbox_collect_def(
                    d[key], key, ontoiri=ontoiri)
                class_set = class_set.union(rcs)
                op_list.extend(rol)
                dtp_list.extend(rdl)
            else:
                dp_row = {TC_SOURCE: f"{PROP_HAS_PREFIX}{key.capitalize()}",
                    TC_TYPE: "Data Property",
                    TC_DOMAIN: elt_name.capitalize(),
                    TC_RANGE: d[key]}
                if ontoiri is not None:
                    dp_row[TC_DEFINED_BY] = ontoiri
                dtp_list.append(dp_row)
        return class_set, op_list, dtp_list

    def tbox_dict_to_csv(self, d: dict, filename: str,
        ontoname: str=None, ontoiri: str=None, version: str=None) -> None:
        tbox_row_list = []
        if ontoname is not None and ontoiri is not None:
            tbox_row_list.append({TC_SOURCE: ontoname, TC_TYPE: "TBox",
                TC_TARGET: ontoiri,
                TC_RELATION: "https://www.w3.org/2007/05/powder-s#hasIRI"})
        if version is not None:
            tbox_row_list.append({TC_SOURCE: ontoname, TC_TYPE: "TBox",
                TC_TARGET: version,
                TC_RELATION: "http://www.w3.org/2002/07/owl#versionInfo"})
        class_set, op_list, dtp_list = self.tbox_collect_def(
            d, "", ontoiri=ontoiri)
        for c in class_set:
            class_row = {TC_SOURCE: c, TC_TYPE: "Class"}
            if ontoiri is not None:
                class_row[TC_DEFINED_BY] = ontoiri
            tbox_row_list.append(class_row)
        cop_list, op_names = self.consolidate_property_list(op_list)
        tbox_row_list.extend(cop_list)
        cdtp_list, _ = self.consolidate_property_list(dtp_list, op_names)
        tbox_row_list.extend(cdtp_list)
        tbox_df = pd.DataFrame(tbox_row_list, columns=tbox_cols)
        tbox_df.to_csv(filename, encoding='utf-8', index=False)

    def __init__(self, r: requests.Response=None) -> None:
        self.content = r

    def write_to_file(self, filename: str) -> None:
        raise Exception(f"Writing to file '{filename}' for this result type is not implemented!")

    def read_from_file(self, filename: str) -> None:
        raise Exception(f"Reading from file '{filename}' for this result type is not implemented!")

    def get_num_found(self) -> int:
        return None

    def get_cursor(self) -> str:
        return None

    def download_xml_sources(self, foldername: str) -> None:
        raise Exception(f"Downloading XML sources into '{foldername}' for this result type is not implemented!")

class JSONResult(Result):

    def set_content(self, c: dict) -> None:
        self.content = c
        num_found = self.get_num_found()
        if num_found is not None:
            log_msg(f"JSON result reports {num_found} found entries.")
        num_docs = self.count_num_documents()
        if num_docs is not None:
            log_msg(f"JSON result contains {num_docs} documents.")

    def __init__(self, r: requests.Response=None) -> None:
        # The result of a query in JSON format is a dictionary.
        self.set_content({} if r is None else r.json())

    def write_to_file(self, filename: str) -> None:
        export_dict_to_json(self.content, filename)

    def read_from_file(self, filename: str) -> None:
        with open(filename, "r", encoding=ES_UTF_8) as infile:
            json_str = infile.read()
            self.set_content(json.loads(json_str))

    def get_num_found(self) -> int:
        return self.content[FN_NUM_FOUND] if FN_NUM_FOUND in self.content else None

    def get_cursor(self) -> str:
        return self.content[FN_CURSOR] if FN_CURSOR in self.content else None

    def count_num_documents(self) -> int:
        return len(self.content[FN_DOCUMENTS]) if FN_DOCUMENTS in self.content else None

    def get_document_xml_urls(self) -> list[str]:
        url_list = []
        # Iterate through documents
        for doc in self.content[FN_DOCUMENTS]:
            try:
                url = doc[FN_SOURCE][FN_XML_URL]
            except:
                url = None
                log_msg(f"Document number '{doc[FN_DOC_NUMBER]}' does not have an XML URL!",
                    level=logging.WARN)
            if url is not None:
                url_list.append(url)
        log_msg(f"{len(url_list)} out of {self.count_num_documents()} documents have XML URLs.")
        return url_list

    def download_xml_sources(self, foldername: str) -> None:
        url_list = self.get_document_xml_urls()
        for url in url_list:
            log_msg(f"Downloading '{url}'...")
            resp = requests.get(url)
            if resp.status_code == 200:
                resp.encoding = ES_UTF_8
                with open(os.path.join(foldername, url.rsplit("/", 1)[1]),
                    "w", encoding=ES_UTF_8) as outfile:
                    outfile.write(resp.text)
            else:
                log_msg(f"Download failed! Code {resp.status_code}.",
                    level=logging.WARN)
            # Wait before the next request - we don't want to get blocked!
            sleep(WAIT_TIME)

    def _extract_node(self, elt, elt_name: str) -> dict:
        if isinstance(elt, dict):
            d = {}
            for key in elt:
                merge(d, self._extract_node(elt[key], key))
            # Empty dictionaries *will* count as classes.
            return {elt_name: d}
        elif isinstance(elt, list):
            d = {}
            for item in elt:
                merge(d, self._extract_node(item, elt_name))
            # Empty lists will *not* count as classes.
            # Uncomment if you change your mind, and remove comment!
            return d #if len(elt)>0 else {elt_name: {}}
        else:
            return {elt_name: type(elt).__name__}

    def generate_tbox(self, filename: str, ontoname: str=None,
        ontoiri: str=None, version: str=None,
        customise: Callable[[dict, str], dict]=None) -> None:
        # First step: create a dictionary of the class/property hierarchy
        tbox_dict = self._extract_node(self.content[FN_DOCUMENTS], FN_DOCUMENT)
        if customise is not None:
            tbox_dict = customise(tbox_dict, f"{filename}-customisations.json")
        export_dict_to_json(tbox_dict, f"{filename}.json")
        # Second step: turn it into a list of class/property definitions,
        # to be exported to csv
        self.tbox_dict_to_csv(tbox_dict, f"{filename}.csv",
            ontoname=ontoname, ontoiri=ontoiri, version=version)

class XMLResult(Result):

    def set_content(self, c: ET.Element) -> None:
        self.content = c
        num_found = self.get_num_found()
        if num_found is not None:
            log_msg(f"XML result reports {num_found} found entries.")

    def __init__(self, r: requests.Response=None) -> None:
        # The result of a query in XML format is the root of an XML tree.
        self.set_content(None if r is None else ET.fromstring(r.text))

    def write_to_file(self, filename: str) -> None:
        tree = ET.ElementTree(self.content)
        tree.write(filename, encoding=ES_UTF_8)

    def read_from_file(self, filename: str) -> None:
        tree = ET.parse(filename)
        self.set_content(tree.getroot())

    def get_num_found(self) -> int:
        node = self.content.find(FN_NUM_FOUND) if \
            self.content is not None else None
        return None if node is None else int(node.text)

    def get_cursor(self) -> str:
        node = self.content.find(FN_CURSOR)
        return None if node is None else node.text

    def _extract_node(self, node: ET.Element) -> dict:
        d = {}
        # Attributes count as datatype properties.
        # NB If a node contains no children but attributes, it still
        # counts as a class.
        for attrib in node.items():
            # NB There is no good way to infer the type of the field,
            # so we have to assume string!
            d[attrib[0]] = LDTS_STRING
        # Capture text value before the first subelement (!).
        val = node.text.strip(' \t\n\r') if node.text is not None else ""
        if len(node) > 0:
            # This node has children.
            for child in node:
                child_d = self._extract_node(child)
                # Capture text value after any subelement (!).
                if child.tail is not None:
                    tail = child.tail.strip(' \t\n\r')
                    if tail != "":
                        if len(child_d[child.tag]) == 0:
                            child_d[child.tag]["value"] = LDTS_STRING
                        child_d[child.tag]["tail"] = LDTS_STRING
                # We need a deep merge here - a shallow merge does not
                # do the job!
                #d = {**d, **child_d}
                merge(d, child_d)
        if (val != ""):
            # Add a value field only if there are already other fields!
            if len(d) > 0:
                d["value"] = LDTS_STRING
        # NB A node with no children and no attributes will be
        # returned as empty dictionary, so that it can potentially
        # be merged with other occurrences that do!
        return {node.tag: d}

    def generate_tbox(self, filename: str, ontoname: str=None,
        ontoiri: str=None, version: str=None,
        customise: Callable[[dict, str], dict]=None) -> None:
        # First step: create a dictionary of the class/property hierarchy
        tbox_dict = self._extract_node(self.content)
        # All remaining empty dictionary entries will be data properties.
        subst = {
            "datum": LDTS_DATE,
            "_von": LDTS_DATE,
            "_bis": LDTS_DATE,
            "uhrzeit": LDTS_TIME
        }
        tbox_dict = Result.rec_replace_empty_dict(tbox_dict, subst, LDTS_STRING)
        if customise is not None:
            tbox_dict = customise(tbox_dict, f"{filename}-customisations.json")
        export_dict_to_json(tbox_dict, f"{filename}.json")
        # Second step: turn it into a list of class/property definitions,
        # to be exported to csv
        self.tbox_dict_to_csv(tbox_dict, f"{filename}.csv",
            ontoname=ontoname, ontoiri=ontoiri, version=version)

class DIP_API_client:
    """
    Provides a client to retrieve information from the API provided
    by the documentation and information system for parliamentary materials
    (Dokumentations- und Informationssystem für Parlamentsmaterialien, DIP)
    of the German parliament:
    https://dip.bundestag.de/

    XML and JSON returned by the API seem to be equivalent. The XML
    nodes do not seem to contain any attributes.

    In the minutes text resource responses, the "text" node is
    entirely plain, with no mark-up whatsoever.
    The XML files from https://www.bundestag.de/services/opendata
    seem to contain the same content as the "text" fields, but
    substantially marked up! The content of these XML files does not
    appear to be accessible directly via the API, but appears to be
    available at "dokument" -> "fundstelle" -> "xml_url"!

    The person resource responses seem to differ significantly
    from both the speaker nodes in the minute XML files and the
    entries in the "MdB Stammdaten" XML file from the open data page.
    """
    DIP_API_BASE_URL = "https://search.dip.bundestag.de/api/v1/"

    # There is an official, public API key provided by the
    # government at:
    # https://dip.bundestag.de/%C3%BCber-dip/hilfe/api#content
    # This one is valid until 31 May 2025:
    API_KEY = "I9FKdCn.hbfefNWCY336dL6x62vfwNKpoN2RZ1gp21"

    # Resource types
    RT_TRANSACTION     = "vorgang"
    RT_TRANSACTION_POS = "vorgangsposition"
    RT_CIRCULAR        = "drucksache"
    RT_CIRCULAR_TEXT   = "drucksache-text"
    RT_MINUTES         = "plenarprotokoll"
    RT_MINUTES_TEXT    = "plenarprotokoll-text"
    RT_ACTIVITY        = "aktivitaet"
    RT_PERSON          = "person"

    @staticmethod
    def get_url(resource_type: str, id: str=None) -> str:
        """
        Returns a URL to which API requests can be sent. If no ID is
        given, the URL is meant to provide a list of all entities of
        a given resource type. If an ID is given, the URL is meant to
        provide an entity of a given resource type referenced by ID.
        """
        url = "".join([DIP_API_client.DIP_API_BASE_URL, resource_type])
        if (id is not None) and (id != ""):
            url = "".join([url, "/", id])
        return url

    @staticmethod
    def query(resource_type: str, id: str=None, format: str=None,
        start_date: str=None, end_date: str=None,
        cursor: str=None) -> requests.Response:
        """
        Sends a single request to the API via HTTP. If no format is
        given, the API apparently defaults to returning JSON.
        """
        params = {"apikey": DIP_API_client.API_KEY}
        if format is not None:
            params["format"] = format
        if start_date is not None:
            params["f.datum.start"] = start_date
        if end_date is not None:
            params["f.datum.end"] = end_date
        if cursor is not None:
            params[FN_CURSOR] = cursor
        url = DIP_API_client.get_url(resource_type, id=id)
        log_msg(f"Querying {url}...")
        log_msg(f"Parameters: {params}")
        resp = requests.get(url, params=params)
        resp.encoding = ES_UTF_8
        # Wait before the next request - we don't want to get blocked!
        sleep(WAIT_TIME)
        return resp

    @staticmethod
    def query_result(resource_type: str, id: str=None, format: str=None,
        start_date: str=None, end_date: str=None,
        previous: Result=None) -> Result:
        resp = DIP_API_client.query(resource_type, id=id, format=format,
            start_date=start_date, end_date=end_date,
            cursor=None if previous is None else previous.get_cursor())
        if (format is None) or (format == FS_JSON):
            return JSONResult(resp)
        elif format == FS_XML:
            return XMLResult(resp)
        else:
            raise Exception(f"API query format '{format}' is not implemented!")

    @staticmethod
    def download_all(resource_type: str, foldername: str, basename: str,
        start_date: str=None, end_date: str=None,
        incl_xml_src: bool=False) -> None:
        log_msg("Starting data acquisition.")
        # Use JSON format for API responses, as this gives more
        # information on the data types of certain fields.
        fmt = FS_JSON
        current = DIP_API_client.query_result(resource_type, format=fmt,
            start_date=start_date, end_date=end_date)
        current_cursor = current.get_cursor()
        current.write_to_file(os.path.join(foldername,
            f"{basename}-{current_cursor}.{fmt}"))
        if incl_xml_src:
            current.download_xml_sources(foldername)
        previous_cursor = None
        while current_cursor != previous_cursor:
            previous = current
            previous_cursor = current_cursor
            current = DIP_API_client.query_result(resource_type,
                format=fmt, start_date=start_date, end_date=end_date,
                previous=previous)
            current_cursor = current.get_cursor()
            if current_cursor != previous_cursor:
                current.write_to_file(os.path.join(foldername,
                    f"{basename}-{current_cursor}.{fmt}"))
                if incl_xml_src:
                    current.download_xml_sources(foldername)
        log_msg("Finished!")

def shortcut_nodes(d: dict, nodes: list[str],
    nodes_wp: dict, parent: str=None) -> dict:
    scd = {}
    for key in d:
        if isinstance(d[key], dict):
            tmp_d = shortcut_nodes(d[key], nodes, nodes_wp, parent=key)
            keep = True
            if key in nodes:
                keep = False
            else:
                if key in nodes_wp:
                    if nodes_wp[key] == parent:
                        keep = False
            if keep:
                # We need to keep this node.
                scd[key] = tmp_d
            else:
                # We need to shortcut this node.
                scd.update(tmp_d)
        else:
            # This node is not a dictionary.
            if key in nodes:
                # We need to shortcut this node. By convention,
                # we remove it, i.e. do nothing here.
                pass
            else:
                # We need to keep this node.
                scd[key] = d[key]
    return scd

def delete_nodes(d: dict, nodes: list[str]) -> dict:
    dd = {}
    for key in d:
        if key not in nodes:
            # We need to keep this node.
            if isinstance(d[key], dict):
                tmp_d = delete_nodes(d[key], nodes)
                if len(tmp_d) > 0:
                    dd[key] = tmp_d
            else:
                dd[key] = d[key]
    return dd

def add_index_fields(d: dict, nodes: list[str]) -> dict:
    ad = {}
    for key in d:
        if isinstance(d[key], dict):
            tmp_d = add_index_fields(d[key], nodes)
            if key in nodes:
                tmp_d["index"] = "int"
            ad[key] = tmp_d
        else:
            if key in nodes:
                ad[key] = {"value": d[key], "index": "int"}
            else:
                ad[key] = d[key]
    return ad

def replace_nodes(d: dict, rep: dict) -> dict:
    rd = {}
    for key in d:
        if key in rep:
            # Replace the node.
            if isinstance(rep[key], dict):
                rd[key] = copy.deepcopy(rep[key])
            else:
                rd[key] = rep[key]
        else:
            if isinstance(d[key], dict):
                rd[key] = replace_nodes(d[key], rep)
            else:
                rd[key] = d[key]
    return rd

def customise_stammdaten(d: dict, cfilename: str) -> dict:
    shortcuts = ["DOCUMENT", "VERSION", "NAMEN",
        "BIOGRAFISCHE_ANGABEN", "WAHLPERIODEN", "INSTITUTIONEN"]
    customisations = {TC_DELETIONS: [], TC_SHORTCUTS: shortcuts,
        TC_SHORTCUTS_WP: {}, TC_INDEX_FIELDS: {}, TC_REPLACEMENTS: {}}
    export_dict_to_json(customisations, cfilename)
    return shortcut_nodes(d, shortcuts, {})

def generate_stammdaten_tbox(in_folder: str, out_folder: str) -> None:
    fmt = FS_XML
    r = XMLResult()
    basename = "MDB_STAMMDATEN"
    ontoname="OntoMdBStammdaten"
    r.read_from_file(os.path.join(in_folder,
        f"{basename}.{fmt}"))
    r.generate_tbox(os.path.join(out_folder,
        f"{basename}-{fmt}-tbox"), ontoname=ontoname,
        ontoiri=f"{TWA_BASE_IRI}{ontoname.lower()}/",
        version="1", customise=customise_stammdaten)

def customise_debatten(d: dict, cfilename: str) -> dict:
    # Remove "kopfdaten" altogether, as the only not redundant node
    # is "berichtart", which seems to be constant. All other nodes
    # appear to be repeated as attributes to the root node.
    deletions = ["kopfdaten"]
    cd = delete_nodes(d, deletions)
    # Remove unnecessary class layers in order to reduce depth.
    shortcuts = ["inhaltsverzeichnis", "rolle"]
    shortcuts_with_parents = {"name": "redner"}
    cd = shortcut_nodes(cd, shortcuts, shortcuts_with_parents)
    # Nodes which need to carry an index, because their order matters.
    index_fields = ["ivz-block", "ivz-eintrag",
        "tagesordnungspunkt", "rede", "p", "kommentar"]
    cd = add_index_fields(cd, index_fields)
    # Nodes which need to be classes, rather than just literals.
    replacements = {
        "fraktion": {"name_kurz": LDTS_STRING, "name_lang": LDTS_STRING}
    }
    cd = replace_nodes(cd, replacements)
    # Serialise customisations to JSON for future reference.
    customisations = {TC_DELETIONS: deletions, TC_SHORTCUTS: shortcuts,
        TC_SHORTCUTS_WP: shortcuts_with_parents,
        TC_INDEX_FIELDS: index_fields, TC_REPLACEMENTS: replacements}
    export_dict_to_json(customisations, cfilename)
    return cd

if __name__ == "__main__":
    download_folder = os.path.join("data", "raw")
    res_type = DIP_API_client.RT_MINUTES
    year_str = "2023"
    # WARNING: Uncomment this only if you are sure that
    # that is what you want to do!
    #logging.basicConfig(filename=os.path.join(download_folder,
    #    f"{res_type}-{year_str}.log"), level=logging.INFO)
    #DIP_API_client.download_all(res_type, download_folder,
    #    f"{res_type}-{year_str}", start_date=f"{year_str}-01-01",
    #    end_date=f"{year_str}-12-31", incl_xml_src=True)

    processed_folder = os.path.join("data", "processed")
    #generate_stammdaten_tbox(download_folder, processed_folder)

    # TBox for a JSON file of a given resource type
    #fmt = FS_JSON
    #r = JSONResult()
    #cursor = "AoJw0Oi09ZEDK1BlcnNvbi03NjI1"
    #cursor = "AoJwuKSi4oUDNFBsZW5hcnByb3Rva29sbC01NTQy"
    #cursor = "AoJwuJG_gIwDNFBsZW5hcnByb3Rva29sbC01NjEx"
    #r.read_from_file(os.path.join(download_folder,
    #    f"{res_type}-{year_str}-{cursor}.{fmt}"))
    #r.generate_tbox(os.path.join(processed_folder,
    #    f"{res_type}-{year_str}-{cursor}-{fmt}-tbox"))

    # TBox for debate XML files
    #fmt = FS_XML
    #r = XMLResult()
    #number = "20137"
    #r.read_from_file(os.path.join(download_folder,
    #    f"{number}.{fmt}"))
    #r.generate_tbox(os.path.join(processed_folder,
    #    f"{number}-{fmt}-tbox"), ontoname="OntoParlamentsdebatten",
    #    ontoiri="https://www.theworldavatar.com/kg/ontoparlamentsdebatten/",
    #    version="1", customise=customise_debatten)

    #r = DIP_API_client.query_result(res_type, format=fmt,
    #    start_date=f"{year_str}-01-01", end_date=f"{year_str}-12-31")
    #r.write_to_file(os.path.join(download_folder,
    #    f"{res_type}-{year_str}-{r.get_cursor()}.{fmt}"))
