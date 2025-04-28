from storeclient import StoreClient, RemoteStoreClient
from SPARQLBuilder import SPARQLSelectBuilder, makeVarRef, makeIRIRef
import SPARQLConstants
from CommonNamespaces import RDF_TYPE, OWL_DATATYPEPROPERTY, OWL_OBJECTPROPERTY
from ragconfig import *

def get_entity_of_type(sc: StoreClient, t: str) -> list[str]:
    sb = SPARQLSelectBuilder()
    entity_var_name = "e"
    sb.addVar(makeVarRef(entity_var_name))
    sb.addWhere(makeVarRef(entity_var_name),
        makeIRIRef(RDF_TYPE), makeIRIRef(t))
    reply = sc.query(sb.build())
    result_bindings = reply["results"]["bindings"]
    entities = []
    for b in result_bindings:
        entities.append(b[entity_var_name]["value"])
    return entities

def get_properties(sc: StoreClient, obj_filter: str=None) -> list[str]:
    sb = SPARQLSelectBuilder()
    subj_var_name = "s"
    pred_var_name = "p"
    obj_var_name = "o"
    sb.addVar(makeVarRef(pred_var_name))
    sb.set_distinct()
    sb.addWhere(makeVarRef(subj_var_name),
        makeVarRef(pred_var_name), makeVarRef(obj_var_name))
    if obj_filter is not None:
        sb.addFilter(f"{obj_filter}({makeVarRef(obj_var_name)})")
    reply = sc.query(sb.build())
    result_bindings = reply["results"]["bindings"]
    properties = []
    for b in result_bindings:
        properties.append(b[pred_var_name]["value"])
    return properties

def main():
    config = RAGConfig("config.yaml")
    config.check()

    tbox_endpoint = config.get(CVN_TBOX_ENDPOINT)
    tbox_client = RemoteStoreClient(tbox_endpoint)
    tbox_dtps = set(get_entity_of_type(tbox_client, OWL_DATATYPEPROPERTY))
    tbox_ops = set(get_entity_of_type(tbox_client, OWL_OBJECTPROPERTY))
    abox_endpoint = config.get(CVN_ENDPOINT)
    abox_client = RemoteStoreClient(abox_endpoint)
    abox_dtps = set(get_properties(abox_client,
        obj_filter=SPARQLConstants.ISLITERAL))
    abox_ops = set(get_properties(abox_client,
        obj_filter=SPARQLConstants.ISIRI))
    print("Object properties in TBox but not in ABox:")
    print(tbox_ops-abox_ops)
    print("Object properties in ABox but not in TBox:")
    print(abox_ops-tbox_ops)
    print("Datatype properties in TBox but not in ABox:")
    print(tbox_dtps-abox_dtps)
    print("Datatype properties in ABox but not in TBox:")
    print(abox_dtps-tbox_dtps)

if __name__ == "__main__":
    main()
