# written by Erica Andreose

from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from processor import Processor 
import json
import sqlite3
import pandas as pd



# URIs definition

IntId = URIRef("https://github.com/n1kg0r/ds-project-dhdk/intId/")

# classes
EntityWithMetadata = URIRef("https://github.com/n1kg0r/ds-project-dhdk/classes/EntityWithMetadata")
Collection = URIRef("https://github.com/n1kg0r/ds-project-dhdk/classes/Collection")
Manifest = URIRef("https://github.com/n1kg0r/ds-project-dhdk/classes/Manifest")
Canvas = URIRef("https://github.com/n1kg0r/ds-project-dhdk/classes/Canvas")

# attributes related to classes
ids = URIRef("https://github.com/n1kg0r/ds-project-dhdk/attributes/id")
label = URIRef("https://github.com/n1kg0r/ds-project-dhdk/attributes/label")
title = URIRef("https://github.com/n1kg0r/ds-project-dhdk/attributes/title")
creators = URIRef("https://github.com/n1kg0r/ds-project-dhdk/attributes/creators")

# relations among classes
items = URIRef("https://github.com/n1kg0r/ds-project-dhdk/relations/items")
int_id = URIRef("https://github.com/n1kg0r/ds-project-dhdk/relations/hasInternalId")


class CollectionProcessor(Processor):

    def __init__(self):
        super().__init__()
        self.setdbPathOrUrl = Processor.setDbPathOrUrl #check this

    def uploadData(self, path: str):
        my_graph = Graph()

        try:

            # load the json file
            with open(path, mode='r', encoding="utf-8") as jsonfile:
                json_object = json.load(jsonfile)

            # create an internal id for the canvases using an external counter
            with open("collection_counter.txt", "r") as a:
                collection_counter = int(a.read().strip())

            # create an internal id for the manifest using an external counter
            with open("manifest_counter.txt", "r") as b:
                manifest_counter = int(b.read().strip())

            # create an internal id for the canvases using an external counter
            with open("canvas_counter.txt", "r") as c:
                canvas_counter = int(c.read().strip())
                

                # iterate over it: first step is entering the collection dictionary
                # for collection in json_object:
                collection_id = json_object['id'] 

                # create an internal id splitting the url and taking the number
                collection_counter =+ 1
                collection_IntId = f"Collection_{collection_counter}"
                internal_CollId = URIRef(IntId + collection_IntId)

                # create a list from the dictionary of label and catch the value of the key "none" (language)
                values_list = list(json_object['label'].values())  
                value_label = values_list[0]


                collection_node = URIRef(collection_id)
                my_graph.add((internal_CollId, RDF.type, Collection))
                my_graph.add((internal_CollId, ids, Literal(collection_id)))
                my_graph.add((internal_CollId, label, Literal(value_label)))
                my_graph.add((collection_node, int_id, internal_CollId))

                
                # second step is entering the collection items list
                # here i take the id
                for manifest in json_object["items"]:
                    manifest_id = manifest['id']

                    # create a list from the dictionary of label and catch the value of the key "none" (language)
                    values_list_m = list(manifest['label'].values())  
                    value_label_m = values_list_m[0]

                    # here i raise the counter for the manifest internal id
                    manifest_counter += 1
                    manifest_IntId = f"Manifest_{manifest_counter}"
                    
                    
                    # create the Manifest node with its attributes
                    manifest_idUri = URIRef(manifest_id)
                    manifest_IntId = URIRef(IntId + manifest_IntId)
                    # my_graph.add((manifest_node, int_id, internal_ManId))
                    my_graph.add((manifest_IntId, RDF.type, Manifest))
                    my_graph.add((manifest_IntId, ids, Literal(manifest_id)))
                    my_graph.add((manifest_IntId, label, Literal(value_label_m)))

                    my_graph.add((collection_node, items, manifest_IntId))
                    my_graph.add((manifest_idUri, int_id, manifest_IntId))

                    # third step is entering the manifest items list
                    # here i take the id and the label (entering label's dictionary)
                    for item in manifest["items"]:
                        canvas_id = item['id']

                        # create a list from the dictionary of label and catch the value of the key "none" (language)
                        values_list_c = list(manifest['label'].values())  
                        value_label_c = values_list_c[0]
                        
                        # here i raise the counter for the canvases internal id
                        canvas_counter += 1
                        canvas_IntId = f"Canvas_{canvas_counter}"
                        

                        # create the Canvas node with
                        canvas_node = URIRef(canvas_id)
                        canvas_IntId = URIRef(IntId + canvas_IntId)
                        # my_graph.add((canvas_node, int_id, internal_id))
                        my_graph.add((canvas_IntId, RDF.type, Canvas))
                        my_graph.add((canvas_IntId, ids, Literal(item['id'])))
                        my_graph.add((canvas_IntId, label, Literal(value_label_c)))

                        my_graph.add((manifest_IntId, items, canvas_IntId))
                        my_graph.add((canvas_node, int_id, canvas_IntId))

            with open("manifest_counter.txt", "w") as a:
                a.write(str(manifest_counter))

            with open("canvas_counter.txt", "w") as b:
                b.write(str(canvas_counter))
            
            with open("collection_counter.txt", "w") as c:
                c.write(str(collection_counter))



            store_json = SPARQLUpdateStore()

            endpoint = self.dbPathOrUrl #chech the beginning

            store_json.open((endpoint, endpoint))

            for triple in my_graph.triples((None, None, None)):
                store_json.add(triple)

            store_json.close()


            return True 
    
        except Exception as e:

            print(str(e))
            return False
        

try1 = CollectionProcessor()
try1.dbPathOrUrl = "http://192.168.3.24:9999/blazegraph/sparql"
try1.getDbPathOrUrl()

print(try1.uploadData("collection-1.json"))


try2 = CollectionProcessor()
try2.dbPathOrUrl = "http://192.168.3.24:9999/blazegraph/sparql"
try2.getDbPathOrUrl()

print(try2.uploadData("collection-2.json"))
