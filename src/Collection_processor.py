# written by Erica Andreose

from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from processors import Processor
import json



# URIs definition

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


class CollectionProcessor(Processor):

    def __init__(self):
        super().__init__()

    def uploadData(self, path: str):
        my_graph = Graph()

        if path:

            # load the json file
            with open(path, mode='r', encoding="utf-8") as jsonfile:
                json_object = json.load(jsonfile)

                # iterate over it: first step is entering the collection dictionary
                for collection in json_object:
                    collection_id = json_object['id']

                    collection_node = URIRef(collection_id)
                    my_graph.add((collection_node, RDF.type, Collection))
                    my_graph.add((collection_node, ids, Literal(collection_id)))
                    my_graph.add((collection_node, label, Literal(json_object['label']['none'][0])))

                    # second step is entering the collection items list
                    # here i take the id
                    for manifest in json_object["items"]:
                        manifest_id = manifest['id']

                        # create the Manifest node with its attributes
                        manifest_node = URIRef(manifest_id)
                        my_graph.add((manifest_node, RDF.type, Manifest))
                        my_graph.add((manifest_node, ids, Literal(manifest_id)))
                        my_graph.add((manifest_node, label, Literal(manifest['label']['none'][0])))

                        # third step is entering the manifest items list
                        # here i take the id and the label (entering label's dictionary)
                        for item in manifest["items"]:
                            canvas_id = item['id']
                            canvas_label = item['label']['none'][0]

                            # create the Canvas node with
                            canvas_node = URIRef(canvas_id)
                            my_graph.add((canvas_node, RDF.type, Canvas))
                            my_graph.add((canvas_node, ids, Literal(item['id'])))
                            my_graph.add((canvas_node, label, Literal(canvas_label)))

                # create the items relation between the Manifest and the Canvas
                my_graph.add((collection_node, items, manifest_node))
                my_graph.add((manifest_node, items, canvas_node))

            store_json = SPARQLUpdateStore()

            endpoint = self.dbPathOrUrl

            store_json.open((endpoint, endpoint))

            for triple in my_graph.triples((None, None, None)):
                store_json.add(triple)

            store_json.close()

            return True 
        else: 
            return False
        

try1 = CollectionProcessor()
try1.dbPathOrUrl = "http://192.168.1.37:9999/blazegraph/sparql"
try1.getDbPathOrUrl()
print(try1.uploadData("data/collection-1.json"))


# try2 = CollectionProcessor()
# try2.dbPathOrUrl = "http://192.168.0.168:9999/blazegraph/sparql"
# try2.getdbPathOrUrl()
# try2.uploadData("collection-2.json")

# print(try2.uploadData("collection-2.json"))
