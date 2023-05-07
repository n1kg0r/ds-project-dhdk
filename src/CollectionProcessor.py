from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from processor import Processor 
from json import load
import sqlite3
from CreateGraph import create_Graph

class CollectionProcessor(Processor):

    def __init__(self):
        super().__init__()
        self.setdbPathOrUrl = Processor.setDbPathOrUrl #check this

    def uploadData(self, path: str):

        try: 

            base_url = "https://github.com/n1kg0r/ds-project-dhdk/"
            my_graph = Graph()
            

            with open(path, mode='r', encoding="utf-8") as jsonfile:
                json_object = load(jsonfile)
            
            #CREATE GRAPH
            if type(json_object) is list: #CONTROLLARE!!!
                for collection in json_object:
                    create_Graph(collection, base_url, my_graph)
            
            else:
                create_Graph(json_object, base_url, my_graph)
            
                    
            #DB UPTDATE
            store = SPARQLUpdateStore()

            endpoint = self.getDbPathOrUrl()

            store.open((endpoint, endpoint))

            for triple in my_graph.triples((None, None, None)):
                store.add(triple)
            store.close()

            with open('grafo.ttl', mode='a', encoding='utf-8') as f:
                f.write(my_graph.serialize(format='turtle'))

            return True
        
        except Exception as e:
            print(str(e))
            return False
        

try1 = CollectionProcessor()
try1.dbPathOrUrl = "http://192.168.0.168:9999/blazegraph/sparql"
try1.getDbPathOrUrl()

print(try1.uploadData("collection-1.json"))


try2 = CollectionProcessor()
try2.dbPathOrUrl = "http://192.168.0.168:9999/blazegraph/sparql"
try2.getDbPathOrUrl()

print(try2.uploadData("collection-2.json"))

