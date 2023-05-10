from processor import *
from QueryProcessor import QueryProcessor
from pandas import DataFrame
from sparql_dataframe import get 
from clean_str import remove_special_chars

class TriplestoreQueryProcessor(QueryProcessor):

    def __init__(self):
        super().__init__()

    def getAllCanvases(self):

        endpoint = Processor.getDbPathOrUrl
        query_canvases = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?canvas ?id ?label
        WHERE {
            ?canvas a <https://github.com/n1kg0r/ds-project-dhdk/classes/Canvas>;
            ns2:identifier ?id;
            ns1:label ?label.
        }
        """

        df_sparql_getAllCanvases = get(endpoint, query_canvases, True)
        return df_sparql_getAllCanvases

    def getAllCollections(self):

        endpoint = Processor.getDbPathOrUrl
        query_collections = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?collection ?id ?label
        WHERE {
           ?collection a <https://github.com/n1kg0r/ds-project-dhdk/classes/Collection>;
           ns2:identifier ?id;
           ns1:label ?label .
        }
        """

        df_sparql_getAllCollections = get(endpoint, query_collections, True)
        return df_sparql_getAllCollections

    def getAllManifest(self):

        endpoint = Processor.getDbPathOrUrl
        query_manifest = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?manifest ?id ?label
        WHERE {
           ?manifest a <https://github.com/n1kg0r/ds-project-dhdk/classes/Manifest>;
           ns2:identifier ?id;
           ns1:label ?label .
        }
        """

        df_sparql_getAllManifest = get(endpoint, query_manifest, True)
        return df_sparql_getAllManifest

    def getCanvasesInCollection(self, collectionId: str):

        endpoint = Processor.setDbPathOrUrl
        query_canInCol = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?canvas ?id ?label 
        WHERE {
            ?collection a <https://github.com/n1kg0r/ds-project-dhdk/classes/Collection> ;
            ns2:identifier "%s" ;
            ns3:items ?manifest .
            ?manifest a <https://github.com/n1kg0r/ds-project-dhdk/classes/Manifest> ;
            ns3:items ?canvas .
            ?canvas a <https://github.com/n1kg0r/ds-project-dhdk/classes/Canvas> ;
            ns2:identifier ?id ;
            ns1:label ?label .
        }
        """ % collectionId

        df_sparql_getCanvasesInCollection = get(endpoint, query_canInCol, True)
        return df_sparql_getCanvasesInCollection

    def getCanvasesInManifest(self, manifestId: str):

        endpoint = Processor.getDbPathOrUrl
        query_canInMan = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?canvas ?id ?label
        WHERE {
            ?manifest a <https://github.com/n1kg0r/ds-project-dhdk/classes/Manifest> ;
            ns2:identifier "%s" ;
            ns3:items ?canvas .
            ?canvas a <https://github.com/n1kg0r/ds-project-dhdk/classes/Canvas> ;
            ns2:identifier ?id ;
            ns1:label ?label .
        }
        """ % manifestId

        df_sparql_getCanvasesInManifest = get(endpoint, query_canInMan, True)
        return df_sparql_getCanvasesInManifest


    def getManifestInCollection(self, collectionId: str):

        endpoint = Processor.getDbPathOrUrl
        query_manInCol = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?manifest ?id ?label
        WHERE {
            ?collection a <https://github.com/n1kg0r/ds-project-dhdk/classes/Collection> ;
            ns2:identifier "%s" ;
            ns3:items ?manifest .
            ?manifest a <https://github.com/n1kg0r/ds-project-dhdk/classes/Manifest> ;
            ns2:identifier ?id ;
            ns1:label ?label .
        }
        """ % collectionId

        df_sparql_getManifestInCollection = get(endpoint, query_manInCol, True)
        return df_sparql_getManifestInCollection
    

    def getEntitiesWithLabel(self, label: str): 
            

        endpoint = "http://192.168.1.55:9999/blazegraph/sparql"
        query_entityLabel = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?entity ?type ?id
        WHERE {
            ?entity ns1:label "%s" ;
            a ?type ;
            ns2:identifier ?id .
        }
        """ % remove_special_chars(label)

        df_sparql_getEntitiesWithLabel = get(endpoint, query_entityLabel, True)
        return df_sparql_getEntitiesWithLabel




# create an instance of the TriplestoreQueryProcessor class
query_processor = TriplestoreQueryProcessor()

# call the getAllCanvases method to retrieve the canvases from the triplestore
entity_df = query_processor.getEntitiesWithLabel('Raimondi, Giuseppe. Quaderno manoscritto, "Caserma Scalo : 1930-1968"')
entity_dt = query_processor.getEntitiesWithLabel("Raimondi, Giuseppe. Quaderno manoscritto, \"Caserma Scalo : 1930-1968\"")
# print the dataframe containing the canvases
print(entity_df)
print(entity_dt)
