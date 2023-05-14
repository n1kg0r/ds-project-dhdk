from processor import *
from QueryProcessor import QueryProcessor
from pandas import DataFrame
from sparql_dataframe import get 
from clean_str import remove_special_chars

class TriplestoreQueryProcessor(Processor):

    def __init__(self):
        super().__init__()

    def getAllCanvases(self):

        endpoint = self.getDbPathOrUrl()
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

        endpoint = self.getDbPathOrUrl()
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

        endpoint = self.getDbPathOrUrl()
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

        endpoint = self.setDbPathOrUrl()
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

        endpoint = self.getDbPathOrUrl()
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


    def getManifestsInCollection(self, collectionId: str):

        endpoint = self.getDbPathOrUrl()
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
            

        endpoint = self.getDbPathOrUrl()
        query_entityLabel = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?entity ?type ?label ?id
        WHERE {
            ?entity ns1:label "%s" ;
            a ?type ;
            ns1:label ?label ;
            ns2:identifier ?id .
        }
        """ % remove_special_chars(label)

        df_sparql_getEntitiesWithLabel = get(endpoint, query_entityLabel, True)
        return df_sparql_getEntitiesWithLabel
    

    def getEntitiesWithCanvas(self, canvasId: str): 
            
        endpoint = self.getDbPathOrUrl()
        query_entityCanvas = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?id ?label ?type
        WHERE {
            ?entity ns2:identifier "%s" ;
            ns2:identifier ?id ;
            ns1:label ?label ;
            a ?type .
        }
        """ % remove_special_chars(canvasId)

        df_sparql_getEntitiesWithCanvas = get(endpoint, query_entityCanvas, True)
        return df_sparql_getEntitiesWithCanvas
    
    def getEntitiesWithId(self, id: str): 
            
        endpoint = self.getDbPathOrUrl()
        query_entityId = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?id ?label ?type
        WHERE {
            ?entity ns2:identifier "%s" ;
            ns2:identifier ?id ;
            ns1:label ?label ;
            a ?type .
        }
        """ % remove_special_chars(id)

        df_sparql_getEntitiesWithId = get(endpoint, query_entityId, True)
        return df_sparql_getEntitiesWithId
    

    def getAllEntities(self): 
            
        endpoint = self.getDbPathOrUrl()
        query_AllEntities = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT *
        WHERE {
            ?entity ns2:identifier ?id .
        }
        """ 

        df_sparql_getAllEntities = get(endpoint, query_AllEntities, True)
        return df_sparql_getAllEntities




# create an instance of the TriplestoreQueryProcessor class
# Trp_query_processor = TriplestoreQueryProcessor()

# call the getAllCanvases method to retrieve the canvases from the triplestore
# entity_df = Trp_query_processor.getEntitiesWithLabel('Raimondi, Giuseppe. Quaderno manoscritto, "Caserma Scalo : 1930-1968"')
# entity_dt = Trp_query_processor.getEntitiesWithLabel("Raimondi, Giuseppe. Quaderno manoscritto, \"Caserma Scalo : 1930-1968\"")
# print the dataframe containing the canvases
# print(entity_df)
# print(entity_dt)
