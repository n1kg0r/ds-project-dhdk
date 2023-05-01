from try_processor import Processor
from Collection_processor import CollectionProcessor
from pandas import DataFrame
from sparql_dataframe import get 

class TriplestoreQueryProcessor(QueryProcessor):

    def __init__(self):
        pass

    def getAllCanvases():

        endpoint = Processor.setdbPathOrUrl
        query_canvases = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/classes/>

        SELECT ?id ?label
        WHERE {
           ?s rdf:type ns3:Canvas ;
           ns1:id ?id ;
           ns1:label ?label .
        }
        """

        df_sparql = get(endpoint, query_canvases, True)
        return df_sparql

    def getAllCollections():

        endpoint = Processor.setdbPathOrUrl
        query_collections = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/classes/>

        SELECT ?id ?label
        WHERE {
           ?s rdf:type ns3:Collection ;
           ns1:id ?id ;
           ns1:label ?label .
        }
        """

        df_sparql = get(endpoint, query_collections, True)
        return df_sparql

    def getAllManifest():

        endpoint = Processor.setdbPathOrUrl
        query_manifest = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/classes/>

        SELECT ?id ?label
        WHERE {
           ?s rdf:type ns3:Manifest ;
           ns1:id ?id ;
           ns1:label ?label .
        }
        """

        df_sparql = get(endpoint, query_manifest, True)
        return df_sparql

    def getCanvasesInCollection(collectionId: str):

        endpoint = Processor.setdbPathOrUrl
        query_canInCol = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/classes/>

        SELECT DISTINCT ?canvas_id ?canvas_label
        WHERE {
            ?collection rdf:type ns3:Collection ;
            ns1:id "%s" ;
            ns2:items ?manifest .
            ?manifest rdf:type ns3:Manifest ;
            ns2:items ?canvas .
            ?canvas rdf:type ns3:Canvas ;
            ns1:id ?canvas_id ;
            ns1:label ?canvas_label .
        }
        """ % collectionId

        df_sparql = get(endpoint, query_canInCol, True)
        return df_sparql

    def getCanvasesInManifest(manifestId: str):

        endpoint = Processor.setdbPathOrUrl
        query_canInMan = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/classes/>

        SELECT DISTINCT ?canvas_id ?canvas_label
        WHERE {
            ?manifest rdf:type ns3:Manifest ;
            ns1:id "%s" ;
            ns2:items ?canvas .
            ?canvas rdf:type ns3:Canvas ;
            ns1:id ?canvas_id ;
            ns1:label ?canvas_label .
        }
        """ % manifestId

        df_sparql = get(endpoint, query_canInMan, True)
        return df_sparql

    def getEntitiesWithLabel(label: str): #difficile, devi metterti con calma

        endpoint = Processor.setdbPathOrUrl
        # query = """
        # PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        # PREFIX ns2: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 
        # PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/classes/>

        # SELECT DISTINCT ?canvas_id ?canvas_label
        # WHERE {
        #     ?manifest rdf:type ns3:Manifest ;
        #     ?manifest ns1:id "%s" ;
        #     ?manifest ns2:items ?canvas .
        #     ?canvas rdf:type ns3:Canvas ;
        #     ?canvas ns1:id ?canvas_id ;
        #     ?canvas ns1:label ?canvas_label .
        # }
        # """ % manifestId

        # df_sparql = get(endpoint, query, True)
        # return df_sparql

    def getManifestInCollection(collectionId: str):

        endpoint = Processor.setdbPathOrUrl
        query_manInCol = """
        PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX ns2: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 
        PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/classes/>

        SELECT DISTINCT ?manifest_id ?manifest_label
        WHERE {
            ?collection rdf:type ns3:Collection ;
            ns1:id "%s" ;
            ns2:items ?manifest .
            ?manifest rdf:type ns3:Manifest ;
            ns1:id ?manifest_id ;
            ns1:label ?manifest_label .
        }
        """ % collectionId

        df_sparql = get(endpoint, query_manInCol, True)
        return df_sparql
    

