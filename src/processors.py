from sqlite3 import connect
from pandas import read_sql, DataFrame
from utils import RDF_DB_URL, SQL_DB_URL
from rdflib import Graph, Literal, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get 




class Processor():
    def __init__(self):
        self.dbPathOrUrl = ''
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    def setDbPathOrUrl(self, dbPathOrUrl:str):
        self.dbPathOrUrl = dbPathOrUrl
        # TODO: check the validity of the url

class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        entityId_stripped = entityId.strip("'")
        db_url = self.getDbPathOrUrl() if len(self.getDbPathOrUrl()) else SQL_DB_URL
        df = DataFrame()
        if db_url == SQL_DB_URL:
            with connect(db_url) as con:
                query = \
                "SELECT *" +\
                " FROM Entity" +\
                " LEFT JOIN Annotation" +\
                " ON Entity.id = Annotation.target" +\
                " WHERE 1=1" +\
                f" AND Entity.id='{entityId_stripped}'"
                df = read_sql(query, con)

        elif db_url == RDF_DB_URL:
            endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
            query = """
                PREFIX ns1: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
                PREFIX ns2: <http://purl.org/dc/elements/1.1/> 
                PREFIX ns3: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

                SELECT ?entity
                WHERE {
                    ?entity ns2:identifier "%s" .
                }
                """ % entityId 

            df = get(endpoint, query, True)
            print(df['entity'].head().to_list())
            return df
        return df


# Uncomment for a test of query processor    

# qp = QueryProcessor()

# qp.setDbPathOrUrl(RDF_DB_URL)
# print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))

# qp.setDbPathOrUrl(SQL_DB_URL)
# print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))
# check library sparqldataframe



class TriplestoreQueryProcessor(QueryProcessor):

    def __init__(self):
        pass

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

        df_sparql = get(endpoint, query_canvases, True)
        return df_sparql

    def getAllCollections():

        endpoint = Processor.setDbPathOrUrl
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

        df_sparql = get(endpoint, query_collections, True)
        return df_sparql

    def getAllManifest():

        endpoint = Processor.setDbPathOrUrl
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

        df_sparql = get(endpoint, query_manifest, True)
        return df_sparql

    def getCanvasesInCollection(collectionId: str):

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

        df_sparql = get(endpoint, query_canInCol, True)
        return df_sparql

    def getCanvasesInManifest(manifestId: str):

        endpoint = Processor.setDbPathOrUrl
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

        df_sparql = get(endpoint, query_canInMan, True)
        return df_sparql


    def getManifestInCollection(collectionId: str):

        endpoint = Processor.setDbPathOrUrl
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

        df_sparql = get(endpoint, query_manInCol, True)
        return df_sparql
    

    def getEntitiesWithLabel(label: str): 

        # trova metodo di escape per le virgolette (doppie e singole) e per le parentesi quadre

        # for char in label:
        #     if char == "[":
        #         label.replace('[', '%5B')
        #     elif char == "]":
        #         label.replace(']', '%5D')
            

        endpoint = Processor.setDbPathOrUrl
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
        """ % ('""' + label + '""')

        df_sparql = get(endpoint, query_entityLabel, True)
        return df_sparql





class GenericQueryProcessor():
    def __init__(self):
        self.queryProcessors = []
    def cleanQueryProcessors(self):
        self.queryProcessors = []
    def addQueryProcessor(self, processor: QueryProcessor):
        self.queryProcessors.append(processor)
    def getAllAnnotations(self):
        result = []
        for processor in self.queryProcessors:
            try:
                result.append(processor.getAllAnnotations())
            except Exception as e:
                print(e)
        return result
    
    def getAllCanvases(self):
        result = []
        for processor in self.queryProcessors:
            try:
                result.append(processor.getAllCanvases())
            except Exception as e:
                print(e)
        return result
    def getAllCollections(self):
        for processor in self.queryProcessors:
            try:
                processor.getAllCollections()
            except Exception as e:
                print(e)
    def getAllImages(self):
        for processor in self.queryProcessors:
            try:
                processor.getAllImages()
            except Exception as e:
                print(e)
    def getAllManifests(self):
        for processor in self.queryProcessors:
            try:
                processor.getAllManifests()
            except Exception as e:
                print(e)
    def getAnnotationsToCanvas(self):
        for processor in self.queryProcessors:
            try:
                processor.getAnnotationsToCanvas()
            except Exception as e:
                print(e)
    def getAnnotationsToCollection(self):
        for processor in self.queryProcessors:
            try:
                processor.getAnnotationsToCollection()
            except Exception as e:
                print(e)
    def getAnnotationsToManifest(self):
        for processor in self.queryProcessors:
            try:
                processor.getAnnotationsToManifest()
            except Exception as e:
                print(e)
    def getAnnotationsWithBody(self):
        for processor in self.queryProcessors:
            try:
                processor.getAnnotationsWithBody()
            except Exception as e:
                print(e)
    def getAnnotationsWithBodyAndTarget(self):
        for processor in self.queryProcessors:
            try:
                processor.getAnnotationsWithBodyAndTarget()
            except Exception as e:
                print(e)
    def getAnnotationsWithTarget(self):
        for processor in self.queryProcessors:
            try:
                processor.getAnnotationsWithTarget()
            except Exception as e:
                print(e)
    def getEntityById(self, entityId):
        result = []
        for processor in self.queryProcessors:
            try:
                result.append(processor.getEntityById(entityId))
            except Exception as e:
                print(e)
        return result









grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
qp = QueryProcessor()

qp.setDbPathOrUrl(RDF_DB_URL)

p = Processor()
tqp = TriplestoreQueryProcessor()
tqp.setDbPathOrUrl("http://127.0.0.1:9999/blazegraph/sparql")
print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))

generic = GenericQueryProcessor()
generic.addQueryProcessor(qp)
generic.addQueryProcessor(p)
generic.addQueryProcessor(tqp)
#print(generic.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))
#print(generic.getAllCanvases())

# col_dp = CollectionProcessor()
# col_dp.setDbPathOrUrl(grp_endpoint)
# col_dp.uploadData("data/collection-1.json")
# col_dp.uploadData("data/collection-2.json")

# # In the next passage, create the query processors for both
# # the databases, using the related classes
# rel_qp = RelationalQueryProcessor()
# rel_qp.setDbPathOrUrl(rel_path)

# grp_qp = TriplestoreQueryProcessor()
# grp_qp.setDbPathOrUrl(grp_endpoint)

# # Finally, create a generic query processor for asking
# # about data
# generic = GenericQueryProcessor()
# generic.addQueryProcessor(rel_qp)
# generic.addQueryProcessor(grp_qp)

# result_q1 = generic.getAllManifests()
# result_q3 = generic.getAnnotationsToCanvas("https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1")