from sqlite3 import connect
from pandas import read_sql, DataFrame
from utils import RDF_DB_URL, SQL_DB_URL
from rdflib import Graph, Literal, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore


class Processor():
    def __init__(self):
        self.dbPathOrUrl = ''
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    def setDbPathOrUrl(self, dbPathOrUrl:str):
        self.dbPathOrUrl = dbPathOrUrl

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
            df = DataFrame(columns=['id', 'type', 'label'])
            store = SPARQLUpdateStore()
            endpoint = 'http://172.20.10.3:9999/blazegraph/sparql'
            store.open((endpoint, endpoint))
            for triple in store.triples((URIRef(entityId), None, None)):
                list_row = [triple[0][0], triple[0][1], triple[0][2]]
                df.loc[len(df)] = list_row
            store.close()
        return df


# Uncomment for a test of query processor    

# qp = QueryProcessor()

# qp.setDbPathOrUrl(RDF_DB_URL)
# print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))

# qp.setDbPathOrUrl(SQL_DB_URL)
# print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))

class GenericQueryProcessor():
    def __init__(self):
        self.queryProcessors = []
    def cleanQueryProcessors(self):
        self.queryProcessors = []
    def addQueryProcessor(self, processor: QueryProcessor):
        self.queryProcessors.append(processor)
    def getAllAnnotations():
        pass
    def getAllCanvas():
        pass
    def getAllCollections():
        pass
    def getAllImages():
        pass
    def getAllManifests():
        pass
    def getAnnotationsToCanvas():
        pass
    def getAnnotationsToCollection():
        pass
    def getAnnotationsToManifest():
        pass
    def getAnnotationsWithBody():
        pass
    def getAnnotationsWithBodyAndTarget():
        pass
    def getAnnotationsWithTarget():
        pass