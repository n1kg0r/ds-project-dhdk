from sqlite3 import connect
from pandas import read_sql, DataFrame, concat, read_csv, Series
from utils.paths import RDF_DB_URL, SQL_DB_URL
from rdflib import Graph, Literal, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get 
from utils.clean_str import remove_special_chars
from json import load
from utils.create_graph import create_Graph


#NOTE: BLOCK DATA MODEL

class IdentifiableEntity():
    def __init__(self, id:str):
        self.id = id
    def getId(self):
        return self.id


class Image(IdentifiableEntity):
    pass


class Annotation(IdentifiableEntity):
    def __init__(self, id, motivation:str, target:IdentifiableEntity, body:Image):
        self.motivation = motivation
        self.target = target
        self.body = body
        super().__init__(id)
    def getBody(self):
        return self.body
    def getMotivation(self):
        return self.motivation
    def getTarget(self):
        return self.target
    

class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, id, label, title, creators):
        self.label = label 
        self.title = title
        self.creators = list()
        if creators:
            for creator in creators:
                self.creators.append(creator)

        super().__init__(id)

    def getLabel(self):
        return self.label
    
    def getTitle(self):
        if self.title:
            return self.title
        else:
            return None
    
    def getCreators(self):
        return self.creators
    

class Canvas(EntityWithMetadata):
    def __init__(self, id:str, label:str, title:str, creators:list[str]):
        super().__init__(id, label, title, creators)

class Manifest(EntityWithMetadata):
    def __init__(self, id:str, label:str, title:str, creators:list[str], items:list[Canvas]):
        super().__init__(id, label, title, creators)
        self.items = items
    def getItems(self):
        return self.items

class Collection(EntityWithMetadata):
    def __init__(self, id:str, label:str, title:str, creators:list[str], items:list[Manifest]):
        super().__init__(id, label, title, creators)
        self.items = items
    def getItems(self):
        return self.items




# NOTE: BLOCK PROCESSORS

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
            return df
        return df


class TriplestoreQueryProcessor(QueryProcessor):

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

        df_sparql = get(endpoint, query_canvases, True)
        return df_sparql


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

        df_sparql = get(endpoint, query_collections, True)
        return df_sparql


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

        df_sparql = get(endpoint, query_manifest, True)
        return df_sparql


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

        df_sparql = get(endpoint, query_canInCol, True)
        return df_sparql


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

        df_sparql = get(endpoint, query_canInMan, True)
        return df_sparql


    def getManifestInCollection(self, collectionId: str):

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

        df_sparql = get(endpoint, query_manInCol, True)
        return df_sparql


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



class RelationalQueryProcessor(QueryProcessor):          
    def __init__(self):
        pass
    def getAllAnnotations(self):
        with connect(self.getDbPathOrUrl()) as con:
          q1="SELECT * FROM Annotation;" 
          q1_table = read_sql(q1, con)
          return q1_table
    def getAllImages(self):
        with connect(self.getDbPathOrUrl()) as con:
          q2="SELECT * FROM Image;" 
          q2_table = read_sql(q2, con)
          return q2_table
    def getAnnotationsWithBody(self, bodyId:str):
        with connect(self.getDbPathOrUrl())as con:
            q3 = "SELECT * FROM Annotation WHERE body = "+ bodyId
            q3_table = read_sql(q3, con)
            return q3_table
    def getAnnotationsWithBodyAndTarget(self, bodyId:str,targetId:str):
        with connect(self.getDbPathOrUrl())as con:
            q4 = "SELECT* FROM Annotation WHERE body = " + bodyId + " AND target = '" + targetId +"'"
            q4_table = read_sql (q4, con)
            return q4_table
    def getAnnotationsWithTarget(self, targetId:str):
        with connect(self.getDbPathOrUrl())as con:
            q5 = "SELECT * FROM Annotation WHERE target = '" + targetId +"'"
            q5_table = read_sql(q5, con)
            return q5_table
    def getEntitiesWithCreator(self, creatorName):
        with connect(self.getDbPathOrUrl())as con:
             q6 = "SELECT * FROM Entity LEFT JOIN Creators ON Entity.entityId == Creators.entityId WHERE creator = '" + creatorName +"'"
             result = read_sql(q6, con)
             return result.drop_duplicates(subset=["entityId"])
    def getEntitiesWithLabel(self):
        pass
    def getEntitiesWithTitle(self,title):
        with connect(self.getDbPathOrUrl())as con:
             q6 = "SELECT * FROM Entity WHERE title = '" + title +"'"
             return read_sql(q6, con)
        

class AnnotationProcessor(Processor):
    def __init__(self):
        pass
    def uploadData(self, path:str): 
  
        annotations = read_csv(path, 
                                keep_default_na=False,
                                dtype={
                                    "id": "string",
                                    "body": "string",
                                    "target": "string",
                                    "motivation": "string"
                                })
        annotations_internalId = []
        for idx, row in annotations.iterrows():
            annotations_internalId.append("annotation-" +str(idx))
        annotations.insert(0, "annotationId", Series(annotations_internalId, dtype = "string"))
        
        image = annotations[["body"]]
        image = image.rename(columns={"body": "id"})
        image_internalId = []
        for idx, row in image.iterrows():
            image_internalId.append("image-" +str(idx))
        image.insert(0, "imageId", Series(image_internalId, dtype = "string"))

        with connect(self.getDbPathOrUrl()) as con:
            annotations.to_sql("Annotation", con, if_exists="replace", index=False)
            image.to_sql("Image", con, if_exists="replace", index=False)

class MetadataProcessor(Processor):
    def __init__(self):
        pass
    def uploadData(self, path:str): 
        entityWithMetadata= read_csv(path, 
                                keep_default_na=False,
                                dtype={
                                    "id": "string",
                                    "title": "string",
                                    "creator": "string"
                                })
        
        metadata_internalId = []
        for idx, row in entityWithMetadata.iterrows():
            metadata_internalId.append("entity-" +str(idx))
        entityWithMetadata.insert(0, "entityId", Series(metadata_internalId, dtype = "string"))
        creator = entityWithMetadata[["entityId", "creator"]]
        #I recreate entityMetadata since, as I will create a proxy table, I will have no need of
        #coloumn creator
        entityWithMetadata = entityWithMetadata[["entityId", "id", "title"]]
        

        for idx, row in creator.iterrows():
                    for item_idx, item in row.iteritems():
                        if "entity-" in item:
                            entity_id = item
                        if ";" in item:
                            list_of_creators =  item.split(";")
                            creator = creator.drop(idx)
                            new_serie = []
                            for i in range (len(list_of_creators)):
                                new_serie.append(entity_id)
                            new_data = DataFrame({"entityId": new_serie, "creator": list_of_creators})
                            creator = concat([creator.loc[:idx-1], new_data, creator.loc[idx:]], ignore_index=True)

        with connect(self.getDbPathOrUrl()) as con:
            entityWithMetadata.to_sql("Entity", con, if_exists="replace", index = False)
            creator.to_sql("Creators", con, if_exists="replace", index = False)



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
        


#NOTE: BLOCK GENERIC PROCESSOR

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



# NOTE: TEST BLOCK, TO BE DELETED
# TODO: DELETE COMMENTS
#  
# Uncomment for a test of query processor    
# qp = QueryProcessor()
# qp.setDbPathOrUrl(RDF_DB_URL)
# print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))
# qp.setDbPathOrUrl(SQL_DB_URL)
# print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))
# check library sparqldataframe




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




# try1 = CollectionProcessor()
# try1.dbPathOrUrl = "http://192.168.1.55:9999/blazegraph/sparql"
# try1.getDbPathOrUrl()

# print(try1.uploadData("collection-1.json"))


# try2 = CollectionProcessor()
# try2.dbPathOrUrl = "http://192.168.1.55:9999/blazegraph/sparql"
# try2.getDbPathOrUrl()

# print(try2.uploadData("collection-2.json"))


# # create an instance of the TriplestoreQueryProcessor class
# query_processor = TriplestoreQueryProcessor()

# # call the getAllCanvases method to retrieve the canvases from the triplestore
# entity_df = query_processor.getEntitiesWithLabel('Raimondi, Giuseppe. Quaderno manoscritto, "Caserma Scalo : 1930-1968"')
# entity_dt = query_processor.getEntitiesWithLabel("Raimondi, Giuseppe. Quaderno manoscritto, \"Caserma Scalo : 1930-1968\"")
# # print the dataframe containing the canvases
# print(entity_df)
# print(entity_dt)




#upload_metadata= MetadataProcessor()
#upload_metadata.setDbPathOrUrl("database.db")
#upload_metadata.uploadData("metadata.csv")
#upload_annotation= AnnotationProcessor()
#upload_annotation.setDbPathOrUrl("database.db")
#upload_annotation.uploadData("annotations.csv")
