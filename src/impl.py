from sqlite3 import connect
from pandas import read_sql, read_sql_table, DataFrame, concat, read_csv, Series, merge, options
from numpy import nan
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib import Graph, Namespace
from sparql_dataframe import get 
from utils.clean_str import remove_special_chars
from json import load
from utils.create_graph import create_Graph
from urllib.parse import urlparse
options.mode.chained_assignment = None
#NOTE: BLOCK DATA MODEL

class IdentifiableEntity():
    def __init__(self, id:str):
        if not isinstance(id, str):
            raise ValueError('IdentifiableEntity.id must be a string')
        self.id = id
    def getId(self):
        return self.id

class Image(IdentifiableEntity):
    pass

class Annotation(IdentifiableEntity):
    def __init__(self, id, motivation:str, target:IdentifiableEntity, body:Image):
        if not isinstance(motivation, str):
            raise ValueError('Annotation.motivation must be a string')
        if not isinstance(body, Image):
            raise ValueError('Annotation.body must be an Image')
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
    def __init__(self, id: str, label: str, title:str|None=None, creators:str|list[str]|None=None):
        super().__init__(id)
        if not isinstance(label, str):
            raise Exception('EntityWithMetadata.label must be a string')
        if (not isinstance(title, str)) and title is not None:
            raise Exception('EntityWithMetadata.title must be a string or None')
        if not isinstance(creators, str) and not isinstance(creators, list) and creators is not None:
            raise Exception('EntityWithMetadata.creators must be a list or a string or None')
        
        self.label = label 
        self.title = title
        self.creators = list()
        
        if type(creators) == str:
            self.creators.append(creators)
        elif type(creators) == list:
            self.creators = creators

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
    pass


class Manifest(EntityWithMetadata):
    def __init__(self, id:str, label:str, items:list[Canvas], title:str=None, creators:list[str]=None):
        super().__init__(id, label, title, creators)
        if (not isinstance(items, list)):
            raise Exception('Manifest.items must be a list')
        self.items = items
    def getItems(self):
        return self.items

class Collection(EntityWithMetadata):
    def __init__(self, id:str, label:str, items:list[Manifest], title:str=None, creators:list[str]=None):
        super().__init__(id, label, title, creators)
        if (not isinstance(items, list)):
            raise Exception('Collection.items must be a list')
        self.items = items
    def getItems(self):
        return self.items


# NOTE: BLOCK PROCESSORS


class Processor(object):
    def __init__(self):
        self.dbPathOrUrl = ""
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl 
    def setDbPathOrUrl(self, newpath):
        if len(newpath.split('.')) and newpath.split('.')[-1] == "db":
            self.dbPathOrUrl = newpath
            return True
        elif len(urlparse(newpath).scheme) and len(urlparse(newpath).netloc):
            self.dbPathOrUrl = newpath
            return True
        return False
        




class AnnotationProcessor(Processor):
    def __init__(self):
        pass
    def uploadData(self, path:str):
        try:
            lenth_ann = 0
            try:
                with connect(self.getDbPathOrUrl()) as con:
                    q1="SELECT * FROM Annotation;" 
                    q1_table = read_sql(q1, con)
                    lenth_ann = len(q1_table)
            except:
                pass
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
                annotations_internalId.append("annotation-" +str(idx+lenth_ann))
            annotations.insert(0, "annotationId", Series(annotations_internalId, dtype = "string"))

            image = annotations[["body"]]
            image = image.rename(columns={"body": "id"})
            image_internalId = []
            for idx, row in image.iterrows():
                image_internalId.append("image-" +str(idx+lenth_ann))
            image.insert(0, "imageId", Series(image_internalId, dtype = "string"))

            with connect(self.getDbPathOrUrl()) as con:
                annotations.to_sql("Annotation", con, if_exists="append", index=False)
                image.to_sql("Image", con, if_exists="append", index=False)

            return True


        except Exception as e:
            print(str(e))
            return False

class MetadataProcessor(Processor):
    def __init__(self):
        pass
    def uploadData(self, path:str):
        try:
            lenth_meta = 0
            try:
                with connect(self.getDbPathOrUrl()) as con:
                    q1="SELECT * FROM Entity;" 
                    q1_table = read_sql(q1, con)
                    lenth_meta = len(q1_table)
            except:
                pass
            entityWithMetadata= read_csv(path, 
                                    keep_default_na=False,
                                    dtype={
                                        "id": "string",
                                        "title": "string",
                                        "creator": "string"
                                    })

            metadata_internalId = []
            for idx, row in entityWithMetadata.iterrows():
                metadata_internalId.append("entity-" +str(idx+lenth_meta))
            entityWithMetadata.insert(0, "entityId", Series(metadata_internalId, dtype = "string"))
            creator = entityWithMetadata[["entityId", "creator"]]
            #I recreate entityMetadata since, as I will create a proxy table, I will have no need of
            #coloumn creator
            entityWithMetadata = entityWithMetadata[["entityId", "id", "title"]]


            for idx, row in creator.iterrows():
                if ";" in row["creator"]:
                    list_of_creators =  row["creator"].split("; ")
                    internal_id = row["entityId"]
                    creator = creator.drop(idx)
                    new_serie = []
                    for i in range (len(list_of_creators)):
                        new_serie.append(internal_id)
                    new_data = DataFrame({"entityId": new_serie, "creator": list_of_creators})
                    creator = concat([creator.loc[:idx-1], new_data, creator.loc[idx:]], ignore_index=True)

            with connect(self.getDbPathOrUrl()) as con:
                entityWithMetadata.to_sql("Entity", con, if_exists="append", index = False)
                creator.to_sql("Creators", con, if_exists="append", index = False)
            return True
        except Exception as e:
                print(str(e))
                return False




class CollectionProcessor(Processor):

    def __init__(self):
        super().__init__()

    def uploadData(self, path: str):
        try: 
            base_url = "https://github.com/n1kg0r/ds-project-dhdk/"
            my_graph = Graph()

            # define namespaces 
            nikCl = Namespace("https://github.com/n1kg0r/ds-project-dhdk/classes/")
            nikAttr = Namespace("https://github.com/n1kg0r/ds-project-dhdk/attributes/")
            nikRel = Namespace("https://github.com/n1kg0r/ds-project-dhdk/relations/")
            dc = Namespace("http://purl.org/dc/elements/1.1/")

            my_graph.bind("nikCl", nikCl)
            my_graph.bind("nikAttr", nikAttr)
            my_graph.bind("nikRel", nikRel)
            my_graph.bind("dc", dc)
            
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
            
            with open('Graph_db.ttl', mode='a', encoding='utf-8') as f:
                f.write(my_graph.serialize(format='turtle'))

            return True
        
        except Exception as e:
            print(str(e))
            return False




class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        db_url = self.getDbPathOrUrl()
        df = DataFrame(columns=['id', 'type', 'title', 'body', 'target', 'motivation', 'label'])
        

        if len(db_url.split('.')) and db_url.split('.')[-1] == "db":
            select_entity = f"""
                        SELECT *
                        FROM Entity
                        WHERE 1=1
                        AND id='{entityId}'
                    """
            select_image = f"""
                        SELECT *
                        FROM Image
                        WHERE 1=1
                        AND id='{entityId}'
                    """
            select_annotation = f"""
                        SELECT *
                        FROM Annotation
                        WHERE 1=1
                        AND id='{entityId}'
                    """
            try:
                with connect(db_url) as con:
                    df_entity = read_sql(select_entity, con) 
                    df_image = read_sql(select_image, con) 
                    df_annotation = read_sql(select_annotation, con) 

                    
            except Exception as e:
                print(f"couldn't connect to sql database due to the following error: {e}")
            
            df_image['type'] = 'image'
            df_annotation['type'] = 'annotation'
            df_entity['type'] = 'entity'

            df_to_concat = DataFrame()
            if not df_annotation.empty:
                df_to_concat = df_annotation
            elif not df_image.empty:
                df_to_concat = df_image

            if not df_to_concat.empty:
                df = concat([df, df_to_concat], sort=False, ignore_index=True ).replace([nan], [None])

            # print(df)
            
            
        elif len(urlparse(db_url).scheme) and len(urlparse(db_url).netloc):
            endpoint = db_url
            query = """
                    PREFIX dc: <http://purl.org/dc/elements/1.1/> 
                    PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
                    PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
                    PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/>  

                    SELECT ?entity ?id ?label ?type
                    WHERE {
                        ?entity dc:identifier "%s" ;
                        dc:identifier ?id ;
                        nikAttr:label ?label ;
                        a ?type .
                    }
                    """ % entityId 
            try:
                df_graph = get(endpoint, query, True)[['id', 'label', 'type']].drop_duplicates()
                
                
                for _, row in df_graph.iterrows():
                    row['type'] = row['type'].split('/')[-1].lower()

                df = concat([df, df_graph], sort=False, ignore_index=True ).replace([nan], [None])

                

            except Exception as e:
                print(f"couldn't connect to blazegraph due to the following error: \n{e}")
                print(f"trying to reconnect via local connection at http://127.0.0.1:9999/blazegraph/sparql")
                try:
                    endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
                    df_graph = get(endpoint, query, True)[['id', 'label', 'type']].drop_duplicates()
                    df = concat([df, df_graph], sort=False, ignore_index=True ).replace([nan], [None])
                except Exception as e2:
                    print(f"couldn't connect to blazegraph due to the following error: {e2}")
            
        return df.drop_duplicates()



class RelationalQueryProcessor(QueryProcessor):      
    def __init__(self):
        super().__init__()

    def getAllAnnotations(self):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                q1="SELECT * FROM Annotation;" 
                q1_table = read_sql(q1, con)
                return q1_table
        except Exception as e:
                print(f"couldn't connect to sql database due to the following error: {e}") 
                return DataFrame()            
    def getAllImages(self):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                q2="SELECT * FROM Image;" 
                q2_table = read_sql(q2, con)
                return q2_table 
        except Exception as e:
                print(f"couldn't connect to sql database due to the following error: {e}") 
                return DataFrame()      
    def getAnnotationsWithBody(self, bodyId:str):
        try:
            with connect(self.getDbPathOrUrl())as con:
                q3 = f"SELECT* FROM Annotation WHERE body = '{bodyId}'"
                q3_table = read_sql(q3, con)
                return q3_table
        except Exception as e:
                print(f"couldn't connect to sql database due to the following error: {e}")  
                return DataFrame()        
    def getAnnotationsWithBodyAndTarget(self, bodyId:str,targetId:str):
        try:
            with connect(self.getDbPathOrUrl())as con:
                q4 = f"SELECT* FROM Annotation WHERE body = '{bodyId}' AND target = '{targetId}'"
                q4_table = read_sql(q4, con)
                return q4_table
        except Exception as e:
                print(f"couldn't connect to sql database due to the following error: {e}")    
                return DataFrame()      
    def getAnnotationsWithTarget(self, targetId:str):#I've decided not to catch the empty string since in this case a Dataframe is returned, witch is okay
        try:
            with connect(self.getDbPathOrUrl())as con:
                q5 = f"SELECT* FROM Annotation WHERE target = '{targetId}'"
                q5_table = read_sql(q5, con)
                return q5_table
        except Exception as e:
            print(f"couldn't connect to sql database due to the following error: {e}") 
            return DataFrame() 
    def getEntitiesWithCreator(self, creatorName):
        try:
            with connect(self.getDbPathOrUrl())as con:
                q6 = "SELECT Entity.entityid, Entity.id, Creators.creator, Entity.title FROM Entity LEFT JOIN Creators ON Entity.entityId == Creators.entityId WHERE creator = '" + creatorName +"'"
                result = read_sql(q6, con)
                return result
        except Exception as e:
                print(f"couldn't connect to sql database due to the following error: {e}")
                return DataFrame() 
    def getEntitiesWithTitle(self,title):
        try:
            with connect(self.getDbPathOrUrl())as con:
                q6 = "SELECT Entity.entityid, Entity.id, Creators.creator, Entity.title FROM Entity LEFT JOIN Creators ON Entity.entityId == Creators.entityId WHERE title = '" + title +"'"
                result = read_sql(q6, con)  
                return result
        except Exception as e:
                print(f"couldn't connect to sql database due to the following error: {e}")
                return DataFrame() 
    def getEntities(self):
        try:
            with connect(self.getDbPathOrUrl())as con:
                q7 = "SELECT Entity.entityid, Entity.id, Creators.creator, Entity.title FROM Entity LEFT JOIN Creators ON Entity.entityId == Creators.entityId"
                result = read_sql(q7, con) 
                return result
        except Exception as e:
                print(f"couldn't connect to sql database due to the following error: {e}")
                return DataFrame() 
                

class TriplestoreQueryProcessor(QueryProcessor):

    def __init__(self):
        super().__init__()

    def getAllCanvases(self):

        endpoint = self.getDbPathOrUrl()
        query_canvases = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/> 
        PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
        PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?canvas ?id ?label
        WHERE {
            ?canvas a nikCl:Canvas;
            dc:identifier ?id;
            nikAttr:label ?label.
        }
        """

        df_sparql_getAllCanvases = get(endpoint, query_canvases, True)
        return df_sparql_getAllCanvases

    def getAllCollections(self):

        endpoint = self.getDbPathOrUrl()
        query_collections = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/> 
        PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
        PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?collection ?id ?label
        WHERE {
           ?collection a nikCl:Collection;
           dc:identifier ?id;
           nikAttr:label ?label .
        }
        """

        df_sparql_getAllCollections = get(endpoint, query_collections, True)
        return df_sparql_getAllCollections

    def getAllManifests(self):

        endpoint = self.getDbPathOrUrl()
        query_manifest = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/> 
        PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
        PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/>

        SELECT ?manifest ?id ?label
        WHERE {
           ?manifest a nikCl:Manifest ;
           dc:identifier ?id ;
           nikAttr:label ?label .
        }
        """

        df_sparql_getAllManifest = get(endpoint, query_manifest, True)
        return df_sparql_getAllManifest

    def getCanvasesInCollection(self, collectionId: str):

        endpoint = self.getDbPathOrUrl()
        query_canInCol = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/> 
        PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
        PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/>

        SELECT ?canvas ?id ?label 
        WHERE {
            ?collection a nikCl:Collection ;
            dc:identifier "%s" ;
            nikRel:items ?manifest .
            ?manifest a nikCl:Manifest ;
            nikRel:items ?canvas .
            ?canvas a nikCl:Canvas ;
            dc:identifier ?id ;
            nikAttr:label ?label .
        }
        """ % collectionId

        df_sparql_getCanvasesInCollection = get(endpoint, query_canInCol, True)
        return df_sparql_getCanvasesInCollection

    def getCanvasesInManifest(self, manifestId: str):

        endpoint = self.getDbPathOrUrl()
        query_canInMan = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/> 
        PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
        PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?canvas ?id ?label
        WHERE {
            ?manifest a nikCl:Manifest ;
            dc:identifier "%s" ;
            nikRel:items ?canvas .
            ?canvas a nikCl:Canvas ;
            dc:identifier ?id ;
            nikAttr:label ?label .
        }
        """ % manifestId

        df_sparql_getCanvasesInManifest = get(endpoint, query_canInMan, True)
        return df_sparql_getCanvasesInManifest


    def getManifestsInCollection(self, collectionId: str):

        endpoint = self.getDbPathOrUrl()
        query_manInCol = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/> 
        PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
        PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/>  

        SELECT ?manifest ?id ?label
        WHERE {
            ?collection a nikCl:Collection ;
            dc:identifier "%s" ;
            nikRel:items ?manifest .
            ?manifest a nikCl:Manifest ;
            dc:identifier ?id ;
            nikAttr:label ?label .
        }
        """ % collectionId

        df_sparql_getManifestInCollection = get(endpoint, query_manInCol, True)
        return df_sparql_getManifestInCollection
    

    def getEntitiesWithLabel(self, label: str): 
            

        endpoint = self.getDbPathOrUrl()
        query_entityLabel = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/> 
        PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
        PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/>

        SELECT ?entity ?type ?label ?id
        WHERE {
            ?entity nikAttr:label "%s" ;
            a ?type ;
            nikAttr:label ?label ;
            dc:identifier ?id .
        }
        """ % remove_special_chars(label)

        df_sparql_getEntitiesWithLabel = get(endpoint, query_entityLabel, True)
        return df_sparql_getEntitiesWithLabel
    

    def getEntitiesWithCanvas(self, canvasId: str): 
            
        endpoint = self.getDbPathOrUrl()
        query_entityCanvas = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/> 
        PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
        PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?id ?label ?type
        WHERE {
            ?entity dc:identifier "%s" ;
            dc:identifier ?id ;
            nikAttr:label ?label ;
            a ?type .
        }
        """ % canvasId

        df_sparql_getEntitiesWithCanvas = get(endpoint, query_entityCanvas, True)
        return df_sparql_getEntitiesWithCanvas
    
    def getEntitiesWithId(self, id: str): 
            
        endpoint = self.getDbPathOrUrl()
        query_entityId = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/> 
        PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
        PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?id ?label ?type
        WHERE {
            ?entity dc:identifier "%s" ;
            dc:identifier ?id ;
            nikAttr:label ?label ;
            a ?type .
        }
        """ % id

        df_sparql_getEntitiesWithId = get(endpoint, query_entityId, True)
        return df_sparql_getEntitiesWithId
    

    def getAllEntities(self): 
            
        endpoint = self.getDbPathOrUrl()
        query_AllEntities = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/> 
        PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
        PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
        PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/> 

        SELECT ?entity ?id ?label ?type
        WHERE {
            ?entity dc:identifier ?id ;
                    dc:identifier ?id ;
                    nikAttr:label ?label ;
                    a ?type .
        }
        """ 

        df_sparql_getAllEntities = get(endpoint, query_AllEntities, True)
        return df_sparql_getAllEntities
        
# t_qp = TriplestoreQueryProcessor()
# t_qp.setDbPathOrUrl('./data/test.db')
# print(t_qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/manifest'))











# NOTE: BLOCK GENERIC PROCESSOR

class GenericQueryProcessor():
    def __init__(self):
        self.queryProcessors = []

    def cleanQueryProcessors(self):
        self.queryProcessors = []
        return True
    
    def addQueryProcessor(self, processor: QueryProcessor):
        try:
            self.queryProcessors.append(processor)
            return True 
        except Exception as e:
            print(e)
            return False
        
    def getAllAnnotations(self):
        relation_db = DataFrame()
        result=[]
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getAllAnnotations()
                relation_db = concat([relation_db, relation_to_add], ignore_index=True).drop_duplicates()
        
        if not relation_db.empty:
            relation_db = relation_db[['id', 'motivation', 'target', 'body']].drop_duplicates().fillna('')

            result = [
                Annotation(row['id'],
                            row['motivation'],
                            self.getEntityById(row['target']), 
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
            ]
        return result
    
    
    def getAllCanvas(self):
        graph_db = DataFrame()
        relation_db = DataFrame()
        result = []

        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_to_add = processor.getAllCanvases()
                graph_db = concat([graph_db, graph_to_add], ignore_index= True)
            elif isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getEntities()
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)

        df_joined = merge(graph_db, relation_db, left_on="id", right_on="id", how='left').fillna("").drop_duplicates()
        df_joined['creator'] =  df_joined.groupby(['canvas','id','label', 'entityId', 'title'])['creator'].transform(lambda x: '; '.join(x))
        df_joined = df_joined[['canvas','id','label', 'title', 'creator']].drop_duplicates()

        result = [
                Canvas(row['id'], 
                       row['label'], 
                       row['title'],
                       row['creator'].split('; '))
                       for _, row in df_joined.iterrows()
                ]
            
        return result 
    
  
    


    def getAllCollections(self):
        graph_db = DataFrame()
        relation_db = DataFrame()
        
        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_to_add = processor.getAllCollections()
                graph_db = concat([graph_db,graph_to_add], ignore_index= True).drop_duplicates()
            elif isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getEntities()
                relation_db = concat([relation_db, relation_to_add], ignore_index=True).drop_duplicates()
        
        df_joined = merge(graph_db, relation_db, how='left',
                           left_on="id", right_on="id").fillna('')
        
        collections_list = []
        
        df_joined['creator'] =  df_joined.groupby(['collection','id','label', 'entityId', 'title'])['creator'].transform(lambda x: '; '.join(x))
        df_joined = df_joined.drop_duplicates()

        for _, row in df_joined.iterrows():

            graph_db_manifest = DataFrame()
            for processor in self.queryProcessors:
                    if isinstance(processor, TriplestoreQueryProcessor):
                        graph_to_add_manifest = processor.getManifestsInCollection(row['id'])
                        graph_db_manifest = concat([graph_db_manifest, graph_to_add_manifest], ignore_index= True).drop_duplicates()

            df_joined_manifest = merge(graph_db_manifest, 
                                       relation_db,
                                       how='left',
                                       left_on='id',
                                       right_on='id').fillna('')
            

            manifests_list = []
            #print(df_joined_manifest.columns.to_list())
            df_joined_manifest['creator'] =  df_joined_manifest.groupby(['manifest','id','label', 'entityId', 'title'])['creator'].transform(lambda x: '; '.join(x))
            df_joined_manifest = df_joined_manifest.drop_duplicates()
            for _, row1 in df_joined_manifest.iterrows():
                graph_db_canvas = DataFrame()

                for processor in self.queryProcessors:
                    if isinstance(processor, TriplestoreQueryProcessor):
                        graph_to_add_canvas = processor.getCanvasesInManifest(row1['id'])
                        graph_db_canvas = concat([graph_db_canvas,graph_to_add_canvas], ignore_index= True).drop_duplicates()

                df_joined_canvas = merge(graph_db_canvas, 
                                       relation_db,
                                       how='left',
                                       left_on='id',
                                       right_on='id'
                                       ).fillna('')

                df_joined_canvas['creator'] =  df_joined_canvas.groupby(['canvas','id','label', 'entityId', 'title'])['creator'].transform(lambda x: '; '.join(x))
                df_joined_canvas = df_joined_canvas.drop_duplicates()
                canvases_list = [
                    Canvas(row2['id'], 
                       row2['label'], 
                       row2['title'],
                       row2['creator'].split('; ')) 
                       for _, row2 in df_joined_canvas.iterrows()
                ]
                
                manifests_list.append(
                    Manifest(row1["id"],
                        row1["label"], 
                        canvases_list,
                        row1['title'], 
                        row1['creator'].split('; '),
                        ) 
                )

            
            collections_list.append(
                Collection(row["id"],
                        row["label"], 
                        manifests_list,
                        row['title'], 
                        row['creator'].split('; '),
                        ) 
            )
        return collections_list 
    

    def getAllManifests(self):
        graph_db = DataFrame()
        relation_db = DataFrame()
        
        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_to_add = processor.getAllManifests()
                graph_db = concat([graph_db,graph_to_add], ignore_index= True).drop_duplicates()
            elif isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getEntities()
                relation_db = concat([relation_db, relation_to_add], ignore_index=True).drop_duplicates()
            
        df_joined = merge(graph_db, relation_db, how='left',
                           left_on="id", right_on="id").fillna('')
        df_joined['creator'] =  df_joined.groupby(['manifest','id','label', 'entityId', 'title'])['creator'].transform(lambda x: '; '.join(x))
        df_joined = df_joined.drop_duplicates()

        manifests_list = []
        for _, row in df_joined.iterrows():
 
            graph_db_canvas = DataFrame()

            for processor in self.queryProcessors:
                if isinstance(processor, TriplestoreQueryProcessor):
                    graph_to_add_canvas = processor.getCanvasesInManifest(row['id'])
                    graph_db_canvas = concat([graph_db_canvas,graph_to_add_canvas], ignore_index= True).drop_duplicates()

            
            df_joined_canvas = merge(graph_db_canvas, 
                                       relation_db,
                                       how='left',
                                       left_on='id',
                                       right_on='id'
                                       ).fillna('')
            
            
            df_joined_canvas['creator'] =  df_joined_canvas.groupby(['canvas','id','label', 'entityId', 'title'])['creator'].transform(lambda x: '; '.join(x))
            df_joined_canvas = df_joined_canvas.drop_duplicates()

            canvases_list = [
                    Canvas(row1['id'], 
                       row1['label'], 
                       row1['title'],
                       row1['creator'].split('; ')) 
                       for _, row1 in df_joined_canvas.iterrows()
                ]
                
            manifests_list.append(
                    Manifest(row["id"],
                        row["label"], 
                        canvases_list,
                        row['title'], 
                        row['creator'].split('; '),
                        ) 
                )

        return manifests_list
    

    def getAllImages(self):
        result = []
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                try:
                    df = processor.getAllImages()
                    df = df[['id']].drop_duplicates().fillna('') 
                    images_list = [
                        Image(row['id'])
                                 for _, row in df.iterrows()
                    ] 
                    
                    result += images_list
                except Exception as e:
                    print(e) 
        
        return result
    

    def getAnnotationsToCanvas(self, canvasId):
        relation_db = DataFrame()
        result=[]
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getAnnotationsWithTarget(canvasId)
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
        if not relation_db.empty:
            relation_db = relation_db[['id', 'motivation', 'target', 'body']].drop_duplicates().fillna('')
            result = [
                Annotation(row['id'],
                            row['motivation'],
                            self.getEntityById(row['target']), 
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
            ]
        return result
    

    def getAnnotationsWithBody(self, bodyId:str):
        bodyId = bodyId.strip('"')
        relation_db = DataFrame()
        result=[]
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getAnnotationsWithBody(bodyId)
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
        
        if not relation_db.empty:
            relation_db = relation_db[['id', 'motivation', 'target', 'body']].drop_duplicates().fillna('')

            result = [
                Annotation(row['id'],
                            row['motivation'],
                            self.getEntityById(row['target']), 
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
            ]
        return result


    def getAnnotationsWithBodyAndTarget(self, bodyId:str, targetId:str):
        bodyId = bodyId.strip('"')
        relation_db = DataFrame()
        result=[]
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getAnnotationsWithBodyAndTarget(bodyId, targetId)
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
        
        if not relation_db.empty:
            relation_db = relation_db[['id', 'motivation', 'target', 'body']].drop_duplicates().fillna('')

            result = [
                Annotation(row['id'],
                            row['motivation'],
                            self.getEntityById(row['target']), 
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
            ]
        return result


    def getAnnotationsWithTarget(self, targetId:str):
        relation_db = DataFrame()
        result=[]
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getAnnotationsWithTarget(targetId)
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
        
        if not relation_db.empty:
            relation_db = relation_db[['id', 'motivation', 'target', 'body']].drop_duplicates().fillna('')
            result = [
                Annotation(row['id'],
                            row['motivation'],
                            self.getEntityById(row['target']), 
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
            ]
        return result
    
    
    def getAnnotationsToCollection(self, collectionId):
        relation_db = DataFrame()
        result = []        
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getAnnotationsWithTarget(collectionId)
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)

        if not relation_db.empty:
            relation_db = relation_db[['id', 'motivation', 'target', 'body']].drop_duplicates().fillna('')
            
            result = [
                Annotation(row['id'],
                            row['motivation'],
                            self.getEntityById(row['target']), 
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
                        ] 
        return result


    def getAnnotationsToManifest(self, manifestId):
        relation_db = DataFrame()
        result = []        
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getAnnotationsWithTarget(manifestId)
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)

        if not relation_db.empty:
            relation_db = relation_db[['id', 'motivation', 'target', 'body']].drop_duplicates().fillna('')
            
            result = [
                Annotation(row['id'],
                            row['motivation'],
                            self.getEntityById(row['target']),  
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
                        ] 
        return result

    #Nicole 
    def getCanvasesInCollection(self, collectionId):
        graph_db = DataFrame()
        relation_db = DataFrame()
        canvas_list = []
        for item in self.queryProcessors:
            if isinstance(item, TriplestoreQueryProcessor):
                graph_to_add = item.getCanvasesInCollection(collectionId)#restituisce canva, id, collection
                graph_db = concat([graph_db,graph_to_add], ignore_index= True)
            elif isinstance(item, RelationalQueryProcessor):
                relation_to_add = item.getEntities() #restituisce entityId, id, creator,title
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
        if not graph_db.empty:
            df_joined = merge(graph_db, relation_db, left_on="id", right_on="id")
            # itera le righe del dataframe e crea gli oggetti Canvas
            for index, row in df_joined.iterrows():
                canvas = Canvas(row['id'], row['label'], row['title'], row['creator'])
                canvas_list.append(canvas)
        return canvas_list 
    
    def getCanvasesInManifest(self, manifestId):
        graph_db = DataFrame()
        relation_db = DataFrame()
        canvas_list = []
        for item in self.queryProcessors:
            if isinstance(item, TriplestoreQueryProcessor):
                graph_to_add = item.getCanvasesInManifest(manifestId)
                graph_db = concat([graph_db,graph_to_add], ignore_index= True)
            elif isinstance(item, RelationalQueryProcessor):
                relation_to_add = item.getEntities() #restituisce entityId, id, title, creator
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
            else:
                break
        if not graph_db.empty:
            df_joined = merge(graph_db, relation_db, left_on="id", right_on="id")
            for _, row in df_joined.iterrows():
                    canvas = Canvas(row['id'], row['label'], row['title'], row['creator'])
                    canvas_list.append(canvas)
        return canvas_list 
    

    

    def getEntityById(self, id):
        pd = DataFrame()
        relation_db = DataFrame() 

        for processor in self.queryProcessors:
            
            if isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getEntities()
                relation_db = concat([relation_db, relation_to_add], ignore_index=True).drop_duplicates()
            

        for processor in self.queryProcessors:
            if isinstance(processor, QueryProcessor):
                pd_to_add = processor.getEntityById(id)
                pd = concat([pd, pd_to_add], ignore_index=True).replace([nan], [None])

        pd = pd.drop_duplicates()

        if not pd.empty:
            entity_data = pd.iloc[[0]]
            # print('here here here')
            # print(entity_data.iloc[[0]]['type'] == 'annotation')
            # print()
            if entity_data['type'].iloc[0] == 'annotation':
                return Annotation(entity_data['id'].iloc[0], 
                                  entity_data['motivation'].iloc[0],
                                  self.getEntityById(entity_data['target'].iloc[0]),
                                  self.getEntityById(entity_data['body'].iloc[0]), 
                                  )
            if entity_data['type'].iloc[0] == 'image':
                return Image(entity_data['id'].iloc[0])
            else:
                
        
                searched_row = relation_db[relation_db['id'] == entity_data['id'].iloc[0]]
                searched_row['creator'] = searched_row.groupby(['id','title', 'entityId'])['creator'].transform(lambda x: '; '.join(x))
                searched_row = searched_row.drop_duplicates()

                if entity_data['type'].iloc[0] == 'canvas':
                    return Canvas(entity_data['id'].iloc[0],
                                entity_data['label'].iloc[0],
                                entity_data['title'].iloc[0],
                                searched_row['creator'].iloc[0].split('; ')
                                )
                elif entity_data['type'].iloc[0] == 'manifest':
                    return Manifest(entity_data['id'].iloc[0],
                                    entity_data['label'].iloc[0],
                                    self.getCanvasesInManifest(entity_data['id'].iloc[0]),
                                    entity_data['title'].iloc[0],
                                    searched_row['creator'].iloc[0].split('; '))
                elif entity_data['type'].iloc[0] == 'collection':
                    return Collection(
                        entity_data['id'].iloc[0],
                        entity_data['label'].iloc[0],
                        self.getManifestsInCollection(entity_data['id'].iloc[0]),
                        entity_data['title'].iloc[0],
                        searched_row['creator'].iloc[0].split('; ')
                    )

            return IdentifiableEntity(pd.loc[0, 'id'])

        # print('no entities in DB have this id')
        # print('returning an empty Identifiable Entity')
        return None
    

    def getEntitiesWithCreator(self, creator):
        graph_db = DataFrame()
        relation_db = DataFrame()
        for item in self.queryProcessors:  
            if isinstance(item, RelationalQueryProcessor):
                relation_to_add = item.getEntitiesWithCreator(creator) #restituisce entityId, id, title, creator
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
            else:
                pass
        if not relation_db.empty:
            id_db = relation_db[["id"]]
            for item in self.queryProcessors:  
                if isinstance(item, TriplestoreQueryProcessor):
                    for i, r in id_db.iterrows():
                        graph_to_add = item.getEntitiesWithId(r['id']) #restituisce id label title
                        graph_db = concat([graph_db,graph_to_add], ignore_index= True)
                else:
                    pass    
        entity_list =[]
        if not relation_db.empty:
            df_joined = merge(graph_db, relation_db, left_on="id", right_on="id")
            # itera le righe del dataframe e crea gli oggetti Canvas
            for index, row in df_joined.iterrows():
                entity = self.getEntityById(row['id'])
                entity_list.append(entity)
        return entity_list
        

    # ERICA:
    def getEntitiesWithLabel(self, label):
        
        graph_db = DataFrame()
        relation_db = DataFrame()
        result = list()

        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_to_add = processor.getEntitiesWithLabel(label)
                graph_db = concat([graph_db, graph_to_add], ignore_index= True)
            elif isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getEntities()
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
            else:
                break
        
        
        if not graph_db.empty: #check if the call got some result
            df_joined = merge(graph_db, relation_db, left_on="id", right_on="id") #create the merge with the two db
            grouped = df_joined.groupby("id").agg({
                                                        "label": "first",
                                                        "title": "first",
                                                        "creator": lambda x: "; ".join(x)
                                                    }).reset_index() #this is to avoid duplicates when we have more than one creator
            grouped_fill = grouped.fillna('')
            sorted = grouped_fill.sort_values("id") #sorted for id
            
            if not sorted.empty:
                for row_idx, row in sorted.iterrows():
                    id = row["id"]
                    label = label
                    title = row["title"]
                    creators = row['creator'].split('; ')
                    entities = self.getEntityById(id)
                    result.append(entities)            
            
                return result

            else:
                for _, row in graph_db.iterrows():
                    id = row["id"]
                    label = label
                    title = ""
                    creators = ""
                    entities = self.getEntityById(id)
                    result.append(entities)
                return result
        return result
                

        
    def getEntitiesWithTitle(self, title):

        graph_db = DataFrame()
        relation_db = DataFrame()
        result = list()

        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_to_add = processor.getAllEntities()
                graph_db = concat([graph_db ,graph_to_add], ignore_index= True)
            elif isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getEntitiesWithTitle(title)
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
            else:
                break        
        

        if not graph_db.empty:
            df_joined = merge(graph_db, relation_db, left_on="id", right_on="id")
            grouped = df_joined.groupby("id").agg({
                                                        "label": "first",
                                                        "title": "first",
                                                        "creator": lambda x: "; ".join(x)
                                                    }).reset_index() #this is to avoid duplicates when we have more than one creator
            grouped_fill = grouped.fillna('')
            # sorted = grouped_fill.sort_values("id")

            for _, row in grouped_fill.iterrows():
                id = row["id"]
                label = row["label"]
                title = row['title']
                creators = row['creator'].split('; ')
                entities = self.getEntityById(id)
                result.append(entities)

        return result
        

    def getImagesAnnotatingCanvas(self, canvasId):

        graph_db = DataFrame()
        relation_db = DataFrame()
        result = list()

        for processor in self.queryProcessors:

            if isinstance(processor, TriplestoreQueryProcessor):
                graph_to_add = processor.getEntitiesWithCanvas(canvasId)
                graph_db = concat([graph_db,graph_to_add], ignore_index= True)
            elif isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getAllAnnotations()
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
            else:
                break

        if not graph_db.empty:
            df_joined = merge(graph_db, relation_db, left_on="id", right_on="target")

            for _, row in df_joined.iterrows():
                id = row["body"]
                images = Image(id)
            result.append(images)

        return result
    

    def getManifestsInCollection(self, collectionId):

        graph_db = DataFrame()
        relation_db = DataFrame()
        result = list()
        
        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_to_add = processor.getManifestsInCollection(collectionId)
                graph_db = concat([graph_db,graph_to_add], ignore_index= True)
            elif isinstance(processor, RelationalQueryProcessor):
                relation_to_add = processor.getEntities()
                relation_db = concat([relation_db, relation_to_add], ignore_index=True)
            else:
                break
        

        if not graph_db.empty:
            df_joined = merge(graph_db, relation_db, left_on="id", right_on="id") 

            for _, row in df_joined.iterrows():

                graph_db_canvas = DataFrame()

                for processor in self.queryProcessors:
                    if isinstance(processor, TriplestoreQueryProcessor):
                        graph_to_add_canvas = processor.getCanvasesInManifest(row['id'])
                        graph_db_canvas = concat([graph_db_canvas,graph_to_add_canvas], ignore_index= True).drop_duplicates()

                        df_joined_canvas = merge(graph_db_canvas, 
                                                relation_db,
                                                how='left',
                                                left_on='id',
                                                right_on='id'
                                                ).fillna('')
        
        
                        df_joined_canvas['creator'] =  df_joined_canvas.groupby(['canvas','id','label', 'entityId', 'title'])['creator'].transform(lambda x: '; '.join(x))
                        df_joined_canvas = df_joined_canvas.drop_duplicates()

                        canvases_list = [
                                Canvas(row1['id'], 
                                row1['label'], 
                                row1['title'],
                                row1['creator'].split('; ')) 
                                for _, row1 in df_joined_canvas.iterrows()
                            ]
                            
                        result.append(
                                Manifest(row["id"],
                                    row["label"], 
                                    canvases_list,
                                    row['title'], 
                                    row['creator'].split('; ')
                                    ) 
                            )

        return result
            
            
