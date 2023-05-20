from sqlite3 import connect
from pandas import read_sql, read_sql_table, DataFrame, concat, read_csv, Series, merge
from rdflib import Graph, Namespace
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get 
from utils.clean_str import remove_special_chars
from json import load
from utils.create_graph import create_Graph
from urllib.parse import urlparse


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
    
# small fix (title and creators can have 0 values)
class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, id: str, label: str, title:str=None, creators:str|list[str]=None):
        super().__init__(id)
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
    def __init__(self, id:str, label:str, title:str=None, creators:list[str]=None):
        super().__init__(id, label, title, creators)

class Manifest(EntityWithMetadata):
    def __init__(self, id:str, label:str, items:list[Canvas], title:str=None, creators:list[str]=None):
        super().__init__(id, label, title, creators)
        self.items = items
    def getItems(self):
        return self.items

class Collection(EntityWithMetadata):
    def __init__(self, id:str, label:str, items:list[Manifest], title:str, creators:list[str]):
        super().__init__(id, label, title, creators)
        self.items = items
    def getItems(self):
        return self.items


# NOTE: BLOCK PROCESSORS

# refactoring
class Processor(object):
    def __init__(self):
        self.dbPathOrUrl = ""
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl 
    def setDbPathOrUrl(self, newpath):
        if len(newpath.split('.')) and newpath.split('.')[-1] == "db":
            self.dbPathOrUrl = newpath
            return True
        elif urlparse(newpath).scheme and urlparse(newpath).netloc:
            self.dbPathOrUrl = newpath
            return True
        return False


class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        entityId_stripped = entityId.strip("'")
        db_url = self.getDbPathOrUrl()
        df = DataFrame()

        if isinstance(self, RelationalQueryProcessor):
            select_entity = f"""
                        SELECT *
                        FROM Entity
                        WHERE 1=1
                        AND id='{entityId_stripped}'
                    """
            select_image = f"""
                        SELECT *
                        FROM Image
                        WHERE 1=1
                        AND id='{entityId_stripped}'
                    """
            select_annotation = f"""
                        SELECT *
                        FROM Annotation
                        WHERE 1=1
                        AND target='{entityId_stripped}'
                    """
            select_creators = f"""
                        SELECT *
                        FROM Creators
                    """
                    
            try:
                with connect(db_url) as con:
                    df_entity = read_sql(select_entity, con) 
                    df_image = read_sql(select_image, con) 
                    df_annotation = read_sql(select_annotation, con) 
                    df_creators = read_sql(select_creators, con) 

                    print(df_entity)
                    print(df_image)
                    print(df_annotation)
                    print(df_creators)
                    
            except Exception as e:
                print(f"couldn't connect to sql database due to the following error: {e}")
            df_entity = df_entity.merge(df_annotation, 'left', left_on='id', right_on='target', suffixes=('_ie', '_ann')).drop_duplicates()
            print(df_entity)
            df_entity = df_entity.merge(df_image, 'left', left_on='id_ie',right_on='id', suffixes=('_ie', '_img')).drop_duplicates()
            df_entity = df_entity.merge(df_creators, 'left', on='entityId', suffixes=('_ie', '_cr')).drop_duplicates()
            df = df_entity
            
        elif isinstance(self, TriplestoreQueryProcessor):
            endpoint = db_url
            query = """
                    PREFIX dc: <http://purl.org/dc/elements/1.1/> 
                    PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
                    PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
                    PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/>  

                    SELECT ?entity ?id
                    WHERE {
                        ?entity dc:identifier "%s" .
                    }
                    """ % entityId 
            try:
                df = get(endpoint, query, True)
            except Exception as e:
                print(f"couldn't connect to blazegraph due to the following error: \n{e}")
                print(f"trying to reconnect via local connection at http://127.0.0.1:9999/blazegraph/sparql")
                try:
                    endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
                    df = get(endpoint, query, True) 
                except Exception as e2:
                    print(f"couldn't connect to blazegraph due to the following error: {e2}")
            
        return df






class RelationalQueryProcessor(QueryProcessor):      
    def __init__(self):
        super().__init__()

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
            q3 = f"SELECT* FROM Annotation WHERE body = '{bodyId}'"
            q3_table = read_sql(q3, con)
            return q3_table         
    def getAnnotationsWithBodyAndTarget(self, bodyId:str,targetId:str):
        with connect(self.getDbPathOrUrl())as con:
            q4 = f"SELECT* FROM Annotation WHERE body = '{bodyId}' AND target = '{targetId}'"
            q4_table = read_sql(q4, con)
            return q4_table         
    def getAnnotationsWithTarget(self, targetId:str):#I've decided not to catch the empty string since in this case a Dataframe is returned, witch is okay
        with connect(self.getDbPathOrUrl())as con:
            q5 = f"SELECT* FROM Annotation WHERE target = '{targetId}'"
            q5_table = read_sql(q5, con)
            return q5_table  
    def getEntitiesWithCreator(self, creatorName):
        with connect(self.getDbPathOrUrl())as con:
             q6 = "SELECT Entity.entityid, Entity.id, Creators.creator, Entity.title FROM Entity LEFT JOIN Creators ON Entity.entityId == Creators.entityId WHERE creator = '" + creatorName +"'"
             result = read_sql(q6, con)
             return result
    def getEntitiesWithTitle(self,title):
        with connect(self.getDbPathOrUrl())as con:
             q6 = "SELECT Entity.entityid, Entity.id, Creators.creator, Entity.title FROM Entity LEFT JOIN Creators ON Entity.entityId == Creators.entityId WHERE title = '" + title +"'"
             result = read_sql(q6, con)  
             return result
    def getEntities(self):
        with connect(self.getDbPathOrUrl())as con:
             q7 = "SELECT Entity.entityid, Entity.id, Creators.creator, Entity.title FROM Entity LEFT JOIN Creators ON Entity.entityId == Creators.entityId"
             result = read_sql(q7, con) 
             return result 
        
# rel_qp = RelationalQueryProcessor()
# rel_qp.setDbPathOrUrl('./data/test.db')
# print(rel_qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/manifest'))




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
        
t_qp = TriplestoreQueryProcessor()
t_qp.setDbPathOrUrl('./data/test.db')
print(t_qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/manifest'))








class AnnotationProcessor(Processor):
    def __init__(self):
        # initializing dbPathURL!
        super().__init__()
    def uploadData(self, path:str): 
        try:
            annotations = read_csv(path, 
                                    keep_default_na=False,
                                    dtype={
                                        "id": "string",
                                        "body": "string",
                                        "target": "string",
                                        "motivation": "string"
                                    }, encoding='utf-8')
            
            images = annotations[["body"]].rename(columns={"body": "id"})
            images["imageId"] = 'image-'+str(images.index)

            # this is not just syntax:
            # once we provide a new file we want to update the DB, 
            # not replace it
            with connect(self.getDbPathOrUrl()) as con:
                CREATE_ANNOTATION_QUERY = """ 
                            CREATE TABLE IF NOT EXISTS Annotation (
                            annotationId VARCHAR(255) NOT NULL,
                            id VARCHAR(100) NOT NULL,
                            body VARCHAR(100),
                            target VARCHAR(100),
                            motivation VARCHAR(100)
                            );
                        """
                
                CREATE_IMAGE_QUERY = """ 
                            CREATE TABLE IF NOT EXISTS Image (
                            imageId VARCHAR(255) NOT NULL,
                            id VARCHAR(100) NOT NULL
                            ); 
                        """
                
                con.cursor().execute(CREATE_ANNOTATION_QUERY)
                con.cursor().execute(CREATE_IMAGE_QUERY)

                annotations_prev = read_sql("SELECT * FROM Annotation", con)
                annotations = concat([annotations_prev, annotations], ignore_index=True)
                annotations["annotationId"] = 'annotation-' + annotations.index.astype(str)
                annotations.to_sql("Annotation", con, if_exists="replace", index=False)

                images_prev = read_sql("SELECT * FROM Image", con)
                images = concat([images_prev, images], ignore_index=True)
                images["imageId"] = 'image-' + images.index.astype(str)
                images.to_sql("Image", con, if_exists="replace", index=False)
            return True
        
        except Exception as e:
            print(str(e))
        return False

# ap = AnnotationProcessor()
# ap.setDbPathOrUrl('./data/test.db')
# print(ap.uploadData('./data/annotations.csv'))
# print(ap.getDbPathOrUrl())


class MetadataProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path:str):
        entityWithMetadata = read_csv(path, 
                                keep_default_na=False,
                                dtype={
                                    "id": "string",
                                    "title": "string",
                                    "creator": "string"
                                })
    
        # this works computanionally better then iterrows
        # plus it is shorter to write
        entityWithMetadata['entityId'] = 'entity-' + entityWithMetadata.index.astype(str)
        creator = entityWithMetadata[["entityId", "creator"]]
        entityWithMetadata = entityWithMetadata[["entityId", "id", "title"]]
                
        # what if creator had a substring 'entity-'
        # or what if entityId had a substring ';'
        creator_upd = DataFrame(columns=["entityId", "creator"])
        for _, row in creator.iterrows():
            list_of_creators = row['creator'].split(';')
            for c in list_of_creators:
                creator_upd.loc[len(creator_upd)] = [row['entityId'], c]
        try: 
            with connect(self.getDbPathOrUrl()) as con:
                entityWithMetadata.to_sql("Entity", con, if_exists="append", index = False)
                creator_upd.to_sql("Creators", con, if_exists="append", index = False)
                return True
        except Exception as e:
            print(str(e))
        return False


# mp = MetadataProcessor()
# mp.setDbPathOrUrl('./data/test.db')
# print(mp.uploadData('./data/metadata.csv'))
# print(mp.getDbPathOrUrl())



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
            
            # FIX
            # my_graph.serialize(destination="Graph_db.ttl", format="turtle")
            with open('Graph_db.ttl', mode='a', encoding='utf-8') as f:
                f.write(my_graph.serialize(format='turtle'))

            return True
        
        except Exception as e:
            print(str(e))
            return False


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
        result = []
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                try:
                    df = processor.getAllAnnotations()
                    df = df.reset_index() 

                    annotations_list = [
                        Annotation(row['id'], 
                                row['motivation'], 
                                IdentifiableEntity(row['target']), 
                                Image(row['body'])
                                ) for _, row in df.iterrows()
                    ] 

                    result += annotations_list
                except Exception as e:
                    print(e)
        return result
    
    
    def getAllCanvas(self):
        tqp_df = DataFrame()
        rqp_df = DataFrame()
        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                try:
                    df = processor.getAllCanvases()
                    df = df.reset_index() 
                    tqp_df = df if tqp_df.empty else tqp_df.append(df).drop_duplicates()
                    for processor2 in self.queryProcessors:
                        if isinstance(processor2, RelationalQueryProcessor):
                            df2 = processor.getEntities()
                            rqp_df = df2 if rqp_df.empty else rqp_df.append(df2).drop_duplicates()

                except Exception as e:
                    print(e)

        if not tqp_df.empty:
            df_joined = merge(tqp_df, rqp_df, left_on="id", right_on="id")
            
            result = [
                            Canvas(row['id'],
                                    row['label'], 
                                    row['title'],
                                    row['creator']
                                    ) for _, row in df_joined.iterrows()
                        ] 
            
        return result
    

    def getAllCollections(self):
        tqp_df = DataFrame()
        rqp_df = DataFrame()
        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                try:
                    df = processor.getAllCollections()
                    df = df.reset_index() 
                    tqp_df = df if tqp_df.empty else tqp_df.append(df).drop_duplicates()
                    for processor2 in self.queryProcessors:
                        if isinstance(processor2, RelationalQueryProcessor):
                            df2 = processor.getEntities()
                            rqp_df = df2 if rqp_df.empty else rqp_df.append(df2).drop_duplicates()

                except Exception as e:
                    print(e)

        if not tqp_df.empty:
            df_joined = merge(tqp_df, rqp_df, left_on="id", right_on="id")
            
            result = [
                            Collection(row['id'],
                                       row['label'],
                                       row['title'],
                                       row['creator'],
                                       row['items']
                                       ) for _, row in df_joined.iterrows()
                        ] 
        #     def __init__(self, id:str, label:str, title:str, creators:list[str], items:list[Manifest]):
        return result

    def getAllImages(self):
        result = []
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                try:
                    df = processor.getAllAnnotations()
                    df = df.reset_index() 

                    annotations_list = [
                        Image(row['id'])
                                 for _, row in df.iterrows()
                    ] 

                    result += annotations_list
                except Exception as e:
                    print(e)
        return result

    def getAllManifests(self):
        tqp_df = DataFrame()
        rqp_df = DataFrame()
        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                try:
                    df = processor.getAllCollections()
                    df = df.reset_index() 
                    tqp_df = df if tqp_df.empty else tqp_df.append(df).drop_duplicates()
                    for processor2 in self.queryProcessors:
                        if isinstance(processor2, RelationalQueryProcessor):
                            df2 = processor.getEntities()
                            rqp_df = df2 if rqp_df.empty else rqp_df.append(df2).drop_duplicates()

                except Exception as e:
                    print(e)

        if not tqp_df.empty:
            df_joined = merge(tqp_df, rqp_df, left_on="id", right_on="id")
            
            result = [
                            Manifest(row['id'],
                                     row['label'],
                                     row['title'],
                                     row['creator'],
                                     [item for item in processor.getCanvasesInManifest(id)])
                                       for _, row in df_joined.iterrows()
                        ] 
        #     def __init__(self, id:str, label:str, title:str, creators:list[str], items:list[Manifest]):
        return result
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
            for index, row in df_joined.iterrows():
                    canvas = Canvas(row['id'], row['label'], row['title'], row['creator'])
                    canvas_list.append(canvas)
        return canvas_list 
    
    def getEntityById(self, id):#non ancora implementato
        for item in self.queryProcessors:
            if isinstance(item, TriplestoreQueryProcessor):
                graph_db = item.getEntitiesWithId(id)  
                for index, row in graph_db.iterrows(): 
                    entity = IdentifiableEntity(row['id'])
                    return entity
            else:
                pass
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
                entity = EntityWithMetadata(row['id'], row['label'], row['title'], row['creator'])
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
                # if the merge has some result inside, proceed
                for row_idx, row in sorted.iterrows():
                    if row["title"] != '' and row["creator"] != '':
                        id = row["id"]
                        label = label
                        title = row["title"]
                        creators = row['creator']
                        for item in creators: # iterate the string and find out if there are some ";", if there are, split them
                            if ";" in item:
                                creators = creators.split(';') 
                            

                        entities = EntityWithMetadata(id, label, title, creators)
                        result.append(entities)
                    
                    else: 

                        id = row["id"]
                        label = label
                        title = ""
                        creators = ""

                        entities = EntityWithMetadata(id, label, title, creators)
                        result.append(entities)
            
                return result
        
            else:
                for _, row in graph_db.iterrows():
                    id = row["id"]
                    label = label
                    title = ""
                    creators = ""

                    entities = EntityWithMetadata(id, label, title, creators)
                    result.append(entities)
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
            sorted = grouped_fill.sort_values("id")

            for _, row in sorted.iterrows():
                id = row["id"]
                label = row["label"]
                title = title
                creators = row["creator"]
                for item in creators: # iterate the string and find out if there are some ";", if there are, split them
                            if ";" in item:
                                creators = creators.split(';') 
                entities = EntityWithMetadata(id, label, title, creators)
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

            if not df_joined.empty:

                for _, row in df_joined.iterrows():
                    id = row["id"]
                    label = row["label"]
                    title = row["title"]
                    creators = row["creator"]
                    items = processor.getCanvasesInManifest(id)
                    manifests = Manifest(id, label, title, creators, items)
                    result.append(manifests)

            return result
        else: 

            for _, row in graph_db.iterrows():
                id = row["id"]
                label = row["label"]
                title = ""
                creators = ""
                items = processor.getCanvasesInManifest(id)
                manifests = Manifest(id, label, title, creators, items)
                result.append(manifests)            

            return result

