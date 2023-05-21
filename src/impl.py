from sqlite3 import connect
from pandas import read_sql, read_sql_table, DataFrame, concat, read_csv, Series, merge
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib import Graph, Namespace
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
    pass


# TODO: think about default values while initialisation
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
        elif len(urlparse(newpath).scheme) and len(urlparse(newpath).netloc):
            self.dbPathOrUrl = newpath
            return True
        return False
        

# TODO: redo sql query for concatenating instead of merging
class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        entityId_stripped = entityId.strip("'")
        db_url = self.getDbPathOrUrl()
        df = DataFrame()

        if len(db_url.split('.')) and db_url.split('.')[-1] == "db":
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

                    # print(df_entity)
                    # print(df_image)
                    # print(df_annotation)
                    # print(df_creators)  
            except Exception as e:
                print(f"couldn't connect to sql database due to the following error: {e}")
            df_entity = df_entity.merge(df_annotation, 'left', left_on='id', right_on='target', suffixes=('_ie', '_ann')).drop_duplicates()
            # print(df_entity)
            df_entity = df_entity.merge(df_image, 'left', left_on='id_ie',right_on='id', suffixes=('_ie', '_img')).drop_duplicates()
            df_entity = df_entity.merge(df_creators, 'left', on='entityId', suffixes=('_ie', '_cr')).drop_duplicates()
            df = df_entity
            
        elif len(urlparse(db_url).scheme) and len(urlparse(db_url).netloc):
            endpoint = db_url
            query = """
                    PREFIX dc: <http://purl.org/dc/elements/1.1/> 
                    PREFIX nikAttr: <https://github.com/n1kg0r/ds-project-dhdk/attributes/> 
                    PREFIX nikCl: <https://github.com/n1kg0r/ds-project-dhdk/classes/> 
                    PREFIX nikRel: <https://github.com/n1kg0r/ds-project-dhdk/relations/>  

                    SELECT ?entity ?id
                    WHERE {
                        ?entity dc:identifier "%s" ;
                        dc:identifier ?id .
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
        
# t_qp = TriplestoreQueryProcessor()
# t_qp.setDbPathOrUrl('./data/test.db')
# print(t_qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/manifest'))








class AnnotationProcessor(QueryProcessor):
    def __init__(self):
        pass
    def uploadData(self, path:str):
        try:
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
                annotations.to_sql("Annotation", con, if_exists="append", index=False)
                image.to_sql("Image", con, if_exists="append", index=False)

            return True
            
        
        except Exception as e:
            print(str(e))
            return False

class MetadataProcessor(QueryProcessor):
    def __init__(self):
        pass
    def uploadData(self, path:str):
        try: 
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
                        for item_idx, item in row.items():
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
                entityWithMetadata.to_sql("Entity", con, if_exists="append", index = False)
                creator.to_sql("Creators", con, if_exists="append", index = False)
            return True
        except Exception as e:
                print(str(e))
                return False

# ap = AnnotationProcessor()
# ap.setDbPathOrUrl('./data/test.db')
# print(ap.uploadData('./data/annotations.csv'))
# print(ap.getDbPathOrUrl())




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
                            IdentifiableEntity(row['target']), 
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

        # print(relation_db)

        df_joined = merge(graph_db, relation_db, left_on="id", right_on="id", how='left').fillna("").drop_duplicates()
        df_joined['creator'] =  df_joined.groupby(['canvas','id','label', 'entityId', 'title'])['creator'].transform(lambda x: '; '.join(x))
        df_joined = df_joined[['canvas','id','label', 'title', 'creator']].drop_duplicates()

        # df_joined.to_csv('./get_all_canvases_res.csv', sep='\t')
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
                # print(df_joined_canvas.columns.to_list())
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
                        row1['title'], 
                        row1['creator'].split('; '),
                        canvases_list,
                        ) 
                )

            
            collections_list.append(
                Collection(row["id"],
                        row["label"], 
                        row['title'], 
                        row['creator'].split('; '),
                        manifests_list,
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
                        row['title'], 
                        row['creator'].split('; '),
                        canvases_list,
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
                    # print(df.columns.to_list())
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
                            IdentifiableEntity(row['target']), 
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
            ]
        return result


    def getAnnotationsToCollection(self, collectionId):
        graph_db = DataFrame()
        relation_db = DataFrame()
        result = []

        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_to_add = processor.getCanvasesInCollection(collectionId)
                graph_db = concat([graph_db, graph_to_add], ignore_index=True)

        # graph_db.to_csv('./get_annotations_to_collection_res.csv', sep='\t')
        graph_db = graph_db[["id"]].drop_duplicates().fillna('')
        
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                for _, row in graph_db.iterrows():
                    relation_to_add = processor.getAnnotationsWithTarget(row["id"])
                    relation_db = concat([relation_db, relation_to_add], ignore_index=True)
                    # print(relation_db.shape[0])
        
        # print(relation_db.columns.to_list())

        if not relation_db.empty:
            relation_db = relation_db[['id', 'motivation', 'target', 'body']].drop_duplicates().fillna('')
            
            result = [
                Annotation(row['id'],
                            row['motivation'],
                            IdentifiableEntity(row['target']), 
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
                        ] 
        return result

  
    def getAnnotationsToManifest(self, manifestId):
        graph_db = DataFrame()
        relation_db = DataFrame()
        result = []

        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_to_add = processor.getCanvasesInManifest(manifestId)
                graph_db = concat([graph_db, graph_to_add], ignore_index=True)

        graph_db = graph_db[["id"]].drop_duplicates().fillna('')
        graph_db.to_csv('./graph_manifest_test.csv', sep='\t')
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                for _, row in graph_db.iterrows():
                    relation_to_add = processor.getAnnotationsWithTarget(row["id"])
                    relation_db = concat([relation_db, relation_to_add], ignore_index=True)
        
        if not relation_db.empty:
            relation_db = relation_db[['id', 'motivation', 'target', 'body']].drop_duplicates().fillna('')

            result = [
                Annotation(row['id'],
                            row['motivation'],
                            IdentifiableEntity(row['target']), 
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
            ]
        return result
    

    def getAnnotationsWithBody(self, bodyId:str):
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
                            IdentifiableEntity(row['target']), 
                            Image(row['body'])
                            ) for _, row in relation_db.iterrows()
            ]
        return result


    def getAnnotationsWithBodyAndTarget(self, bodyId:str, targetId:str):
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
                            IdentifiableEntity(row['target']), 
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
                            IdentifiableEntity(row['target']), 
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
            for index, row in df_joined.iterrows():
                    canvas = Canvas(row['id'], row['label'], row['title'], row['creator'])
                    canvas_list.append(canvas)
        return canvas_list 
    


    def getEntityById(self, id):
        pd = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, QueryProcessor):
                pd_to_add = processor.getEntityById(id)[['id']]
                pd = concat([pd, pd_to_add], ignore_index=True)

        pd = pd.drop_duplicates().fillna('')
        for _, row in pd.iterrows():
            if len(row['id']):
                return IdentifiableEntity(row['id'])

        # TODO: merge from blazegraph and sql
            
        return IdentifiableEntity('')
    

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
                        creators = row['creator'].split('; ')
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
                creators = row['creator'].split('; ')
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



# rel_qp = RelationalQueryProcessor()
# rel_qp.setDbPathOrUrl('./data/test.db')

# grp_qp = TriplestoreQueryProcessor()
# print(grp_qp.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql'))


# generic = GenericQueryProcessor()
# generic.addQueryProcessor(rel_qp)
# generic.addQueryProcessor(grp_qp)

# cp = CollectionProcessor()
# cp.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql')
# cp.uploadData("./data/collection-1.json")
# cp.uploadData("./data/collection-2.json")
# print(generic.getEntitiesWithLabel("Raimondi, Giuseppe. Quaderno manoscritto, \'Caserma Scalo : 1930-1968\'"))

# print(grp_qp.getEntityById('https://dl.ficlit.unibo.it/iiif/28429/collection'))

# print(generic.getAllManifests())
# print(generic.getAllCanvas())
# print(generic.getEntitiesWithCreator("Dante, Alighieri"))
# print(generic.getAnnotationsToCanvas("https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1"))

# print(generic.getAllCollections())

# triplestore processor?

# print(generic.getAllCanvas())

# generic.getAllCollections()
# for collection in generic.getAllCollections():
#     print(collection.id, collection.label, collection.title, collection.items, collection.creators)


# for manifest in generic.getAllCollections()[0].items:
#     print(manifest.id, manifest.label, manifest.title, manifest.items, manifest.creators)