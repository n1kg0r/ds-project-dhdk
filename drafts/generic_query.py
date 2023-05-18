from TriplestoreQueryProcessor import *
from QueryProcessor import *
from processor import *
from data_model import *
from CollectionProcessor import *
from RelationalQueryProcessor import *
from pandas import Series, DataFrame, merge, concat

class GenericQueryProcessor(object):

    def __init__(self):
        self.queryProcessors = list()

    def cleanQueryProcessors(self):
        self.queryProcessors.clear()
        if len(self.queryProcessors) == 0:
            return True
        else:
            return False

    def addQueryProcessor(self, processor):
        self.queryProcessors.append(processor)


    def getEntitiesWithLabel(self, label):
        
        graph_db = DataFrame()
        relational_db = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_db = processor.getEntitiesWithLabel(label)
            elif isinstance(processor, RelationalQueryProcessor):
                relational_db = processor.getEntities()
            else:
                break

        if not graph_db.empty:
            df_joined = merge(graph_db, relational_db, left_on="id", right_on="id")
            result = list()

            for row_idx, row in df_joined.iterrows():
                id = row["id"]
                label = row["label"]
                title = row["title"]
                creators = row["creator"]
                entities = EntityWithMetadata(id, label, title, creators)
                result.append(entities)

        return result
        


    def getEntitieswithTitle(self, title):

        for processor in self.queryProcessors:
            graph_db = DataFrame()
            relational_db = DataFrame()
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_db = processor.getAllEntities()
            elif isinstance(processor, RelationalQueryProcessor):
                relational_db = processor.getEntitiesWithTitle(title)
            else:
                break

        if not graph_db.empty:
            df_joined = merge(graph_db, relational_db, left_on="id", right_on="id")
            result = list()

        for row_idx, row in df_joined.iterrows():
            id = row["id"]
            label = row["label"]
            title = row["title"]
            creators = row["creators"]
            entities = EntityWithMetadata(id, label, title, creators)
            result.append(entities)

        return result
        

    def getImagesAnnotatingCanvas(self, canvasId):

        for processor in self.queryProcessors:
            graph_db = DataFrame()
            relational_db = DataFrame()
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_db = processor.getEntitiesWithCanvas(canvasId)
            elif isinstance(processor, RelationalQueryProcessor):
                relational_db = processor.getAllImages()
            else:
                break

        if not graph_db.empty:
            df_joined = merge(graph_db, relational_db, left_on="id", right_on="id")
            result = list()

        for row_idx, row in df_joined.iterrows():
            id = row["id"]
            label = row["label"]
            title = row["title"]
            creators = row["creators"]
            images = Image(id, label, title, creators)
            result.append(images)

        return result

    def getManifestsInCollection(self, collectionId):
        
        for processor in self.queryProcessors:
            graph_db = DataFrame()
            relational_db = DataFrame()
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_db = processor.getManifestsInCollection(collectionId)
            elif isinstance(processor, RelationalQueryProcessor):
                relational_db = processor.getAllAnnotations()
            else:
                break

        if not graph_db.empty:
            df_joined = merge(graph_db, relational_db, left_on="id", right_on="id")
            result = list()

        for row_idx, row in df_joined.iterrows():
            id = row["id"]
            label = row["label"]
            title = row["title"]
            creators = row["creators"]
            manifests = Manifest(id, label, title, creators)
            result.append(manifests)

        return result
    
        
        
    # crea un metodo per tirare fuori tutte le canvases che corrispondono ad un id di input 
    # nel triplestore query processor in modo da usare un metodo in più qua e fare prima
    
generic = GenericQueryProcessor()
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl("database.db")
rel_path = "database.db"
rel_qp.setDbPathOrUrl(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl("http://192.168.0.168:9999/blazegraph/sparql")
grp_endpoint = "http://192.168.0.168:9999/blazegraph/sparql"
grp_qp.setDbPathOrUrl(grp_endpoint)
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)

ciao = generic.getEntitiesWithLabel('Fondo Giuseppe Raimondi')
print(ciao)

print("that contains")
a = None
for i in ciao:
    a = i
    break
print(vars(a))