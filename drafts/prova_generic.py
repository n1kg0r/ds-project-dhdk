from TriplestoreQueryProcessor import TriplestoreQueryProcessor
from pandas import DataFrame, merge
from RelationalQueryProcessor import RelationalQueryProcessor, Processor
from data_model import Canvas

class GenericQueryProcessor():
    def __init__(self):
        self.queryProcessors = []
    def cleanQueryProcessors(self):
        self.queryProcessors = []
    def addQueryProcessor(self, processor):
        self.queryProcessors.append(processor)
    def getCanvasesInCollection(self, collectionId):
        graph_db = DataFrame()
        relation_db = DataFrame()
        for item in self.queryProcessors:
            if isinstance(item, TriplestoreQueryProcessor):
                graph_db = item.getCanvasesInCollection(collectionId)#restituisce canva, id, collection
            elif isinstance(item, RelationalQueryProcessor):
                relation_db = item.getEntities() #restituisce entityId, id, title, creator
            else:
                break
        if not graph_db.empty:
            df_joined = merge(graph_db, relation_db, left_on="id", right_on="id")
            canvas_list = []
            # itera le righe del dataframe e crea gli oggetti Canvas
            for index, row in df_joined.iterrows():
                canvas = Canvas(row['id'], row['label'], row['title'], row['creator'])
                canvas_list.append(canvas)
            return canvas_list
        
generic = GenericQueryProcessor()
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl("database.db")
rel_path = "database.db"
rel_qp.setDbPathOrUrl(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl("http://10.5.98.175:9999/blazegraph/sparql")
grp_endpoint = "http://10.5.98.175:9999/blazegraph/sparql"
grp_qp.setDbPathOrUrl(grp_endpoint)
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)
ciao = generic.getCanvasesInCollection("https://dl.ficlit.unibo.it/iiif/28429/collection")


a = None
for i in ciao:
    a = i
    break
print(vars(a))