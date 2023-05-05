from pandas import DataFrame, read_csv, Series, concat, read_sql
from sqlite3 import connect
from processors import QueryProcessor


class RelationalQueryProcessor(QueryProcessor):          
    def __init__(self):
        pass
    def getAllAnnotations(self):
        with connect(self.getDbPathOrUrl()) as con:
          q1="SELECT * FROM Annotation;" 
          q1_table = read_sql(q1, con)
          return q1_table       #fa quello che gli viene chiesto(per controllare l'ho momentaneamente reso figlio di Processor)
    def getAllImages(self):
        with connect(self.getDbPathOrUrl()) as con:
          q2="SELECT * FROM Image;" 
          q2_table = read_sql(q2, con)
          return q2_table       #fa quello che gli viene chiesto(per controllare l'ho momentaneamente reso figlio di Processor)
    def getAnnotationsWithBody(self, bodyId:str):
        with connect(self.getDbPathOrUrl())as con:
            q3 = "SELECT* FROM Annotation WHERE body = "+ bodyId
            q3_table = read_sql(q3, con)
            return q3_table         #fa quello che gli viene chiesto(per controllare l'ho momentaneamente reso figlio di Processor)
    def getAnnotationsWithBodyAndTarget(self, bodyId:str,targetId:str):
        with connect(self.getDbPathOrUrl())as con:
            q4 = "SELECT* FROM Annotation WHERE body = " + bodyId + " AND target = '" + targetId +"'"
            q4_table = read_sql (q4, con)
            return q4_table         #macello con le quotes ma funziona
    def getAnnotationsWithTarget(self, targetId:str):
        with connect(self.getDbPathOrUrl())as con:
            q5 = "SELECT* FROM Annotation WHERE target = '" + targetId +"'"
            q5_table = read_sql(q5, con)
            return q5_table     #macello con le quotes ma funziona
    def getEntitiesWithCreator(self, creatorName):
        with connect(self.getDbPathOrUrl())as con:
             q6 = "SELECT* FROM Entity LEFT JOIN Creators ON Entity.entityId == Creators.entityId WHERE creator = '" + creatorName +"'"
             result = read_sql(q6, con)       #attenzione il risultato contiene due volte entityId come colonna
             return result.drop_duplicates(subset=["entityId"])
    def getEntitiesWithLabel(self):
        pass    #attenzione, da implementare
    def getEntitiesWithTitle(self,title):
        with connect(self.getDbPathOrUrl())as con:
             q6 = "SELECT* FROM Entity WHERE title = '" + title +"'"
             return read_sql(q6, con)   #funziona
        
