from pandas import DataFrame, read_csv, Series, concat, read_sql
from sqlite3 import connect
from Processor_MetadataP_AnnotationP import Processor


class RelationalQueryProcessor(QueryProcessor):          
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
            q4_table = read_sql (q4, con)
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
        
# ciao = RelationalQueryProcessor()
# ciao.setDbPathOrUrl("database.db")
# print(ciao.getAnnotationsWithBodyAndTarget("",""))
# print(unifyCreators(ciao)) #il risultato è una stringa però(attenzione: non c'è modo per produrre come output una lista. mai.)




