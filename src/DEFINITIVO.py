from pandas import DataFrame, read_csv, Series, concat, read_sql
from sqlite3 import connect

class Processor(object):
    def __init__(self):
        self.dbPathOrUrl = ""
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl 
    def setDbPathOrUrl(self, newpath):
        self.dbPathOrUrl = newpath

class AnnotationProcessor(QueryProcessor):
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

class MetadataProcessor(QueryProcessor):
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
                    list_of_creators =  row["creator"].split(";")
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

