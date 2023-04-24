from sqlite3 import connect
from pandas import read_sql, DataFrame


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
        db_url = self.getDbPathOrUrl() if len(self.getDbPathOrUrl()) else './data/annotation.db'
        with connect(db_url) as con:
            query = \
            "SELECT *" +\
            " FROM Entity" +\
            " LEFT JOIN Annotation" +\
            " ON Entity.id = Annotation.target" +\
            " WHERE 1=1" +\
            f" AND Entity.id='{entityId_stripped}'"
            df_sql = read_sql(query, con)
        return df_sql

# Uncomment for a small test of query processor    
# qp = QueryProcessor()
# print(qp.getEntityById('haha'))
# print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))