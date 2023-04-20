class Processor():
    def __init__(self):
        self.dbPathOrUrl = ''
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    def setDbPathOrUrl(self, dbPathOrUrl:str):
        self.dbPathOrUrl = dbPathOrUrl

p = Processor()
p.setDbPathOrUrl('haha')
print(p.getDbPathOrUrl())

x = Processor('hehe')